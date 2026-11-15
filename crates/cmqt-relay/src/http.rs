//! `cmqt flush` HTTP mode: OTLP/HTTP (`POST .../v1/metrics`, protobuf or JSON, optionally gzip)
//! behind a TLS-terminating proxy, one bearer token per sender.
//!
//! Each request is converted, packed into independently compressed datagrams and SEALED at once.
//! With `amqp_url` the sealed datagrams are published into a durable queue (publisher confirms,
//! persistent), and a drain task paces them out to the box, so the queue's memory is RabbitMQ's
//! and the AMQP hop only ever carries ciphertext. Without it they are sent directly.

use crate::flush::{COUNTERS, Config, prepare_zstd};
use crate::metrics::{self, Counters};
use crate::wire::{self, Bucket, Sealer};
use futures_lite::StreamExt;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties, options::*, types::FieldTable};
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::{Handle, Runtime};

/// Largest body after decompression (an OTLP batch is far below this).
const MAX_BODY: u64 = 8 * 1024 * 1024;
static CACHE: std::sync::OnceLock<(std::sync::Mutex<crate::cache::Cache>, String)> = std::sync::OnceLock::new();
static STATIC_DIR: std::sync::OnceLock<String> = std::sync::OnceLock::new();
const DEFAULT_QUEUE: &str = "relay_datagrams";

/// Token file: one `name:token` or bare `token` per line; `#` comments.
pub fn load_tokens(path: &str) -> Result<Vec<(String, String)>, String> {
    let s = std::fs::read_to_string(path).map_err(|e| format!("tokens {path}: {e}"))?;
    let v: Vec<(String, String)> = s
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(|l| match l.rsplit_once(':') {
            Some((n, t)) => (n.to_string(), t.to_string()),
            None => (String::new(), l.to_string()),
        })
        .filter(|(_, t)| t.len() >= 16)
        .collect();
    if v.is_empty() {
        return Err(format!("tokens {path}: no token of at least 16 characters"));
    }
    Ok(v)
}

fn eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |d, (x, y)| d | (x ^ y)) == 0
}

/// The device name of the matching token (`Some("")` for a bare token), `None` when none matches.
pub fn authorized<'a>(header: Option<&str>, tokens: &'a [(String, String)]) -> Option<&'a str> {
    let t = header?.strip_prefix("Bearer ")?;
    // Check every token (no early exit) so timing does not reveal which matched.
    tokens.iter().fold(None, |hit, (n, k)| if eq(k.as_bytes(), t.trim().as_bytes()) { Some(n.as_str()) } else { hit })
}

/// `/v1/<device>/<subsystem>` or `/sd/v1/<device>/<subsystem>`.
pub fn sd_path(url: &str) -> Option<(&str, &str)> {
    let p = url.split('?').next()?;
    let p = p.strip_prefix("/sd").unwrap_or(p);
    let mut it = p.strip_prefix("/v1/")?.split('/');
    let (d, s) = (it.next()?, it.next()?);
    (it.next().is_none() && !d.is_empty() && !s.is_empty() && d != "metrics").then_some((d, s))
}

/// Where sealed datagrams go.
enum Sink {
    Direct { sock: std::net::UdpSocket, bucket: Bucket },
    Queue(Publisher),
}

struct Publisher {
    handle: Handle,
    url: String,
    queue: String,
    chan: Option<Channel>,
}

impl Publisher {
    async fn channel(&mut self) -> Result<&Channel, String> {
        if self.chan.as_ref().is_none_or(|c| !c.status().connected()) {
            let conn = Connection::connect(&self.url, ConnectionProperties::default()).await.map_err(|e| e.to_string())?;
            let ch = conn.create_channel().await.map_err(|e| e.to_string())?;
            declare(&ch, &self.queue).await?;
            ch.confirm_select(ConfirmSelectOptions::default()).await.map_err(|e| e.to_string())?;
            self.chan = Some(ch);
        }
        self.chan.as_ref().ok_or_else(|| "no channel".to_string())
    }
    fn publish(&mut self, sealed: &[u8]) -> Result<(), String> {
        let h = self.handle.clone();
        let q = self.queue.clone();
        let r = h.block_on(async {
            let ch = self.channel().await?;
            let confirm = ch
                .basic_publish("".into(), q.as_str().into(), BasicPublishOptions::default(), sealed, BasicProperties::default().with_delivery_mode(2))
                .await
                .map_err(|e| e.to_string())?
                .await
                .map_err(|e| e.to_string())?;
            if confirm.is_ack() { Ok(()) } else { Err("publish not confirmed".to_string()) }
        });
        if r.is_err() {
            self.chan = None;
        }
        r
    }
}

async fn declare(ch: &Channel, queue: &str) -> Result<(), String> {
    ch.queue_declare(queue.into(), QueueDeclareOptions { durable: true, ..Default::default() }, FieldTable::default())
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Drain the datagram queue: pace, send the already sealed bytes, acknowledge.
async fn drain(url: String, queue: String, target: String, rate: f64, burst: f64, prefetch: u16, c: Arc<Counters>) {
    let mut bucket = Bucket::new(rate, burst);
    let sock = match tokio::net::UdpSocket::bind(if target.starts_with('[') { "[::]:0" } else { "0.0.0.0:0" }).await {
        Ok(s) => s,
        Err(e) => return eprintln!("cmqt flush: drain bind: {e}"),
    };
    if let Err(e) = sock.connect(&target).await {
        return eprintln!("cmqt flush: drain target {target}: {e}");
    }
    loop {
        let r: Result<(), String> = async {
            let conn = Connection::connect(&url, ConnectionProperties::default()).await.map_err(|e| e.to_string())?;
            let ch = conn.create_channel().await.map_err(|e| e.to_string())?;
            declare(&ch, &queue).await?;
            ch.basic_qos(prefetch, BasicQosOptions::default()).await.map_err(|e| e.to_string())?;
            let mut consumer = ch.basic_consume(queue.as_str().into(), "cmqt-drain".into(), BasicConsumeOptions::default(), FieldTable::default()).await.map_err(|e| e.to_string())?;
            eprintln!("cmqt flush: draining {queue}");
            while let Some(d) = consumer.next().await {
                let d = d.map_err(|e| e.to_string())?;
                let wait = bucket.take();
                if !wait.is_zero() {
                    tokio::time::sleep(wait).await;
                }
                match sock.send(&d.data).await {
                    Ok(_) => {
                        c.add("cmqt_flush_datagrams_total", 1);
                        c.add("cmqt_flush_bytes_total", d.data.len() as u64);
                        d.ack(BasicAckOptions::default()).await.map_err(|e| e.to_string())?;
                    }
                    // ICMP port unreachable from a stopped receiver: lost like any datagram in an outage (counted).
                    Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => {
                        c.add("cmqt_flush_send_errors_total", 1);
                        d.ack(BasicAckOptions::default()).await.map_err(|e| e.to_string())?;
                    }
                    Err(e) => {
                        eprintln!("cmqt flush: send: {e}");
                        c.add("cmqt_flush_send_errors_total", 1);
                        d.nack(BasicNackOptions { requeue: true, ..Default::default() }).await.map_err(|e| e.to_string())?;
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
            Err("consumer stream ended".into())
        }
        .await;
        c.add("cmqt_flush_amqp_errors_total", 1);
        eprintln!("cmqt flush: drain ended: {}; reconnecting in 5 s", r.err().unwrap_or_default());
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

struct State {
    sealer: Sealer,
    sink: Sink,
}

pub fn run(mut cfg: Config) -> Result<(), String> {
    let listen = cfg.http_listen.clone().ok_or("http_listen missing")?;
    let tokens = load_tokens(cfg.tokens_file.as_deref().ok_or("tokens_file missing")?)?;
    let counters = Counters::new(COUNTERS);
    if let Some(m) = &cfg.metrics_listen {
        metrics::serve(m, counters.clone())?;
    }
    let sealer = Sealer::new(wire::load_key(&cfg.key_file)?);
    let rt: Runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(1).enable_all().build().map_err(|e| e.to_string())?;
    let sink = if let Some(url) = cfg.amqp_url.take() {
        let queue = cfg.datagram_queue.clone().unwrap_or_else(|| DEFAULT_QUEUE.into());
        rt.spawn(drain(url.clone(), queue.clone(), cfg.target.clone(), cfg.rate_pps, cfg.burst, cfg.prefetch, counters.clone()));
        Sink::Queue(Publisher { handle: rt.handle().clone(), url, queue, chan: None })
    } else {
        let sock = std::net::UdpSocket::bind(if cfg.target.starts_with('[') { "[::]:0" } else { "0.0.0.0:0" }).map_err(|e| e.to_string())?;
        sock.connect(&cfg.target).map_err(|e| format!("target {}: {e}", cfg.target))?;
        Sink::Direct { sock, bucket: Bucket::new(cfg.rate_pps, cfg.burst) }
    };
    if let Some(d) = cfg.static_dir.clone() {
        let _ = STATIC_DIR.set(d);
    }
    if cfg.screen_password_file.is_some() {
        let pw = std::fs::read_to_string(cfg.screen_password_file.as_deref().ok_or("screen_password_file missing")?).map_err(|e| e.to_string())?;
        let _ = CACHE.set((Mutex::new(crate::cache::Cache::new((cfg.cache_floor_mib << 20).max(1 << 20))), pw.trim().to_string()));
    }
    let focus: crate::watch::SharedFocus = Default::default();
    if let Some(w) = cfg.watch.clone() {
        crate::watch::spawn(w, focus.clone())?;
    }
    // Governors (D76), once a second: CPU share over a 30 s window paces ingest; the history cache
    // gets the RAM that is left below the total target. Every 5 minutes both go into the log
    // together with this process's resident memory and its peak.
    {
        let (cpu_t, ram_t, floor, ceil) = (cfg.cpu_target, cfg.ram_target_mib << 20, cfg.cache_floor_mib << 20, cfg.cache_mib << 20);
        std::thread::spawn(move || {
            let t0 = std::time::Instant::now();
            let mut win = crate::govern::CpuWindow::new(30.0);
            let (mut n, mut rss_peak) = (0u64, 0usize);
            loop {
                let (cpu, rss) = crate::govern::sample();
                rss_peak = rss_peak.max(rss);
                win.push(t0.elapsed().as_secs_f64(), cpu);
                let share = win.share();
                let d = crate::govern::next_delay_ms(crate::govern::DELAY_MS.load(std::sync::atomic::Ordering::Relaxed), share, cpu_t);
                crate::govern::DELAY_MS.store(d, std::sync::atomic::Ordering::Relaxed);
                let mut ch = String::new();
                if let Some((c, _)) = CACHE.get() {
                    let mut g = c.lock().unwrap_or_else(|e| e.into_inner());
                    g.cap = crate::govern::cache_cap(rss, g.bytes(), ram_t, floor, ceil);
                    ch = format!(" | cache {} B (cap {}), {} topics, oldest {:?} ms, evicted {}", g.bytes(), g.cap, g.series_count(), g.oldest_ms(), g.evicted_blocks);
                }
                if n % 300 == 0 {
                    let own = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
                    let f = |k: &str| own.lines().find(|l| l.starts_with(k)).unwrap_or("").split_whitespace().collect::<Vec<_>>().join(" ");
                    eprintln!("cmqt flush: cpu {share:.3} of {cpu_t} (delay {d} ms) | total rss {} B of {ram_t}, peak {rss_peak} | self {} | {}{ch}", rss, f("VmRSS:"), f("VmHWM:"));
                }
                n += 1;
                std::thread::sleep(Duration::from_secs(1));
            }
        });
    }
    let server = tiny_http::Server::http(&listen).map_err(|e| format!("http listen {listen}: {e}"))?;
    eprintln!("cmqt flush: OTLP/HTTP on {listen}");
    let server = Arc::new(server);
    let state = Arc::new(Mutex::new(State { sealer, sink }));
    let tokens = Arc::new(tokens);
    let mut hs = Vec::new();
    for _ in 0..3 {
        let (server, state, tokens, c, focus) = (server.clone(), state.clone(), tokens.clone(), counters.clone(), focus.clone());
        hs.push(std::thread::spawn(move || {
            for rq in server.incoming_requests() {
                handle(rq, &tokens, &state, &c, &focus);
            }
        }));
    }
    for h in hs {
        let _ = h.join();
    }
    Err("http server ended".into())
}

fn handle(mut rq: tiny_http::Request, tokens: &[(String, String)], state: &Mutex<State>, c: &Counters, focus: &crate::watch::SharedFocus) {
    let hdr = |n: &'static str| rq.headers().iter().find(|h| h.field.equiv(n)).map(|h| h.value.as_str().to_string());
    let auth = hdr("Authorization");
    let gz = hdr("Content-Encoding").is_some_and(|e| e.eq_ignore_ascii_case("gzip"));
    let path_ok = rq.url().split('?').next().is_some_and(|p| p.ends_with("/v1/metrics"));
    if *rq.method() == tiny_http::Method::Get
        && let Some(dir) = STATIC_DIR.get()
        && !rq.url().contains("/api/v1/")
        && !rq.url().starts_with("/sd/")
        && !rq.url().starts_with("/v1/")
    {
        let path = rq.url().split('?').next().unwrap_or("/").trim_start_matches('/');
        let (name, ctype) = match path {
            "app.js" => ("app.js", "text/javascript"),
            "style.css" => ("style.css", "text/css"),
            _ => ("index.html", "text/html; charset=utf-8"),
        };
        let r = match std::fs::read(std::path::Path::new(dir).join(name)) {
            Ok(b) => tiny_http::Response::from_data(b).with_status_code(200),
            Err(_) => tiny_http::Response::from_data(Vec::new()).with_status_code(404),
        };
        let r = match tiny_http::Header::from_bytes("Content-Type", ctype) {
            Ok(h) => r.with_header(h),
            Err(_) => r,
        };
        let _ = rq.respond(r);
        return;
    }
    if *rq.method() == tiny_http::Method::Get
        && rq.url().contains("/api/v1/messages")
        && let Some((cache, pw)) = CACHE.get()
    {
        let (code, body) = messages(rq.url(), auth.as_deref(), cache, pw);
        let mut r = tiny_http::Response::from_string(body).with_status_code(code);
        if let Ok(h) = tiny_http::Header::from_bytes("Content-Type", "application/json") {
            r = r.with_header(h);
        }
        let _ = rq.respond(r);
        return;
    }
    let who = authorized(auth.as_deref(), tokens);
    if who.is_some()
        && *rq.method() == tiny_http::Method::Get
        && let Some((device, sub)) = sd_path(rq.url())
    {
        let (device, sub) = (device.to_string(), sub.to_string());
        let (code, body) = if who != Some(device.as_str()) {
            (403, String::new())
        } else if !crate::watch::SUBSYSTEMS.contains(&sub.as_str()) {
            (404, String::new())
        } else if focus.lock().unwrap_or_else(|e| e.into_inner()).in_focus(std::time::Instant::now(), &device, &sub) {
            (200, r#"[{"targets":["focus"],"labels":{}}]"#.to_string())
        } else {
            (200, "[]".to_string())
        };
        c.add("cmqt_flush_sd_requests_total", 1);
        let mut r = tiny_http::Response::from_string(body).with_status_code(code);
        if let Ok(h) = tiny_http::Header::from_bytes("Content-Type", "application/json") {
            r = r.with_header(h);
        }
        if let Ok(h) = tiny_http::Header::from_bytes("Cache-Control", "no-store") {
            r = r.with_header(h);
        }
        let _ = rq.respond(r);
        return;
    }
    let status = if who.is_none() {
        c.add("cmqt_flush_http_unauthorized_total", 1);
        401
    } else if *rq.method() != tiny_http::Method::Post || !path_ok {
        404
    } else {
        let pace = crate::govern::delay();
        if !pace.is_zero() {
            std::thread::sleep(pace);
        }
        let mut body = Vec::new();
        let r = if gz {
            flate2::read::GzDecoder::new(rq.as_reader()).take(MAX_BODY + 1).read_to_end(&mut body)
        } else {
            rq.as_reader().take(MAX_BODY + 1).read_to_end(&mut body)
        };
        if r.is_err() || body.len() as u64 > MAX_BODY {
            400
        } else {
            c.add("cmqt_flush_http_bytes_in_total", body.len() as u64);
            match prepare_zstd(&body, c) {
                Err(e) => {
                    eprintln!("cmqt flush: http: dropping request: {e}");
                    c.add("cmqt_flush_bad_messages_total", 1);
                    400
                }
                Ok(dg) => send(&dg, state, c),
            }
        }
    };
    let _ = rq.respond(tiny_http::Response::empty(status));
}

fn qparam(url: &str, k: &str) -> Option<String> {
    let q = url.split_once('?')?.1;
    q.split('&').find_map(|p| {
        let (a, b) = p.split_once('=')?;
        (a == k).then(|| pct_decode(b))
    })
}
fn pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let (mut o, mut i) = (Vec::new(), 0);
    while i < b.len() {
        match b[i] {
            b'%' if i + 2 < b.len() => {
                if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) { o.push(v); i += 3; continue; }
                o.push(b'%');
            }
            b'+' => o.push(b' '),
            c => o.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&o).into_owned()
}
fn ts_ms(v: &str) -> Option<i64> {
    v.parse::<f64>().ok().map(|f| (f * 1000.0) as i64)
}

/// Feed one message of the box (tile link) into the history cache, if it is on.
pub fn cache_put(topic: &str, payload: &[u8]) {
    if let Some((c, _)) = CACHE.get() {
        let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0);
        c.lock().unwrap_or_else(|e| e.into_inner()).insert(topic, ts, payload);
    }
}

/// `GET .../api/v1/messages?topic=<prefix>&start=<s>&end=<s>&limit=<n>`: the box's output history as
/// `{"status":"success","data":[{"topic","t":<s>,"payload":<json or string>}]}`; HTTP Basic, user `screen`.
fn messages(url: &str, auth: Option<&str>, cache: &Mutex<crate::cache::Cache>, pw: &str) -> (u16, String) {
    use base64::Engine;
    let ok = auth
        .and_then(|a| a.strip_prefix("Basic "))
        .and_then(|b| base64::engine::general_purpose::STANDARD.decode(b.trim()).ok())
        .is_some_and(|d| eq(&d, format!("screen:{pw}").as_bytes()));
    if !ok {
        return (401, String::new());
    }
    let topic = qparam(url, "topic").unwrap_or_default();
    let start = qparam(url, "start").and_then(|v| ts_ms(&v)).unwrap_or(0);
    let end = qparam(url, "end").and_then(|v| ts_ms(&v)).unwrap_or(i64::MAX);
    let limit = qparam(url, "limit").and_then(|v| v.parse::<usize>().ok()).unwrap_or(1000).min(10_000);
    let r = cache.lock().unwrap_or_else(|e| e.into_inner()).range(&topic, start, end, limit);
    let data: Vec<serde_json::Value> = r
        .into_iter()
        .map(|(k, t, p)| {
            let payload = serde_json::from_slice(&p).unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(&p).into_owned()));
            serde_json::json!({"topic": k, "t": t as f64 / 1000.0, "payload": payload})
        })
        .collect();
    (200, serde_json::json!({"status":"success","data":data}).to_string())
}

/// Seal and hand over, in order: sealing and publishing share one lock so the queue keeps the
/// sequence order.
fn send(dg: &[Vec<u8>], state: &Mutex<State>, c: &Counters) -> u16 {
    let mut st = state.lock().unwrap_or_else(|p| p.into_inner());
    let State { sealer, sink } = &mut *st;
    for p in dg {
        let sealed = sealer.seal_zstd(p);
        match sink {
            Sink::Queue(q) => {
                if let Err(e) = q.publish(&sealed) {
                    eprintln!("cmqt flush: queue: {e}");
                    c.add("cmqt_flush_queue_errors_total", 1);
                    return 503;
                }
            }
            Sink::Direct { sock, bucket } => {
                let wait = bucket.take();
                if !wait.is_zero() {
                    std::thread::sleep(wait);
                }
                match sock.send(&sealed) {
                    Ok(_) => {
                        c.add("cmqt_flush_datagrams_total", 1);
                        c.add("cmqt_flush_bytes_total", sealed.len() as u64);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => c.add("cmqt_flush_send_errors_total", 1),
                    Err(e) => {
                        eprintln!("cmqt flush: send: {e}");
                        c.add("cmqt_flush_send_errors_total", 1);
                        return 503;
                    }
                }
            }
        }
    }
    c.add("cmqt_flush_messages_total", 1);
    200
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bearer() {
        let t = vec![("srv".to_string(), "abcdefghijklmnop".to_string())];
        assert_eq!(authorized(Some("Bearer abcdefghijklmnop"), &t), Some("srv"));
        assert_eq!(authorized(Some("Bearer abcdefghijklmnoq"), &t), None);
        assert_eq!(authorized(Some("abcdefghijklmnop"), &t), None);
        assert_eq!(authorized(None, &t), None);
    }

    #[test]
    fn sd_paths() {
        assert_eq!(sd_path("/v1/srv/cpu"), Some(("srv", "cpu")));
        assert_eq!(sd_path("/sd/v1/office.mac.vm.srv/disk-io?x=1"), Some(("office.mac.vm.srv", "disk-io")));
        assert_eq!(sd_path("/v1/metrics"), None);
        assert_eq!(sd_path("/v1/a/b/c"), None);
    }
}

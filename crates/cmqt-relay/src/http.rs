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
const DEFAULT_QUEUE: &str = "relay_datagrams";

/// Token file: one `name:token` or bare `token` per line; `#` comments.
pub fn load_tokens(path: &str) -> Result<Vec<String>, String> {
    let s = std::fs::read_to_string(path).map_err(|e| format!("tokens {path}: {e}"))?;
    let v: Vec<String> = s
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(|l| l.rsplit(':').next().unwrap_or(l).to_string())
        .filter(|t| t.len() >= 16)
        .collect();
    if v.is_empty() {
        return Err(format!("tokens {path}: no token of at least 16 characters"));
    }
    Ok(v)
}

fn eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |d, (x, y)| d | (x ^ y)) == 0
}

pub fn authorized(header: Option<&str>, tokens: &[String]) -> bool {
    let Some(t) = header.and_then(|h| h.strip_prefix("Bearer ")) else { return false };
    // Check every token (no early exit) so timing does not reveal which matched.
    tokens.iter().fold(false, |ok, k| eq(k.as_bytes(), t.trim().as_bytes()) | ok)
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
    if let Some(w) = cfg.watch.clone() {
        crate::watch::spawn(w)?;
    }
    // Resident memory and its peak into the log every 5 minutes (the host's own tools do not see a site's process).
    std::thread::spawn(|| loop {
        if let Ok(st) = std::fs::read_to_string("/proc/self/status") {
            let f = |k: &str| st.lines().find(|l| l.starts_with(k)).unwrap_or("").split_whitespace().collect::<Vec<_>>().join(" ");
            eprintln!("cmqt flush: mem {} | {}", f("VmRSS:"), f("VmHWM:"));
        }
        std::thread::sleep(Duration::from_secs(300));
    });
    let server = tiny_http::Server::http(&listen).map_err(|e| format!("http listen {listen}: {e}"))?;
    eprintln!("cmqt flush: OTLP/HTTP on {listen}");
    let server = Arc::new(server);
    let state = Arc::new(Mutex::new(State { sealer, sink }));
    let tokens = Arc::new(tokens);
    let mut hs = Vec::new();
    for _ in 0..3 {
        let (server, state, tokens, c) = (server.clone(), state.clone(), tokens.clone(), counters.clone());
        hs.push(std::thread::spawn(move || {
            for rq in server.incoming_requests() {
                handle(rq, &tokens, &state, &c);
            }
        }));
    }
    for h in hs {
        let _ = h.join();
    }
    Err("http server ended".into())
}

fn handle(mut rq: tiny_http::Request, tokens: &[String], state: &Mutex<State>, c: &Counters) {
    let hdr = |n: &'static str| rq.headers().iter().find(|h| h.field.equiv(n)).map(|h| h.value.as_str().to_string());
    let auth = hdr("Authorization");
    let gz = hdr("Content-Encoding").is_some_and(|e| e.eq_ignore_ascii_case("gzip"));
    let path_ok = rq.url().split('?').next().is_some_and(|p| p.ends_with("/v1/metrics"));
    let status = if !authorized(auth.as_deref(), tokens) {
        c.add("cmqt_flush_http_unauthorized_total", 1);
        401
    } else if *rq.method() != tiny_http::Method::Post || !path_ok {
        404
    } else {
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
        let t = vec!["abcdefghijklmnop".to_string()];
        assert!(authorized(Some("Bearer abcdefghijklmnop"), &t));
        assert!(!authorized(Some("Bearer abcdefghijklmnoq"), &t));
        assert!(!authorized(Some("abcdefghijklmnop"), &t));
        assert!(!authorized(None, &t));
    }
}

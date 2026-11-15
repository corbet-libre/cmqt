//! `cmqt flush` input 2: OTLP/HTTP (`POST .../v1/metrics`, protobuf or JSON,
//! optionally gzip) behind a TLS-terminating proxy, one bearer token per
//! sender. No queue: the request is converted, paced and sealed before the
//! 200 is returned, so an error status makes the sender retry.

use crate::flush::prepare;
use crate::metrics::Counters;
use crate::wire::{Bucket, Sealer};
use std::io::Read;
use std::net::UdpSocket;
use std::sync::{Arc, Mutex};

/// Largest body after decompression (an OTLP batch is far below this).
const MAX_BODY: u64 = 8 * 1024 * 1024;

pub struct Out {
    pub sock: UdpSocket,
    pub sealer: Sealer,
    pub bucket: Bucket,
}

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

pub fn serve(listen: &str, tokens: Vec<String>, out: Out, c: Arc<Counters>) -> Result<(), String> {
    let server = tiny_http::Server::http(listen).map_err(|e| format!("http listen {listen}: {e}"))?;
    eprintln!("cmqt flush: OTLP/HTTP on {listen}");
    // Resident memory and its peak into the log every 5 minutes (the host's own tools do not see a site's process).
    std::thread::spawn(|| loop {
        if let Ok(st) = std::fs::read_to_string("/proc/self/status") {
            let f = |k: &str| st.lines().find(|l| l.starts_with(k)).unwrap_or("").split_whitespace().collect::<Vec<_>>().join(" ");
            eprintln!("cmqt flush: mem {} | {}", f("VmRSS:"), f("VmHWM:"));
        }
        std::thread::sleep(std::time::Duration::from_secs(300));
    });
    let server = Arc::new(server);
    let out = Arc::new(Mutex::new(out));
    let tokens = Arc::new(tokens);
    let mut hs = Vec::new();
    for _ in 0..3 {
        let (server, out, tokens, c) = (server.clone(), out.clone(), tokens.clone(), c.clone());
        hs.push(std::thread::spawn(move || {
            for rq in server.incoming_requests() {
                handle(rq, &tokens, &out, &c);
            }
        }));
    }
    for h in hs {
        let _ = h.join();
    }
    Err("http server ended".into())
}

fn handle(mut rq: tiny_http::Request, tokens: &[String], out: &Mutex<Out>, c: &Counters) {
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
            match prepare(&body, c) {
                Err(e) => {
                    eprintln!("cmqt flush: http: dropping request: {e}");
                    c.add("cmqt_flush_bad_messages_total", 1);
                    400
                }
                Ok(dg) => send(&dg, out, c),
            }
        }
    };
    let _ = rq.respond(tiny_http::Response::empty(status));
}

fn send(dg: &[Vec<u8>], out: &Mutex<Out>, c: &Counters) -> u16 {
    let mut o = out.lock().unwrap_or_else(|p| p.into_inner());
    for p in dg {
        let wait = o.bucket.take();
        if !wait.is_zero() {
            std::thread::sleep(wait);
        }
        let sealed = o.sealer.seal(p);
        match o.sock.send(&sealed) {
            Ok(_) => {
                c.add("cmqt_flush_datagrams_total", 1);
                c.add("cmqt_flush_bytes_total", sealed.len() as u64);
            }
            // ICMP port unreachable from a stopped receiver: the datagram is lost like any other
            // during an outage (counted); refusing the request would only make senders pile up.
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => {
                c.add("cmqt_flush_send_errors_total", 1);
            }
            Err(e) => {
                eprintln!("cmqt flush: send: {e}");
                c.add("cmqt_flush_send_errors_total", 1);
                return 503;
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

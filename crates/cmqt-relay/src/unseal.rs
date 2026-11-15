//! `cmqt unseal <config.toml>`: UDP/IPv6 in, verified lines to VictoriaMetrics out.

use crate::metrics::{self, Counters};
use crate::wire::{self, OpenError, Replay, Verdict};
use serde::Deserialize;
use socket2::{Domain, Socket, Type};
use std::net::SocketAddr;
use std::sync::mpsc::{RecvTimeoutError, SyncSender, sync_channel};
use std::time::{Duration, Instant};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// UDP listen address, e.g. `[::]:9514`.
    pub listen: String,
    pub key_file: String,
    /// Socket receive buffer in bytes; the kernel clamps it to `net.core.rmem_max`.
    #[serde(default = "default_rcvbuf")]
    pub rcvbuf: usize,
    #[serde(default = "default_forward")]
    pub forward_url: String,
    /// Prometheus text endpoint, e.g. `127.0.0.1:9515`.
    pub metrics_listen: Option<String>,
    /// Forward when this many bytes are collected, or after `batch_ms`.
    #[serde(default = "default_batch_bytes")]
    pub batch_bytes: usize,
    #[serde(default = "default_batch_ms")]
    pub batch_ms: u64,
}
fn default_rcvbuf() -> usize { 8 << 20 }
fn default_forward() -> String { "http://127.0.0.1:8428/influx/write".into() }
fn default_batch_bytes() -> usize { 64 << 10 }
fn default_batch_ms() -> u64 { 200 }

pub const COUNTERS: &[(&str, &str)] = &[
    ("cmqt_unseal_received_total", "Datagrams that authenticated and were accepted."),
    ("cmqt_unseal_gap_total", "Datagrams skipped by sequence jumps (presumed lost)."),
    ("cmqt_unseal_late_total", "Datagrams that arrived after a gap was counted; lost = gap - late."),
    ("cmqt_unseal_auth_failures_total", "Datagrams that failed authentication."),
    ("cmqt_unseal_replays_total", "Authenticated datagrams rejected as duplicates or too old."),
    ("cmqt_unseal_malformed_total", "Datagrams too short or of an unknown version."),
    ("cmqt_unseal_forwarded_total", "Datagrams whose lines VictoriaMetrics accepted."),
    ("cmqt_unseal_forward_errors_total", "Datagrams whose lines could not be forwarded after one retry."),
    ("cmqt_unseal_queue_dropped_total", "Datagrams dropped because the forward queue was full."),
];

pub fn run(cfg: Config) -> Result<(), String> {
    let aead = wire::load_key(&cfg.key_file)?;
    let counters = Counters::new(COUNTERS);
    if let Some(m) = &cfg.metrics_listen {
        metrics::serve(m, counters.clone())?;
    }
    let addr: SocketAddr = cfg.listen.parse().map_err(|e| format!("listen {}: {e}", cfg.listen))?;
    let sock = Socket::new(Domain::for_address(addr), Type::DGRAM, None).map_err(|e| e.to_string())?;
    sock.set_recv_buffer_size(cfg.rcvbuf).map_err(|e| e.to_string())?;
    sock.bind(&addr.into()).map_err(|e| format!("bind {addr}: {e}"))?;
    let got = sock.recv_buffer_size().unwrap_or(0);
    // Linux reports twice the effective size.
    eprintln!("cmqt unseal: listening on {addr}, receive buffer {} bytes (requested {})", got / 2, cfg.rcvbuf);
    if got / 2 < cfg.rcvbuf {
        eprintln!("cmqt unseal: WARNING receive buffer clamped; raise net.core.rmem_max to at least {}", cfg.rcvbuf);
    }
    let sock: std::net::UdpSocket = sock.into();

    let (tx, rx) = sync_channel::<Vec<u8>>(4096);
    let c = counters.clone();
    let (url, max, wait) = (cfg.forward_url.clone(), cfg.batch_bytes, Duration::from_millis(cfg.batch_ms));
    std::thread::spawn(move || forwarder(rx, url, max, wait, c));

    let mut replay = Replay::default();
    let mut buf = [0u8; 2048];
    loop {
        let n = match sock.recv_from(&mut buf) {
            Ok((n, _)) => n,
            Err(e) => {
                eprintln!("cmqt unseal: recv: {e}");
                continue;
            }
        };
        accept(&aead, &mut replay, &counters, &buf[..n], &tx);
    }
}

/// Handle one datagram: authenticate, account, queue for forwarding.
pub fn accept(aead: &chacha20poly1305::ChaCha20Poly1305, replay: &mut Replay, c: &Counters, d: &[u8], tx: &SyncSender<Vec<u8>>) {
    match wire::open(aead, d) {
        Err(OpenError::Auth) => c.add("cmqt_unseal_auth_failures_total", 1),
        Err(_) => c.add("cmqt_unseal_malformed_total", 1), // short, unknown version, or a bad zstd frame
        Ok(o) => match replay.check(o.prefix, o.seq) {
            Verdict::Replay => c.add("cmqt_unseal_replays_total", 1),
            v => {
                c.add("cmqt_unseal_received_total", 1);
                match v {
                    Verdict::New { lost } => c.add("cmqt_unseal_gap_total", lost),
                    _ => c.add("cmqt_unseal_late_total", 1),
                }
                if tx.try_send(o.payload).is_err() {
                    c.add("cmqt_unseal_queue_dropped_total", 1);
                }
            }
        },
    }
}

/// Collect payloads into one HTTP body and POST it to VictoriaMetrics.
fn forwarder(rx: std::sync::mpsc::Receiver<Vec<u8>>, url: String, max: usize, wait: Duration, c: std::sync::Arc<Counters>) {
    let agent: ureq::Agent = ureq::Agent::config_builder().timeout_global(Some(Duration::from_secs(10))).build().into();
    let mut body: Vec<u8> = Vec::new();
    let mut parts = 0u64;
    let mut first = Instant::now();
    loop {
        let r = rx.recv_timeout(wait.saturating_sub(first.elapsed()).max(Duration::from_millis(1)));
        match r {
            Ok(p) => {
                if body.is_empty() {
                    first = Instant::now();
                }
                body.extend_from_slice(&p);
                parts += 1;
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return,
        }
        if !body.is_empty() && (body.len() >= max || first.elapsed() >= wait) {
            let ok = (0..2).any(|i| {
                if i == 1 {
                    std::thread::sleep(Duration::from_millis(200));
                }
                agent.post(&url).send(&body[..]).is_ok()
            });
            c.add(if ok { "cmqt_unseal_forwarded_total" } else { "cmqt_unseal_forward_errors_total" }, parts);
            body.clear();
            parts = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::Sealer;
    use chacha20poly1305::{ChaCha20Poly1305, KeyInit};
    use std::sync::mpsc::sync_channel;

    #[test]
    fn counts_received_gaps_auth_and_replays() {
        let aead = || ChaCha20Poly1305::new_from_slice(&[3u8; 32]).unwrap();
        let mut s = Sealer::with_prefix(aead(), 9);
        let ds: Vec<_> = (0..6).map(|i| s.seal(format!("m value={i}\n").as_bytes())).collect();
        let c = Counters::new(COUNTERS);
        let (tx, rx) = sync_channel(64);
        let mut r = Replay::default();
        let a = aead();
        for i in [0, 1, 4, 4, 3] {
            accept(&a, &mut r, &c, &ds[i], &tx);
        }
        let mut bad = ds[5].clone();
        bad[20] ^= 1;
        accept(&a, &mut r, &c, &bad, &tx);
        accept(&a, &mut r, &c, &[1, 2, 3], &tx);
        assert_eq!(c.get("cmqt_unseal_received_total"), 4);
        assert_eq!(c.get("cmqt_unseal_gap_total"), 2);
        assert_eq!(c.get("cmqt_unseal_late_total"), 1);
        assert_eq!(c.get("cmqt_unseal_replays_total"), 1);
        assert_eq!(c.get("cmqt_unseal_auth_failures_total"), 1);
        assert_eq!(c.get("cmqt_unseal_malformed_total"), 1);
        assert_eq!(rx.try_iter().count(), 4);
    }

    #[test]
    fn zstd_datagram_is_decompressed_and_a_bomb_is_refused() {
        let aead = || ChaCha20Poly1305::new_from_slice(&[3u8; 32]).unwrap();
        let mut s = Sealer::with_prefix(aead(), 1);
        let c = Counters::new(COUNTERS);
        let (tx, rx) = sync_channel(8);
        let mut r = Replay::default();
        let line = b"m value=1 1\n".repeat(40);
        accept(&aead(), &mut r, &c, &s.seal_zstd(&wire::compress(&line)), &tx);
        assert_eq!(rx.try_recv().unwrap(), line);
        let bomb = wire::compress(&vec![b'a'; 1 << 20]);
        accept(&aead(), &mut r, &c, &s.seal_zstd(&bomb), &tx);
        assert_eq!(c.get("cmqt_unseal_malformed_total"), 1);
        assert!(rx.try_recv().is_err());
    }
}

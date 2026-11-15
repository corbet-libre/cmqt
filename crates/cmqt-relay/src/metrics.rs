//! Process counters in Prometheus text format on a plain local TCP port.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct Counters(Vec<(&'static str, &'static str, AtomicU64)>);

impl Counters {
    pub fn new(items: &[(&'static str, &'static str)]) -> Arc<Self> {
        Arc::new(Counters(items.iter().map(|(n, h)| (*n, *h, AtomicU64::new(0))).collect()))
    }
    pub fn add(&self, name: &str, n: u64) {
        match self.0.iter().find(|c| c.0 == name) {
            Some(c) => {
                c.2.fetch_add(n, Ordering::Relaxed);
            }
            None => debug_assert!(false, "unknown counter {name}"),
        }
    }
    #[cfg(test)]
    pub fn get(&self, name: &str) -> u64 {
        self.0.iter().find(|c| c.0 == name).map_or(0, |c| c.2.load(Ordering::Relaxed))
    }
    pub fn render(&self) -> String {
        let mut o = String::new();
        for (n, h, v) in &self.0 {
            o.push_str(&format!("# HELP {n} {h}\n# TYPE {n} counter\n{n} {}\n", v.load(Ordering::Relaxed)));
        }
        o
    }
}

/// Serve `GET /metrics` (any path) from a background thread.
pub fn serve(addr: &str, counters: Arc<Counters>) -> Result<(), String> {
    let l = TcpListener::bind(addr).map_err(|e| format!("metrics listen {addr}: {e}"))?;
    std::thread::spawn(move || {
        for s in l.incoming().flatten() {
            let mut s = s;
            let _ = s.set_read_timeout(Some(std::time::Duration::from_secs(2)));
            let mut buf = [0u8; 1024];
            let _ = s.read(&mut buf);
            let body = counters.render();
            let _ = write!(
                s,
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
    Ok(())
}

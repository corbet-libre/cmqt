//! Resource governors for `cmqt flush` on a plan with hard limits (charter D76): stay close to the
//! CPU and RAM allowance without touching it. The decisions are pure functions over injected
//! measurements; only `sample` reads `/proc` (our own user's relay processes: this one and Mosquitto).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};

/// Linux USER_HZ (fixed at 100 on every supported architecture).
const TICKS: f64 = 100.0;
/// Headroom kept below the RAM target for allocator slack and the listener buffers.
pub const RAM_MARGIN: usize = 8 << 20;
/// Request pacing in milliseconds, set by the CPU governor and slept by the ingest handler.
pub static DELAY_MS: AtomicU64 = AtomicU64::new(0);
const MAX_DELAY_MS: u64 = 3000;

pub fn delay() -> std::time::Duration {
    std::time::Duration::from_millis(DELAY_MS.load(Ordering::Relaxed))
}
pub fn throttled() -> bool {
    DELAY_MS.load(Ordering::Relaxed) > 0
}

/// Sliding window of (seconds, cumulative CPU seconds) samples.
pub struct CpuWindow {
    span: f64,
    s: VecDeque<(f64, f64)>,
}

impl CpuWindow {
    pub fn new(span_secs: f64) -> Self {
        CpuWindow { span: span_secs, s: VecDeque::new() }
    }
    pub fn push(&mut self, t: f64, cpu: f64) {
        self.s.push_back((t, cpu));
        while self.s.len() > 2 && t - self.s[1].0 >= self.span {
            self.s.pop_front();
        }
    }
    /// CPU share of one core over the window (0 until two samples exist).
    pub fn share(&self) -> f64 {
        match (self.s.front(), self.s.back()) {
            (Some(a), Some(b)) if b.0 > a.0 => ((b.1 - a.1).max(0.0)) / (b.0 - a.0),
            _ => 0.0,
        }
    }
}

/// Next pacing delay: grows in proportion to the overshoot, decays geometrically below the target.
pub fn next_delay_ms(prev: u64, share: f64, target: f64) -> u64 {
    if target <= 0.0 {
        return prev;
    }
    if share > target {
        let step = 20.0 + (share - target) / target * 400.0;
        (prev + step as u64).min(MAX_DELAY_MS)
    } else if share < target * 0.9 {
        let d = prev * 7 / 10;
        if d < 10 { 0 } else { d }
    } else {
        prev
    }
}

/// History cache cap so that the total relay RSS lands on `target`: everything that is not cache
/// stays, the cache gets the rest, never less than `floor` and never more than `ceiling` (0 = none).
pub fn cache_cap(rss_total: usize, cache_bytes: usize, target: usize, floor: usize, ceiling: usize) -> usize {
    let base = rss_total.saturating_sub(cache_bytes);
    let cap = target.saturating_sub(RAM_MARGIN).saturating_sub(base).max(floor);
    if ceiling > 0 { cap.min(ceiling).max(floor.min(ceiling)) } else { cap }
}

/// Cumulative CPU seconds (utime + stime) from the text of `/proc/<pid>/stat`.
pub fn parse_stat_cpu(stat: &str) -> Option<f64> {
    // The command name may contain spaces and parentheses; the fields start after the last ')'.
    let rest = &stat[stat.rfind(')')? + 1..];
    let f: Vec<&str> = rest.split_whitespace().collect();
    // After ')': state is f[0]; utime and stime are fields 14 and 15 overall, f[11] and f[12].
    let (u, s): (f64, f64) = (f.get(11)?.parse().ok()?, f.get(12)?.parse().ok()?);
    Some((u + s) / TICKS)
}

/// `(Name, uid, VmRSS in bytes)` from the text of `/proc/<pid>/status`.
pub fn parse_status(st: &str) -> Option<(String, u32, usize)> {
    let get = |k: &str| st.lines().find_map(|l| l.strip_prefix(k)).map(str::trim);
    let name = get("Name:")?.to_string();
    let uid = get("Uid:")?.split_whitespace().next()?.parse().ok()?;
    let rss = get("VmRSS:").and_then(|v| v.split_whitespace().next()?.parse::<usize>().ok()).unwrap_or(0) * 1024;
    Some((name, uid, rss))
}

/// Cumulative CPU seconds and RSS bytes of this process and the user's Mosquitto processes.
pub fn sample() -> (f64, usize) {
    let me = std::process::id().to_string();
    let uid = std::fs::read_to_string("/proc/self/status").ok().and_then(|s| parse_status(&s)).map(|t| t.1);
    let (mut cpu, mut rss) = (0.0, 0usize);
    let Ok(rd) = std::fs::read_dir("/proc") else { return (0.0, 0) };
    for e in rd.flatten() {
        let pid = e.file_name().to_string_lossy().into_owned();
        if !pid.bytes().all(|b| b.is_ascii_digit()) {
            continue;
        }
        let Some((name, u, r)) = std::fs::read_to_string(format!("/proc/{pid}/status")).ok().and_then(|s| parse_status(&s)) else { continue };
        if Some(u) != uid || !(pid == me || name.starts_with("mosquitto")) {
            continue;
        }
        if let Some(c) = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok().and_then(|s| parse_stat_cpu(&s)) {
            cpu += c;
            rss += r;
        }
    }
    (cpu, rss)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_share() {
        let mut w = CpuWindow::new(30.0);
        assert_eq!(w.share(), 0.0);
        for i in 0..=60 {
            w.push(i as f64, i as f64 * 0.5);
        }
        assert!((w.share() - 0.5).abs() < 1e-9);
        // The load stops: after one window the share is 0.
        for i in 61..=100 {
            w.push(i as f64, 30.0);
        }
        assert!(w.share() < 1e-9);
    }

    #[test]
    fn window_ignores_counter_reset() {
        let mut w = CpuWindow::new(30.0);
        w.push(0.0, 10.0);
        w.push(10.0, 2.0);
        assert_eq!(w.share(), 0.0);
    }

    #[test]
    fn delay_rises_above_target_and_decays_below() {
        let mut d = 0;
        for _ in 0..200 {
            d = next_delay_ms(d, 0.5, 0.23);
        }
        assert_eq!(d, MAX_DELAY_MS);
        assert!(next_delay_ms(0, 0.30, 0.23) > 0);
        assert_eq!(next_delay_ms(500, 0.23 * 0.95, 0.23), 500); // dead band
        let mut d = 1000;
        for _ in 0..20 {
            d = next_delay_ms(d, 0.05, 0.23);
        }
        assert_eq!(d, 0);
        assert_eq!(next_delay_ms(0, 0.2299, 0.23), 0);
    }

    #[test]
    fn controller_converges_on_a_simulated_load() {
        // Each request costs 10 ms CPU; the offered rate is 100 requests/s unthrottled.
        let target = 0.23;
        let (mut d, mut share) = (0u64, 0.0);
        for _ in 0..300 {
            let rate = 1000.0 / (10.0 + d as f64); // requests/s with `d` ms pacing per request (one stream)
            share = rate * 0.010 * 8.0; // 8 parallel senders
            d = next_delay_ms(d, share, target);
        }
        assert!(share <= target * 1.05, "share {share}");
        assert!(share >= target * 0.5, "share {share}");
    }

    #[test]
    fn cache_takes_the_rest_of_the_ram() {
        let mib = 1 << 20;
        // 20 MiB relay without cache, target 240: cache 212 MiB (8 MiB margin).
        assert_eq!(cache_cap(20 * mib + 50 * mib, 50 * mib, 240 * mib, 16 * mib, 0), 212 * mib);
        // Over the target the cache shrinks, but not below the floor.
        assert_eq!(cache_cap(300 * mib, 100 * mib, 240 * mib, 16 * mib, 0), 32 * mib);
        assert_eq!(cache_cap(400 * mib, 100 * mib, 240 * mib, 16 * mib, 0), 16 * mib);
        // A ceiling wins over free RAM.
        assert_eq!(cache_cap(20 * mib, 0, 240 * mib, 16 * mib, 100 * mib), 100 * mib);
    }

    #[test]
    fn proc_parsing() {
        let stat = "123 (mosquitto x) y) S 1 1 1 0 -1 4194560 100 0 0 0 250 50 0 0 20 0 1 0 5 1000 100";
        assert_eq!(parse_stat_cpu(stat), Some(3.0));
        let st = "Name:\tmosquitto\nUid:\t1000\t1000\t1000\t1000\nVmRSS:\t   6144 kB\n";
        assert_eq!(parse_status(st), Some(("mosquitto".into(), 1000, 6144 * 1024)));
    }

    #[test]
    fn sample_sees_this_process() {
        let (cpu, rss) = sample();
        assert!(rss > 0 && cpu >= 0.0);
    }
}

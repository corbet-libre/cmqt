//! RAM history of what passes through `cmqt flush` (charter D70): one ring of compressed blocks per
//! series, oldest block evicted globally when the byte cap is reached. Served as a small
//! `query_range` subset (plain selectors with `=` and `!=` matchers).

use std::collections::{BTreeMap, HashMap, VecDeque};

const BLOCK_SAMPLES: usize = 256;

struct Block {
    first_ts: i64, // ms
    n: usize,
    last_ts: i64,
    last_bits: u64,
    data: Vec<u8>, // samples after the first: varint(zigzag dt), varint(bits xor)
    first_bits: u64,
}

struct Series {
    labels: BTreeMap<String, String>,
    blocks: VecDeque<Block>,
}

#[derive(Default)]
pub struct Cache {
    series: HashMap<String, Series>,
    bytes: usize,
    pub cap: usize,
    pub evicted_blocks: u64,
}

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}
fn get_varint(d: &[u8], i: &mut usize) -> u64 {
    let (mut v, mut s) = (0u64, 0);
    while *i < d.len() {
        let b = d[*i];
        *i += 1;
        v |= u64::from(b & 0x7f) << s;
        if b & 0x80 == 0 {
            break;
        }
        s += 7;
    }
    v
}
fn zz(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}
fn unzz(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

/// Split an escaped line-protocol head at unescaped `sep`.
fn split_unescaped(s: &str, sep: char) -> Vec<String> {
    let (mut out, mut cur, mut esc) = (Vec::new(), String::new(), false);
    for c in s.chars() {
        if esc {
            cur.push(c);
            esc = false;
        } else if c == '\\' {
            esc = true;
        } else if c == sep {
            out.push(std::mem::take(&mut cur));
        } else {
            cur.push(c);
        }
    }
    out.push(cur);
    out
}

/// `name,t=v,... value=X ts_ns` to (key, labels, ts_ms, value).
pub fn parse_line(l: &str) -> Option<(String, BTreeMap<String, String>, i64, f64)> {
    let mut parts = Vec::new();
    let (mut start, mut esc) = (0, false);
    for (i, c) in l.char_indices() {
        if esc {
            esc = false;
        } else if c == '\\' {
            esc = true;
        } else if c == ' ' {
            parts.push(&l[start..i]);
            start = i + 1;
        }
    }
    parts.push(&l[start..]);
    if parts.len() != 3 {
        return None;
    }
    let value: f64 = parts[1].strip_prefix("value=")?.parse().ok()?;
    let ts: i64 = parts[2].parse::<i64>().ok()? / 1_000_000;
    let head = split_unescaped(parts[0], ',');
    let mut labels = BTreeMap::new();
    labels.insert("__name__".to_string(), head[0].clone());
    for kv in &head[1..] {
        let (k, v) = kv.split_once('=')?;
        labels.insert(k.to_string(), v.to_string());
    }
    let key = labels.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("\u{1}");
    Some((key, labels, ts, value))
}

pub type RangeResult = Vec<(BTreeMap<String, String>, Vec<(i64, f64)>)>;

impl Cache {
    pub fn new(cap: usize) -> Self {
        Cache { cap, ..Default::default() }
    }
    pub fn bytes(&self) -> usize {
        self.bytes
    }
    pub fn series_count(&self) -> usize {
        self.series.len()
    }
    pub fn insert_line(&mut self, l: &str) {
        if let Some((key, labels, ts, v)) = parse_line(l) {
            self.insert(key, labels, ts, v);
        }
    }
    fn insert(&mut self, key: String, labels: BTreeMap<String, String>, ts: i64, v: f64) {
        let s = self.series.entry(key.clone()).or_insert_with(|| {
            self.bytes += key.len() + 64;
            Series { labels, blocks: VecDeque::new() }
        });
        let bits = v.to_bits();
        if let Some(b) = s.blocks.back_mut().filter(|b| b.n < BLOCK_SAMPLES && ts >= b.last_ts) {
            let before = b.data.len();
            put_varint(&mut b.data, zz(ts - b.last_ts));
            put_varint(&mut b.data, bits ^ b.last_bits);
            b.last_ts = ts;
            b.last_bits = bits;
            b.n += 1;
            self.bytes += b.data.len() - before;
        } else {
            s.blocks.push_back(Block { first_ts: ts, n: 1, last_ts: ts, last_bits: bits, data: Vec::new(), first_bits: bits });
            self.bytes += 96;
        }
        // While the CPU governor throttles, eviction (a scan over all series) is deferred up to 2% over the cap.
        let lim = if crate::govern::throttled() { self.cap + self.cap / 50 } else { self.cap };
        while self.bytes > lim && self.evict_one() {}
    }
    /// Drop the oldest block of the series whose oldest block is oldest.
    fn evict_one(&mut self) -> bool {
        let Some(k) = self.series.iter().filter(|(_, s)| !s.blocks.is_empty()).min_by_key(|(_, s)| s.blocks[0].first_ts).map(|(k, _)| k.clone()) else {
            return false;
        };
        let Some(s) = self.series.get_mut(&k) else { return false };
        if let Some(b) = s.blocks.pop_front() {
            self.bytes = self.bytes.saturating_sub(b.data.len() + 96);
            self.evicted_blocks += 1;
        }
        if s.blocks.is_empty() {
            self.bytes = self.bytes.saturating_sub(k.len() + 64);
            self.series.remove(&k);
        }
        true
    }
    pub fn oldest_ms(&self) -> Option<i64> {
        self.series.values().filter_map(|s| s.blocks.front().map(|b| b.first_ts)).min()
    }
    /// Series matching all matchers: (name, value, negate).
    pub fn range(&self, m: &[(String, String, bool)], start: i64, end: i64, step: i64) -> RangeResult {
        let mut out = Vec::new();
        for s in self.series.values() {
            if !m.iter().all(|(k, v, neg)| (s.labels.get(k).map(String::as_str).unwrap_or("") == v) != *neg) {
                continue;
            }
            let mut samples = Vec::new();
            for b in &s.blocks {
                if b.last_ts < start - 300_000 || b.first_ts > end {
                    continue;
                }
                samples.push((b.first_ts, f64::from_bits(b.first_bits)));
                let (mut i, mut ts, mut bits) = (0, b.first_ts, b.first_bits);
                while i < b.data.len() {
                    ts += unzz(get_varint(&b.data, &mut i));
                    bits ^= get_varint(&b.data, &mut i);
                    samples.push((ts, f64::from_bits(bits)));
                }
            }
            let (mut pts, mut j, mut last) = (Vec::new(), 0, None);
            let mut t = start;
            while t <= end {
                while j < samples.len() && samples[j].0 <= t {
                    last = Some(samples[j]);
                    j += 1;
                }
                if let Some((ts, v)) = last.filter(|(ts, _)| t - ts <= 300_000) {
                    let _ = ts;
                    pts.push((t, v));
                }
                t += step.max(1);
            }
            if !pts.is_empty() {
                out.push((s.labels.clone(), pts));
            }
        }
        out
    }
}

/// `name{a="b",c!="d"}` or bare `name` or `{a="b"}`.
pub fn parse_selector(q: &str) -> Option<Vec<(String, String, bool)>> {
    let q = q.trim();
    let (name, rest) = match q.find('{') {
        Some(i) => (&q[..i], Some(q[i + 1..].strip_suffix('}')?)),
        None => (q, None),
    };
    let mut m = Vec::new();
    if !name.is_empty() {
        m.push(("__name__".to_string(), name.to_string(), false));
    }
    if let Some(r) = rest {
        for part in r.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            let (k, v, neg) = if let Some((k, v)) = part.split_once("!=") { (k, v, true) } else { let (k, v) = part.split_once('=')?; (k, v, false) };
            m.push((k.trim().to_string(), v.trim().trim_matches('"').to_string(), neg));
        }
    }
    (!m.is_empty()).then_some(m)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn roundtrip_range_and_eviction() {
        let mut c = Cache::new(1 << 20);
        for i in 0..1000 {
            c.insert_line(&format!("cpu,instance=a\\ b,job=n value={} {}", i as f64 * 0.5, (1000 + i * 10) * 1_000_000_000i64 / 1000 * 1000));
        }
        let m = parse_selector(r#"cpu{instance="a b"}"#).unwrap();
        let r = c.range(&m, 1_000_000, 10_990_000, 10_000);
        assert_eq!(r.len(), 1);
        assert!(r[0].1.len() > 100);
        assert_eq!(r[0].0["job"], "n");
        let neg = parse_selector(r#"cpu{job!="n"}"#).unwrap();
        assert!(c.range(&neg, 0, 2_000_000_000, 1000).is_empty());
        let mut small = Cache::new(4096);
        for i in 0..5000 {
            small.insert_line(&format!("m{} value={} {}", i % 50, i, i as i64 * 1_000_000_000));
        }
        assert!(small.bytes() <= 4096 + 400);
        assert!(small.evicted_blocks > 0);
    }
}

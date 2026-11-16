//! RAM history of the box's OUTPUT (charter D76): every message the relay receives back from the
//! box over the tile link (tiles, alert/attention/focus states, results), kept per topic as
//! zstd-compressed blocks; the oldest block overall is evicted when the byte cap is reached. Raw
//! device readings are never cached here, they only pass through the queue to the box.

use std::collections::{BTreeMap, VecDeque};

/// Messages per compressed block.
const BLOCK_MSGS: usize = 32;
/// Per-block and per-topic bookkeeping charged to the cap.
const BLOCK_OVERHEAD: usize = 64;
const TOPIC_OVERHEAD: usize = 96;

struct Block {
    first_ts: i64, // ms
    last_ts: i64,
    n: usize,
    data: Vec<u8>, // zstd of: varint(dt from first) varint(len) bytes, repeated
}

#[derive(Default)]
struct Topic {
    blocks: VecDeque<Block>,
    open: Vec<(i64, Vec<u8>)>,
    last: Vec<u8>,
}

#[derive(Default)]
pub struct Cache {
    topics: BTreeMap<String, Topic>,
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

fn open_bytes(t: &Topic) -> usize {
    t.open.iter().map(|(_, p)| p.len() + 16).sum::<usize>() + t.last.len()
}

/// Compress entries into one block body: `(first_ts, last_ts, n, data)`.
pub fn seal_entries(open: &[(i64, Vec<u8>)]) -> (i64, i64, usize, Vec<u8>) {
    let b = seal(open);
    (b.first_ts, b.last_ts, b.n, b.data)
}

/// Inverse of `seal_entries`.
pub fn unseal_entries(first_ts: i64, n: usize, data: Vec<u8>) -> Vec<(i64, Vec<u8>)> {
    unseal(&Block {
        first_ts,
        last_ts: first_ts,
        n,
        data,
    })
}

fn seal(open: &[(i64, Vec<u8>)]) -> Block {
    let first = open[0].0;
    let mut raw = Vec::new();
    for (ts, p) in open {
        put_varint(&mut raw, (*ts - first).max(0) as u64);
        put_varint(&mut raw, p.len() as u64);
        raw.extend_from_slice(p);
    }
    let data = zstd::bulk::compress(&raw, 3).unwrap_or(raw);
    Block {
        first_ts: first,
        last_ts: open[open.len() - 1].0,
        n: open.len(),
        data,
    }
}

fn unseal(b: &Block) -> Vec<(i64, Vec<u8>)> {
    let raw = zstd::bulk::decompress(&b.data, 1 << 24).unwrap_or_else(|_| b.data.clone());
    let (mut i, mut out) = (0, Vec::with_capacity(b.n));
    while i < raw.len() && out.len() < b.n {
        let dt = get_varint(&raw, &mut i) as i64;
        let len = get_varint(&raw, &mut i) as usize;
        let Some(p) = raw.get(i..i + len) else { break };
        out.push((b.first_ts + dt, p.to_vec()));
        i += len;
    }
    out
}

impl Cache {
    pub fn new(cap: usize) -> Self {
        Cache {
            cap,
            ..Default::default()
        }
    }
    pub fn bytes(&self) -> usize {
        self.bytes
    }
    pub fn series_count(&self) -> usize {
        self.topics.len()
    }
    pub fn oldest_ms(&self) -> Option<i64> {
        self.topics
            .values()
            .filter_map(|t| {
                t.blocks
                    .front()
                    .map(|b| b.first_ts)
                    .or_else(|| t.open.first().map(|o| o.0))
            })
            .min()
    }

    /// Store one message of the box (`ts` in ms). A payload equal to the topic's last one is not news
    /// (the retained set replayed after a reconnect).
    pub fn insert(&mut self, topic: &str, ts: i64, payload: &[u8]) {
        let t = match self.topics.get_mut(topic) {
            Some(t) => t,
            None => {
                self.bytes += topic.len() + TOPIC_OVERHEAD;
                self.topics.entry(topic.to_string()).or_default()
            }
        };
        if !t.last.is_empty() && t.last == payload {
            return;
        }
        let before = open_bytes(t);
        t.last = payload.to_vec();
        t.open.push((ts, payload.to_vec()));
        if t.open.len() >= BLOCK_MSGS {
            let b = seal(&t.open);
            t.open.clear();
            self.bytes += b.data.len() + BLOCK_OVERHEAD;
            t.blocks.push_back(b);
        }
        let after = open_bytes(t);
        self.bytes = (self.bytes + after).saturating_sub(before);
        // While the CPU governor throttles, eviction (a scan over all topics) is deferred up to 2% over the cap.
        let lim = if crate::govern::throttled() {
            self.cap + self.cap / 50
        } else {
            self.cap
        };
        while self.bytes > lim && self.evict_one() {}
    }

    /// Drop the oldest block of the topic whose oldest block is oldest. A topic with only an open
    /// tail gives that tail up (sealed into nothing) once no block is left anywhere.
    fn evict_one(&mut self) -> bool {
        let Some(k) = self
            .topics
            .iter()
            .filter(|(_, t)| !t.blocks.is_empty())
            .min_by_key(|(_, t)| t.blocks[0].first_ts)
            .map(|(k, _)| k.clone())
        else {
            return false;
        };
        let Some(t) = self.topics.get_mut(&k) else {
            return false;
        };
        if let Some(b) = t.blocks.pop_front() {
            self.bytes = self.bytes.saturating_sub(b.data.len() + BLOCK_OVERHEAD);
            self.evicted_blocks += 1;
        }
        true
    }

    /// Topics that start with `prefix`.
    pub fn topics_with(&self, prefix: &str) -> Vec<String> {
        self.topics
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Every stored message of one topic, oldest first.
    pub fn entries(&self, topic: &str) -> Vec<(i64, Vec<u8>)> {
        let Some(t) = self.topics.get(topic) else {
            return Vec::new();
        };
        let mut out: Vec<(i64, Vec<u8>)> = t.blocks.iter().flat_map(unseal).collect();
        out.extend(t.open.iter().cloned());
        out
    }

    /// Messages whose topic starts with `prefix` and whose time lies in `[start, end]` (ms), oldest
    /// first, at most `limit` (the newest ones when more match).
    pub fn range(
        &self,
        prefix: &str,
        start: i64,
        end: i64,
        limit: usize,
    ) -> Vec<(String, i64, Vec<u8>)> {
        let mut out = Vec::new();
        for (k, t) in self.topics.range(prefix.to_string()..) {
            if !k.starts_with(prefix) {
                break;
            }
            for b in t
                .blocks
                .iter()
                .filter(|b| b.last_ts >= start && b.first_ts <= end)
            {
                out.extend(
                    unseal(b)
                        .into_iter()
                        .filter(|(ts, _)| *ts >= start && *ts <= end)
                        .map(|(ts, p)| (k.clone(), ts, p)),
                );
            }
            out.extend(
                t.open
                    .iter()
                    .filter(|(ts, _)| *ts >= start && *ts <= end)
                    .map(|(ts, p)| (k.clone(), *ts, p.clone())),
            );
        }
        out.sort_by_key(|m| m.1);
        if out.len() > limit {
            out.drain(..out.len() - limit);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_dedupe_and_prefix() {
        let mut c = Cache::new(1 << 20);
        for i in 0..100 {
            c.insert(
                "public/srv/cpu",
                1000 + i,
                format!(r#"{{"v":{i}}}"#).as_bytes(),
            );
        }
        c.insert("public/srv/cpu", 2000, br#"{"v":99}"#); // same as last: dropped
        c.insert("internal/_focus", 1500, br#"{"firing":[]}"#);
        let all = c.range("", 0, i64::MAX, 1000);
        assert_eq!(all.len(), 101);
        let cpu = c.range("public/", 0, i64::MAX, 1000);
        assert_eq!(cpu.len(), 100);
        assert_eq!(cpu[0].2, br#"{"v":0}"#);
        assert_eq!(cpu[99].2, br#"{"v":99}"#);
        assert_eq!(c.range("public/", 1010, 1019, 1000).len(), 10);
        assert_eq!(c.range("public/", 0, i64::MAX, 5).len(), 5);
        assert_eq!(c.series_count(), 2);
        assert_eq!(c.oldest_ms(), Some(1000));
    }

    #[test]
    fn evicts_oldest_blocks_and_respects_cap() {
        let mut small = Cache::new(4096);
        for i in 0..5000i64 {
            small.insert(
                "t/a",
                i,
                format!(r#"{{"value":{i},"pad":"xxxxxxxxxxxxxxxx{i}"}}"#).as_bytes(),
            );
        }
        assert!(small.bytes() <= 4096 + 2048, "bytes {}", small.bytes());
        assert!(small.evicted_blocks > 0);
        let r = small.range("t/", 0, i64::MAX, 100_000);
        assert!(r.last().is_some_and(|m| m.1 == 4999), "newest kept");
        assert!(small.oldest_ms().is_some_and(|o| o > 0), "oldest gone");
    }

    #[test]
    fn compresses_repetitive_output() {
        let mut c = Cache::new(1 << 24);
        for i in 0..3200i64 {
            c.insert(
                "public/x",
                i,
                format!(
                    r#"{{"tile":"cpu","state":"ok","load":{},"hosts":["a","b","c"]}}"#,
                    i % 7
                )
                .as_bytes(),
            );
        }
        let raw: usize = 3200 * 70;
        assert!(c.bytes() < raw / 3, "bytes {} raw {raw}", c.bytes());
    }

    #[test]
    fn entries_topics_and_block_codec() {
        let mut c = Cache::new(1 << 20);
        for i in 0..70i64 {
            c.insert("public/srv/cpu", i, format!("{{\"v\":{i}}}").as_bytes());
        }
        c.insert("public/web/cpu", 5, b"{}");
        assert_eq!(
            c.topics_with("public/srv/"),
            vec!["public/srv/cpu".to_string()]
        );
        let e = c.entries("public/srv/cpu");
        assert_eq!(e.len(), 70);
        assert_eq!(e[69], (69, br#"{"v":69}"#.to_vec()));
        let (first, last, n, data) = seal_entries(&e[..32]);
        assert_eq!((first, last, n), (0, 31, 32));
        assert_eq!(unseal_entries(first, n, data), e[..32].to_vec());
    }
}

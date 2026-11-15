//! Sealed datagram format v1 (see `docs/relay-datagram.md`), key loading,
//! replay window and sequence gap accounting.
//!
//! ```text
//! offset  size  field
//! 0       1     version (1)
//! 1       4     sender prefix, random per process start (big endian)
//! 5       8     sequence number, starts at 0 per prefix (big endian)
//! 13      n+16  ChaCha20-Poly1305 ciphertext and tag
//! nonce = prefix || sequence (12 bytes), AAD = the 13 header bytes
//! plaintext = InfluxDB line protocol, newline terminated lines
//! ```

use chacha20poly1305::{ChaCha20Poly1305, KeyInit, aead::{Aead, Payload}};
use std::collections::VecDeque;
use std::os::unix::fs::PermissionsExt;

pub const VERSION: u8 = 1;
pub const HEADER: usize = 13;
pub const TAG: usize = 16;
/// Largest plaintext the flusher packs (well below the IPv6 minimum MTU of 1280).
pub const MAX_PAYLOAD: usize = 1100;

/// Key file: 32 raw bytes, or 64 hex characters (trailing newline allowed).
/// Group and other permission bits make the file unusable (except a systemd credential, 0440 under /run/credentials/).
pub fn load_key(path: &str) -> Result<ChaCha20Poly1305, String> {
    let meta = std::fs::metadata(path).map_err(|e| format!("key file {path}: {e}"))?;
    let mode = meta.permissions().mode() & 0o777;
    // systemd credentials (LoadCredential=) are root-owned 0440 inside a 0700 per-service directory.
    let credential = path.starts_with("/run/credentials/") && mode == 0o440;
    if mode & 0o177 != 0 && !credential {
        return Err(format!("key file {path} has mode {mode:o}; it must be 0600 (no group/other access)"));
    }
    let raw = std::fs::read(path).map_err(|e| format!("key file {path}: {e}"))?;
    let key: Vec<u8> = if raw.len() == 32 {
        raw
    } else {
        let t = String::from_utf8_lossy(&raw);
        let t = t.trim();
        if t.len() != 64 || !t.is_ascii() {
            return Err(format!("key file {path}: expected 32 raw bytes or 64 hex characters"));
        }
        (0..32)
            .map(|i| u8::from_str_radix(&t[2 * i..2 * i + 2], 16).map_err(|_| format!("key file {path}: bad hex")))
            .collect::<Result<_, _>>()?
    };
    ChaCha20Poly1305::new_from_slice(&key).map_err(|e| e.to_string())
}

pub struct Sealer {
    aead: ChaCha20Poly1305,
    prefix: u32,
    seq: u64,
}

impl Sealer {
    pub fn new(aead: ChaCha20Poly1305) -> Self {
        Self::with_prefix(aead, rand::random::<u32>())
    }
    pub fn with_prefix(aead: ChaCha20Poly1305, prefix: u32) -> Self {
        Sealer { aead, prefix, seq: 0 }
    }
    pub fn seal(&mut self, payload: &[u8]) -> Vec<u8> {
        let mut hdr = [0u8; HEADER];
        hdr[0] = VERSION;
        hdr[1..5].copy_from_slice(&self.prefix.to_be_bytes());
        hdr[5..13].copy_from_slice(&self.seq.to_be_bytes());
        self.seq += 1;
        let ct = self
            .aead
            .encrypt(&nonce(&hdr), Payload { msg: payload, aad: &hdr })
            .expect("chacha20poly1305 encrypt cannot fail for small input");
        let mut out = Vec::with_capacity(HEADER + ct.len());
        out.extend_from_slice(&hdr);
        out.extend_from_slice(&ct);
        out
    }
}

fn nonce(hdr: &[u8]) -> chacha20poly1305::Nonce {
    let mut n = [0u8; 12];
    n.copy_from_slice(&hdr[1..13]);
    n.into()
}

#[derive(Debug, PartialEq, Eq)]
pub enum OpenError {
    Short,
    Version(u8),
    Auth,
}

pub struct Opened {
    pub prefix: u32,
    pub seq: u64,
    pub payload: Vec<u8>,
}

pub fn open(aead: &ChaCha20Poly1305, d: &[u8]) -> Result<Opened, OpenError> {
    if d.len() < HEADER + TAG {
        return Err(OpenError::Short);
    }
    if d[0] != VERSION {
        return Err(OpenError::Version(d[0]));
    }
    let payload = aead
        .decrypt(&nonce(&d[..HEADER]), Payload { msg: &d[HEADER..], aad: &d[..HEADER] })
        .map_err(|_| OpenError::Auth)?;
    Ok(Opened {
        prefix: u32::from_be_bytes(d[1..5].try_into().unwrap()),
        seq: u64::from_be_bytes(d[5..13].try_into().unwrap()),
        payload,
    })
}

#[derive(Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Accepted; `lost` datagrams were skipped by the sequence jump.
    New { lost: u64 },
    /// Accepted out of order; it was counted as lost before, take one back.
    Late,
    /// Already seen, or older than the window: reject.
    Replay,
}

struct Window {
    prefix: u32,
    highest: u64,
    /// Bit i set: sequence `highest - i` was seen.
    seen: u128,
}

/// Replay window of 128 sequence numbers per sender prefix; the 8 most
/// recent prefixes are kept (a sender restart makes a new prefix).
#[derive(Default)]
pub struct Replay {
    windows: VecDeque<Window>,
}

const MAX_PREFIXES: usize = 8;

impl Replay {
    /// Call only for datagrams that authenticated.
    pub fn check(&mut self, prefix: u32, seq: u64) -> Verdict {
        let Some(w) = self.windows.iter_mut().find(|w| w.prefix == prefix) else {
            if self.windows.len() == MAX_PREFIXES {
                self.windows.pop_front();
            }
            self.windows.push_back(Window { prefix, highest: seq, seen: 1 });
            return Verdict::New { lost: 0 };
        };
        if seq > w.highest {
            let shift = seq - w.highest;
            w.seen = if shift >= 128 { 1 } else { (w.seen << shift) | 1 };
            w.highest = seq;
            Verdict::New { lost: shift - 1 }
        } else {
            let d = w.highest - seq;
            if d >= 128 || w.seen & (1u128 << d) != 0 {
                Verdict::Replay
            } else {
                w.seen |= 1u128 << d;
                Verdict::Late
            }
        }
    }
}

/// Paced sending: a token bucket of datagrams.
pub struct Bucket {
    rate: f64,
    burst: f64,
    tokens: f64,
    last: std::time::Instant,
}

impl Bucket {
    pub fn new(rate_per_s: f64, burst: f64) -> Self {
        Bucket { rate: rate_per_s.max(1.0), burst: burst.max(1.0), tokens: burst.max(1.0), last: std::time::Instant::now() }
    }
    /// Take one token; returns how long to wait before sending.
    pub fn take(&mut self) -> std::time::Duration {
        let now = std::time::Instant::now();
        self.tokens = (self.tokens + (now - self.last).as_secs_f64() * self.rate).min(self.burst);
        self.last = now;
        self.tokens -= 1.0;
        if self.tokens >= 0.0 {
            std::time::Duration::ZERO
        } else {
            std::time::Duration::from_secs_f64(-self.tokens / self.rate)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn aead() -> ChaCha20Poly1305 {
        ChaCha20Poly1305::new_from_slice(&[7u8; 32]).unwrap()
    }

    #[test]
    fn roundtrip_and_header() {
        let mut s = Sealer::with_prefix(aead(), 0xdead_beef);
        let d0 = s.seal(b"a value=1\n");
        let d1 = s.seal(b"b value=2\n");
        assert_eq!(d0[0], VERSION);
        assert_eq!(d0.len(), HEADER + 10 + TAG);
        let o = open(&aead(), &d1).unwrap();
        assert_eq!((o.prefix, o.seq, o.payload.as_slice()), (0xdead_beef, 1, &b"b value=2\n"[..]));
        assert_eq!(open(&aead(), &d0).unwrap().seq, 0);
    }

    #[test]
    fn tamper_wrong_key_version_short() {
        let mut s = Sealer::with_prefix(aead(), 1);
        let d = s.seal(b"x value=1\n");
        for i in [1usize, 7, 14, d.len() - 1] {
            let mut t = d.clone();
            t[i] ^= 1;
            assert_eq!(open(&aead(), &t).err(), Some(OpenError::Auth), "byte {i}");
        }
        let other = ChaCha20Poly1305::new_from_slice(&[8u8; 32]).unwrap();
        assert_eq!(open(&other, &d).err(), Some(OpenError::Auth));
        let mut v = d.clone();
        v[0] = 2;
        assert_eq!(open(&aead(), &v).err(), Some(OpenError::Version(2)));
        assert_eq!(open(&aead(), &d[..20]).err(), Some(OpenError::Short));
    }

    #[test]
    fn nonces_differ_per_datagram_and_prefix() {
        let mut a = Sealer::with_prefix(aead(), 1);
        let mut b = Sealer::with_prefix(aead(), 2);
        let n: Vec<_> = [a.seal(b"p"), a.seal(b"p"), b.seal(b"p"), b.seal(b"p")].iter().map(|d| d[1..13].to_vec()).collect();
        for i in 0..4 {
            for j in i + 1..4 {
                assert_ne!(n[i], n[j]);
            }
        }
    }

    #[test]
    fn replay_gaps_and_late() {
        let mut r = Replay::default();
        assert_eq!(r.check(1, 0), Verdict::New { lost: 0 });
        assert_eq!(r.check(1, 1), Verdict::New { lost: 0 });
        assert_eq!(r.check(1, 1), Verdict::Replay);
        assert_eq!(r.check(1, 5), Verdict::New { lost: 3 });
        assert_eq!(r.check(1, 3), Verdict::Late);
        assert_eq!(r.check(1, 3), Verdict::Replay);
        assert_eq!(r.check(1, 300), Verdict::New { lost: 294 });
        assert_eq!(r.check(1, 5), Verdict::Replay); // fell out of the window
        // a restarted sender has a new prefix and starts at 0 without a gap
        assert_eq!(r.check(2, 0), Verdict::New { lost: 0 });
        assert_eq!(r.check(1, 301), Verdict::New { lost: 0 });
    }

    #[test]
    fn prefixes_are_bounded() {
        let mut r = Replay::default();
        for p in 0..100 {
            r.check(p, 0);
        }
        assert_eq!(r.windows.len(), MAX_PREFIXES);
        assert_eq!(r.check(0, 0), Verdict::New { lost: 0 }); // evicted, treated as new
    }

    #[test]
    fn bucket_paces() {
        let mut b = Bucket::new(100.0, 2.0);
        assert!(b.take().is_zero());
        assert!(b.take().is_zero());
        let w = b.take();
        assert!(w > std::time::Duration::from_millis(5) && w <= std::time::Duration::from_millis(11), "{w:?}");
    }

    #[test]
    fn key_file_mode_enforced() {
        let dir = std::env::temp_dir().join(format!("cmqt-key-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("k");
        std::fs::write(&p, "00".repeat(32)).unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(load_key(p.to_str().unwrap()).unwrap_err().contains("0600"));
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(load_key(p.to_str().unwrap()).is_ok());
        std::fs::write(&p, [1u8; 32]).unwrap();
        assert!(load_key(p.to_str().unwrap()).is_ok());
        std::fs::write(&p, "short").unwrap();
        assert!(load_key(p.to_str().unwrap()).is_err());
        std::fs::remove_file(&p).unwrap();
        std::fs::remove_dir(&dir).unwrap();
    }
}

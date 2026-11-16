//! Long-term keep (charter D78/D79): chosen box-output topics saved in the account's PostgreSQL
//! database `richc_longterm`, unchanged, as the same zstd blocks the RAM cache uses. Imperative keep
//! orders (admin endpoint of the relay only) are stored in the database, so they survive restarts.
//! The size of everything the account holds is measured and held under the declared ceiling.

use crate::cache::{seal_entries, unseal_entries};
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

const BLOCK_MSGS: usize = 32;
const MIB: u64 = 1 << 20;

#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub host: String,
    pub user: String,
    pub dbname: String,
    pub password_file: String,
    /// File holding the password of the HTTP Basic user `admin` (keep orders, export).
    pub admin_password_file: String,
    #[serde(default = "d_warn")]
    pub warn_mib: u64,
    #[serde(default = "d_prune")]
    pub prune_mib: u64,
    #[serde(default = "d_refuse")]
    pub refuse_mib: u64,
    /// What the account holds besides the home directory and the keep tables, as declared bounds:
    /// the last-resort backup (at most 256 MiB, another role's database) and the RabbitMQ queue (64 MiB).
    #[serde(default = "d_reserved")]
    pub reserved_mib: u64,
    /// A topic's pending messages are written at the latest after this many seconds.
    #[serde(default = "d_flush")]
    pub flush_secs: u64,
    /// Home directory whose files count (sites, logs, binaries).
    #[serde(default = "d_home")]
    pub home: String,
}
fn d_warn() -> u64 {
    800
}
fn d_prune() -> u64 {
    900
}
fn d_refuse() -> u64 {
    950
}
fn d_reserved() -> u64 {
    320
}
fn d_flush() -> u64 {
    600
}
fn d_home() -> String {
    "/home/richc".into()
}

#[derive(Clone, Debug, PartialEq)]
pub struct Order {
    pub prefix: String,
    pub end_ms: Option<i64>,
}

/// What the ceiling policy asks for at a measured size (D79), pure so it is testable.
#[derive(Debug, PartialEq)]
pub enum Level {
    Normal,
    Warn,
    Prune,
    Refuse,
}
pub fn level(total_mib: u64, warn: u64, prune: u64, refuse: u64) -> Level {
    if total_mib >= refuse {
        Level::Refuse
    } else if total_mib >= prune {
        Level::Prune
    } else if total_mib >= warn {
        Level::Warn
    } else {
        Level::Normal
    }
}

/// Does an order cover a message of `topic` at `ts`?
pub fn covers(orders: &[Order], topic: &str, ts: i64) -> bool {
    orders
        .iter()
        .any(|o| topic.starts_with(&o.prefix) && o.end_ms.is_none_or(|e| ts <= e))
}

/// Merge entries of the database and of RAM: one message per (topic, ts), RAM wins; the newest `limit`.
pub fn merge(
    db: Vec<(String, i64, Vec<u8>)>,
    ram: Vec<(String, i64, Vec<u8>)>,
    limit: usize,
) -> Vec<(String, i64, Vec<u8>)> {
    let mut m: BTreeMap<(i64, String), Vec<u8>> = BTreeMap::new();
    for (k, t, p) in db.into_iter().chain(ram) {
        m.insert((t, k), p);
    }
    let mut v: Vec<_> = m.into_iter().map(|((t, k), p)| (k, t, p)).collect();
    if v.len() > limit {
        v.drain(..v.len() - limit);
    }
    v
}

type Pending = (Instant, Vec<(i64, Vec<u8>)>);

struct Keeper {
    cfg: Config,
    pw: String,
    client: Option<postgres::Client>,
    orders: Vec<Order>,
    pending: HashMap<String, Pending>,
    last: HashMap<String, Vec<u8>>,
    refuse: bool,
    total_mib: u64,
}

static K: OnceLock<(Mutex<Keeper>, String)> = OnceLock::new();

fn connect(cfg: &Config, pw: &str) -> Result<postgres::Client, String> {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let tls_cfg = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|e| e.to_string())?
    .with_root_certificates(roots)
    .with_no_client_auth();
    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_cfg);
    let mut c = postgres::Config::new();
    c.host(&cfg.host)
        .user(&cfg.user)
        .password(pw)
        .dbname(&cfg.dbname)
        .ssl_mode(postgres::config::SslMode::Require)
        .connect_timeout(Duration::from_secs(10));
    c.connect(tls).map_err(|e| e.to_string())
}

impl Keeper {
    fn db(&mut self) -> Result<&mut postgres::Client, String> {
        if self.client.as_ref().is_none_or(|c| c.is_closed()) {
            self.client = Some(connect(&self.cfg, &self.pw)?);
        }
        self.client
            .as_mut()
            .ok_or_else(|| "no connection".to_string())
    }
    fn run<T>(
        &mut self,
        f: impl FnOnce(&mut postgres::Client) -> Result<T, postgres::Error>,
    ) -> Result<T, String> {
        let r = f(self.db()?).map_err(|e| e.to_string());
        if r.is_err() {
            self.client = None;
        }
        r
    }
    fn put_block(&mut self, topic: &str, entries: &[(i64, Vec<u8>)]) -> Result<(), String> {
        let (first, last, n, data) = seal_entries(entries);
        self.run(|c| c.execute("insert into keep_blocks(topic, first_ts, last_ts, n, data) values ($1,$2,$3,$4,$5) on conflict do nothing", &[&topic, &first, &last, &(n as i32), &data]).map(|_| ()))
    }
    fn flush_topic(&mut self, topic: &str) {
        if let Some((_, entries)) = self.pending.remove(topic)
            && let Err(e) = self.put_block(topic, &entries)
        {
            eprintln!("cmqt flush: keep: write {topic}: {e}");
            self.pending
                .insert(topic.to_string(), (Instant::now(), entries)); // retried on the next tick
        }
    }
    fn last_from_db(&mut self, topic: &str) -> Vec<u8> {
        let topic = topic.to_string();
        let row = self.run(|c| c.query_opt("select first_ts, n, data from keep_blocks where topic = $1 order by first_ts desc limit 1", &[&topic]));
        match row {
            Ok(Some(r)) => unseal_entries(r.get(0), r.get::<_, i32>(1) as usize, r.get(2))
                .pop()
                .map(|e| e.1)
                .unwrap_or_default(),
            _ => Vec::new(),
        }
    }
}

fn lock() -> Option<std::sync::MutexGuard<'static, Keeper>> {
    K.get()
        .map(|(m, _)| m.lock().unwrap_or_else(|e| e.into_inner()))
}

/// Connect, create the tables, load the orders; start the periodic work.
pub fn init(cfg: Config) -> Result<(), String> {
    let pw = std::fs::read_to_string(&cfg.password_file)
        .map_err(|e| format!("keep password: {e}"))?
        .trim()
        .to_string();
    let admin = std::fs::read_to_string(&cfg.admin_password_file)
        .map_err(|e| format!("keep admin password: {e}"))?
        .trim()
        .to_string();
    let mut k = Keeper {
        cfg: cfg.clone(),
        pw,
        client: None,
        orders: Vec::new(),
        pending: HashMap::new(),
        last: HashMap::new(),
        refuse: false,
        total_mib: 0,
    };
    k.run(|c| {
        c.batch_execute(
            "create table if not exists keep_orders(prefix text primary key, created_ms bigint not null, end_ms bigint);
             create table if not exists keep_blocks(topic text not null, first_ts bigint not null, last_ts bigint not null, n int not null, data bytea not null, primary key(topic, first_ts));
             create index if not exists keep_blocks_ts on keep_blocks(first_ts);",
        )
    })?;
    let rows = k.run(|c| c.query("select prefix, end_ms from keep_orders", &[]))?;
    k.orders = rows
        .iter()
        .map(|r| Order {
            prefix: r.get(0),
            end_ms: r.get(1),
        })
        .collect();
    eprintln!("cmqt flush: keep: {} order(s) loaded", k.orders.len());
    let _ = K.set((Mutex::new(k), admin));
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(Duration::from_secs(60));
            tick(&cfg);
        }
    });
    Ok(())
}

pub fn enabled() -> bool {
    K.get().is_some()
}

pub fn admin_ok(auth: Option<&str>) -> bool {
    use base64::Engine;
    let Some((_, pw)) = K.get() else { return false };
    auth.and_then(|a| a.strip_prefix("Basic "))
        .and_then(|b| {
            base64::engine::general_purpose::STANDARD
                .decode(b.trim())
                .ok()
        })
        .is_some_and(|d| crate::http::eq(&d, format!("admin:{pw}").as_bytes()))
}

/// A message of the box arrived (called for every one): append it when an order covers it.
pub fn observe(topic: &str, ts: i64, payload: &[u8]) {
    let Some(mut k) = lock() else { return };
    if !covers(&k.orders, topic, ts) {
        return;
    }
    if !k.last.contains_key(topic) {
        let l = k.last_from_db(topic);
        k.last.insert(topic.to_string(), l);
    }
    if k.last
        .get(topic)
        .is_some_and(|l| !l.is_empty() && l == payload)
    {
        return;
    }
    k.last.insert(topic.to_string(), payload.to_vec());
    let e = k
        .pending
        .entry(topic.to_string())
        .or_insert_with(|| (Instant::now(), Vec::new()));
    e.1.push((ts, payload.to_vec()));
    if e.1.len() >= BLOCK_MSGS {
        k.flush_topic(topic);
    }
}

/// Add an order and write out what RAM holds for it first. Returns the number of blocks written.
pub fn add_order(
    prefix: &str,
    end_ms: Option<i64>,
    cache: &Mutex<crate::cache::Cache>,
) -> Result<usize, (u16, String)> {
    let Some(mut k) = lock() else {
        return Err((503, "keep is off".into()));
    };
    if k.refuse {
        return Err((507, "refused: the account is over its ceiling".into()));
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let p = prefix.to_string();
    k.run(|c| c.execute("insert into keep_orders(prefix, created_ms, end_ms) values ($1,$2,$3) on conflict (prefix) do update set end_ms = excluded.end_ms", &[&p, &now, &end_ms]).map(|_| ())).map_err(|e| (503, e))?;
    k.orders.retain(|o| o.prefix != prefix);
    k.orders.push(Order {
        prefix: prefix.to_string(),
        end_ms,
    });
    let topics = cache
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .topics_with(prefix);
    let mut blocks = 0;
    for t in topics {
        let entries: Vec<_> = cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entries(&t)
            .into_iter()
            .filter(|e| end_ms.is_none_or(|x| e.0 <= x))
            .collect();
        for chunk in entries.chunks(BLOCK_MSGS) {
            k.put_block(&t, chunk).map_err(|e| (503, e))?;
            blocks += 1;
        }
        if let Some(l) = entries.last() {
            k.last.insert(t.clone(), l.1.clone());
        }
        k.pending.remove(&t);
    }
    Ok(blocks)
}

pub fn remove_order(prefix: &str) -> Result<(), (u16, String)> {
    let Some(mut k) = lock() else {
        return Err((503, "keep is off".into()));
    };
    let p = prefix.to_string();
    k.run(|c| {
        c.execute("delete from keep_orders where prefix = $1", &[&p])
            .map(|_| ())
    })
    .map_err(|e| (503, e))?;
    let drop_topics: Vec<String> = k
        .pending
        .keys()
        .filter(|t| t.starts_with(prefix))
        .cloned()
        .collect();
    for t in drop_topics {
        k.flush_topic(&t);
    }
    k.orders.retain(|o| o.prefix != prefix);
    Ok(())
}

pub fn status_json() -> String {
    let Some(k) = lock() else { return "{}".into() };
    let orders: Vec<_> = k
        .orders
        .iter()
        .map(|o| serde_json::json!({"prefix": o.prefix, "end_ms": o.end_ms}))
        .collect();
    serde_json::json!({"orders": orders, "total_mib": k.total_mib, "refusing": k.refuse})
        .to_string()
}

/// Kept messages of the database in `[start, end]` (ms) under `prefix`, for the merged query.
pub fn db_range(prefix: &str, start: i64, end: i64) -> Vec<(String, i64, Vec<u8>)> {
    let Some(mut k) = lock() else {
        return Vec::new();
    };
    let p = prefix.to_string();
    let rows = k.run(|c| c.query("select topic, first_ts, n, data from keep_blocks where left(topic, length($1)) = $1 and last_ts >= $2 and first_ts <= $3", &[&p, &start, &end]));
    let mut out = Vec::new();
    match rows {
        Ok(rows) => {
            for r in rows {
                let topic: String = r.get(0);
                out.extend(
                    unseal_entries(r.get(1), r.get::<_, i32>(2) as usize, r.get(3))
                        .into_iter()
                        .filter(|e| e.0 >= start && e.0 <= end)
                        .map(|e| (topic.clone(), e.0, e.1)),
                );
            }
        }
        Err(e) => eprintln!("cmqt flush: keep: read: {e}"),
    }
    drop(k);
    out.extend(pending_range(prefix, start, end));
    out
}

fn pending_range(prefix: &str, start: i64, end: i64) -> Vec<(String, i64, Vec<u8>)> {
    let Some(k) = lock() else { return Vec::new() };
    k.pending
        .iter()
        .filter(|(t, _)| t.starts_with(prefix))
        .flat_map(|(t, (_, v))| {
            v.iter()
                .filter(|e| e.0 >= start && e.0 <= end)
                .map(|e| (t.clone(), e.0, e.1.clone()))
        })
        .collect()
}

/// Streamed export: NDJSON lines `{"topic","t","payload"}` from the database page by page, then
/// the RAM tail of every topic (newer than what the database gave for it).
pub struct Export {
    prefix: String,
    start: i64,
    end: i64,
    cursor: (i64, String),
    emitted: HashMap<String, i64>,
    ram: Option<std::vec::IntoIter<String>>,
    buf: Vec<u8>,
    pos: usize,
    done: bool,
}

impl Export {
    pub fn new(prefix: String, start: i64, end: i64) -> Self {
        Export {
            prefix,
            start,
            end,
            cursor: (i64::MIN, String::new()),
            emitted: HashMap::new(),
            ram: None,
            buf: Vec::new(),
            pos: 0,
            done: false,
        }
    }
    fn line(&mut self, topic: &str, ts: i64, p: &[u8]) {
        let payload = serde_json::from_slice(p)
            .unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(p).into_owned()));
        self.buf.extend_from_slice(
            serde_json::json!({"topic": topic, "t": ts as f64 / 1000.0, "payload": payload})
                .to_string()
                .as_bytes(),
        );
        self.buf.push(b'\n');
        let e = self.emitted.entry(topic.to_string()).or_insert(i64::MIN);
        *e = (*e).max(ts);
    }
    fn fill(&mut self) {
        self.buf.clear();
        self.pos = 0;
        if self.ram.is_none() {
            let rows = match lock() {
                Some(mut k) => {
                    let (p, s, e, c) = (
                        self.prefix.clone(),
                        self.start,
                        self.end,
                        self.cursor.clone(),
                    );
                    k.run(|cl| cl.query("select topic, first_ts, n, data from keep_blocks where left(topic, length($1)) = $1 and last_ts >= $2 and first_ts <= $3 and (first_ts, topic) > ($4, $5) order by first_ts, topic limit 20", &[&p, &s, &e, &c.0, &c.1]))
                }
                None => Ok(Vec::new()),
            };
            match rows {
                Ok(rows) if !rows.is_empty() => {
                    for r in rows {
                        let (topic, first): (String, i64) = (r.get(0), r.get(1));
                        self.cursor = (first, topic.clone());
                        for e in unseal_entries(first, r.get::<_, i32>(2) as usize, r.get(3)) {
                            if e.0 >= self.start
                                && e.0 <= self.end
                                && e.0 > self.emitted.get(&topic).copied().unwrap_or(i64::MIN)
                            {
                                self.line(&topic, e.0, &e.1);
                            }
                        }
                    }
                    return;
                }
                Err(e) => {
                    eprintln!("cmqt flush: keep: export: {e}");
                    self.done = true;
                    return;
                }
                Ok(_) => {}
            }
            let topics = crate::http::cache_topics(&self.prefix);
            self.ram = Some(topics.into_iter());
        }
        let next = self.ram.as_mut().and_then(Iterator::next);
        match next {
            Some(t) => {
                let from = self.emitted.get(&t).copied().unwrap_or(i64::MIN);
                for e in crate::http::cache_entries(&t) {
                    if e.0 >= self.start && e.0 <= self.end && e.0 > from {
                        self.line(&t, e.0, &e.1);
                    }
                }
            }
            None => self.done = true,
        }
    }
}

impl std::io::Read for Export {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        while self.pos >= self.buf.len() {
            if self.done {
                return Ok(0);
            }
            self.fill();
        }
        let n = (self.buf.len() - self.pos).min(out.len());
        out[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

/// Size of the files under `dir` (regular files only, symlinks not followed).
fn dir_bytes(dir: &std::path::Path) -> u64 {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return 0;
    };
    rd.flatten()
        .map(|e| match e.file_type() {
            Ok(t) if t.is_dir() => dir_bytes(&e.path()),
            Ok(t) if t.is_file() => e.metadata().map(|m| m.len()).unwrap_or(0),
            _ => 0,
        })
        .sum()
}

/// Once a minute: flush old pending blocks; every 5th minute measure the account and apply D79.
fn tick(cfg: &Config) {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if let Some(mut k) = lock() {
        let old: Vec<String> = k
            .pending
            .iter()
            .filter(|(_, (t, _))| t.elapsed() >= Duration::from_secs(k.cfg.flush_secs))
            .map(|(t, _)| t.clone())
            .collect();
        for t in old {
            k.flush_topic(&t);
        }
    }
    if !n.is_multiple_of(5) {
        return;
    }
    let files = dir_bytes(std::path::Path::new(&cfg.home)) / MIB;
    let mut pruning = false;
    loop {
        let Some(mut k) = lock() else { return };
        // Logical size of the kept data (compressed bytes plus a row overhead); the file size of the
        // database only shrinks at VACUUM FULL, so it is not what pruning can act on.
        let kept = match k.run(|c| c.query_one("select coalesce(sum(octet_length(data) + 96), 0)::bigint, count(*)::bigint from keep_blocks", &[])) {
            Ok(r) => r.get::<_, i64>(0) as u64 / MIB,
            Err(e) => return eprintln!("cmqt flush: keep: measure: {e}"),
        };
        let total = files + kept + cfg.reserved_mib + 16;
        k.total_mib = total;
        let lv = level(total, cfg.warn_mib, cfg.prune_mib, cfg.refuse_mib);
        k.refuse = lv == Level::Refuse;
        eprintln!(
            "cmqt flush: keep: account {total} MiB (files {files}, kept {kept}, reserved {}) of {}/{}/{}: {lv:?}",
            cfg.reserved_mib, cfg.warn_mib, cfg.prune_mib, cfg.refuse_mib
        );
        // From the prune level on, prune until the total is 10 MiB under it.
        pruning |= matches!(lv, Level::Prune | Level::Refuse);
        if !pruning || total < cfg.prune_mib.saturating_sub(10) {
            return;
        }
        let r = k.run(|c| c.execute("delete from keep_blocks where (topic, first_ts) in (select topic, first_ts from keep_blocks order by first_ts limit 500)", &[]));
        match r {
            Ok(0) | Err(_) => return,
            Ok(d) => eprintln!("cmqt flush: keep: pruned {d} oldest block(s)"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ceiling_levels() {
        assert_eq!(level(700, 800, 900, 950), Level::Normal);
        assert_eq!(level(800, 800, 900, 950), Level::Warn);
        assert_eq!(level(899, 800, 900, 950), Level::Warn);
        assert_eq!(level(900, 800, 900, 950), Level::Prune);
        assert_eq!(level(949, 800, 900, 950), Level::Prune);
        assert_eq!(level(950, 800, 900, 950), Level::Refuse);
    }

    #[test]
    fn orders_cover_prefix_and_end() {
        let o = vec![
            Order {
                prefix: "public/srv/".into(),
                end_ms: Some(1000),
            },
            Order {
                prefix: "internal/".into(),
                end_ms: None,
            },
        ];
        assert!(covers(&o, "public/srv/cpu", 1000));
        assert!(!covers(&o, "public/srv/cpu", 1001));
        assert!(!covers(&o, "public/other/cpu", 1));
        assert!(covers(&o, "internal/_focus", i64::MAX));
    }

    #[test]
    fn merge_prefers_ram_and_dedupes() {
        let db = vec![
            ("t/a".to_string(), 1, b"old".to_vec()),
            ("t/a".to_string(), 2, b"two".to_vec()),
        ];
        let ram = vec![
            ("t/a".to_string(), 2, b"two-ram".to_vec()),
            ("t/a".to_string(), 3, b"three".to_vec()),
        ];
        let m = merge(db, ram, 10);
        assert_eq!(m.iter().map(|e| e.1).collect::<Vec<_>>(), vec![1, 2, 3]);
        assert_eq!(m[1].2, b"two-ram");
        assert_eq!(
            merge(
                vec![("t".into(), 1, vec![]), ("t".into(), 2, vec![])],
                vec![],
                1
            )
            .len(),
            1
        );
    }

    #[test]
    fn dir_bytes_counts_files() {
        let d = std::env::temp_dir().join(format!("cmqt-keep-test-{}", std::process::id()));
        std::fs::create_dir_all(d.join("a")).unwrap();
        std::fs::write(d.join("a/x"), vec![0u8; 1000]).unwrap();
        std::fs::write(d.join("y"), vec![0u8; 24]).unwrap();
        assert_eq!(dir_bytes(&d), 1024);
        std::fs::remove_file(d.join("a/x")).unwrap();
        std::fs::remove_file(d.join("y")).unwrap();
        std::fs::remove_dir(d.join("a")).unwrap();
        std::fs::remove_dir(&d).unwrap();
    }
}

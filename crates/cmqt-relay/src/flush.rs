//! `cmqt flush <config.toml>`: durable RabbitMQ queue in, sealed paced UDP out.

use crate::metrics::{self, Counters};
use crate::wire::{self, Bucket, MAX_PAYLOAD, Sealer};
use crate::{influx, otlp};
use futures_lite::StreamExt;
use lapin::{Connection, ConnectionProperties, options::*, types::FieldTable};
use serde::Deserialize;
use std::time::Duration;
use tokio::net::UdpSocket;

fn default_ram_target() -> usize {
    240
}
fn default_cache_floor() -> usize {
    16
}
fn default_cpu_target() -> f64 {
    0.23
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// `amqp://user:pass@host:5672/vhost`; `CMQT_AMQP_URL` overrides it.
    /// Without it (and without `http_listen`) the input is the RabbitMQ queue.
    pub amqp_url: Option<String>,
    /// Pre-declared durable queue (this tool never declares it).
    #[serde(default)]
    pub queue: String,
    /// OTLP/HTTP input instead of RabbitMQ, e.g. `[::]:8300`. When unset, the
    /// platform variables `IP` and `PORT` (alwaysdata sites) are used if both exist.
    pub http_listen: Option<String>,
    /// File with one bearer token per line (`name:token` or `token`); needed for HTTP.
    pub tokens_file: Option<String>,
    /// HTTP mode with `amqp_url`: devices' requests are packed, compressed and sealed at once and
    /// published as ready datagrams into this durable queue; the drain task paces them to the box.
    /// Declared (idempotently) by this tool. Default `relay_datagrams`.
    pub datagram_queue: Option<String>,
    /// Box-silence watcher (HTTP mode): MQTT over WebSocket plus ntfy.
    pub watch: Option<crate::watch::Config>,
    /// Directory with the status page (`index.html`, `app.js`, `style.css`): served on GET for any
    /// path that is not `/otlp`, `/sd`, `/mqtt`; unknown paths answer `index.html` (client-side routes).
    pub static_dir: Option<String>,
    /// History cache cap in MiB (0 = off) and the file holding the `screen` password that guards
    /// `/hist/api/v1/query_range` (HTTP Basic, user `screen`).
    /// Optional hard ceiling for the history cache in MiB (0 = none). The cache is on whenever
    /// `screen_password_file` is set; its size follows `ram_target_mib` (see `govern`).
    #[serde(default)]
    pub cache_mib: usize,
    /// Target for the TOTAL resident memory of the relay processes (flush and Mosquitto); the cache
    /// grows into what is left and evicts the oldest blocks before reaching it.
    #[serde(default = "default_ram_target")]
    pub ram_target_mib: usize,
    /// Smallest cache the governor keeps, in MiB.
    #[serde(default = "default_cache_floor")]
    pub cache_floor_mib: usize,
    /// CPU target as a share of one core, flush plus Mosquitto; above it ingest is paced.
    #[serde(default = "default_cpu_target")]
    pub cpu_target: f64,
    pub screen_password_file: Option<String>,
    pub key_file: String,
    /// Box address, e.g. `[2001:db8::1]:9514`.
    pub target: String,
    /// Paced datagrams per second (token bucket) and burst size.
    #[serde(default = "default_rate")]
    pub rate_pps: f64,
    #[serde(default = "default_burst")]
    pub burst: f64,
    #[serde(default = "default_prefetch")]
    pub prefetch: u16,
    pub metrics_listen: Option<String>,
}
fn default_rate() -> f64 { 200.0 }
fn default_burst() -> f64 { 20.0 }
fn default_prefetch() -> u16 { 8 }

pub const COUNTERS: &[(&str, &str)] = &[
    ("cmqt_flush_messages_total", "Queue messages consumed and acknowledged."),
    ("cmqt_flush_bad_messages_total", "Messages that were not valid OTLP metrics (acknowledged and dropped)."),
    ("cmqt_flush_points_total", "Data points converted to line protocol."),
    ("cmqt_flush_skipped_points_total", "Points skipped: no value, NaN or infinity, or the no-recorded-value flag."),
    ("cmqt_flush_exp_histogram_buckets_dropped_total", "Exponential histogram points sent as count and sum only."),
    ("cmqt_flush_oversize_lines_total", "Lines longer than a datagram payload, dropped."),
    ("cmqt_flush_datagrams_total", "Sealed datagrams sent."),
    ("cmqt_flush_bytes_total", "Bytes of sealed datagrams sent."),
    ("cmqt_flush_send_errors_total", "UDP send failures (the message is requeued)."),
    ("cmqt_flush_amqp_errors_total", "AMQP session failures (reconnect after a pause)."),
    ("cmqt_flush_queue_errors_total", "Publishes into the datagram queue that failed (the request got a 503)."),
    ("cmqt_flush_sd_requests_total", "Foveal discovery requests answered (authorized)."),
    ("cmqt_flush_http_unauthorized_total", "OTLP/HTTP requests without a valid bearer token."),
    ("cmqt_flush_http_bytes_in_total", "OTLP/HTTP request body bytes after decompression."),
];

/// HTTP mode is selected by `tokens_file`. `http_listen` defaults to the platform's IP and PORT.
/// `Ok(true)` means run `http::run`; `Ok(false)` the RabbitMQ consumer of `run`.
pub fn resolve_http(cfg: &mut Config) -> Result<bool, String> {
    if cfg.tokens_file.is_none() {
        return Ok(false);
    }
    if cfg.http_listen.is_none() {
        let (Ok(ip), Ok(port)) = (std::env::var("IP"), std::env::var("PORT")) else {
            return Err("http mode needs http_listen or the platform variables IP and PORT".into());
        };
        cfg.http_listen = Some(if ip.contains(':') { format!("[{ip}]:{port}") } else { format!("{ip}:{port}") });
    }
    if let Ok(u) = std::env::var("CMQT_AMQP_URL") {
        cfg.amqp_url = Some(u);
    }
    Ok(true)
}

pub async fn run(mut cfg: Config) -> Result<(), String> {
    if let Ok(u) = std::env::var("CMQT_AMQP_URL") {
        cfg.amqp_url = Some(u);
    }
    let url = cfg.amqp_url.clone().ok_or("amqp_url missing (config or CMQT_AMQP_URL)")?;
    let mut sealer = Sealer::new(wire::load_key(&cfg.key_file)?);
    let mut bucket = Bucket::new(cfg.rate_pps, cfg.burst);
    let counters = Counters::new(COUNTERS);
    if let Some(m) = &cfg.metrics_listen {
        metrics::serve(m, counters.clone())?;
    }
    let sock = UdpSocket::bind(if cfg.target.starts_with('[') { "[::]:0" } else { "0.0.0.0:0" }).await.map_err(|e| e.to_string())?;
    sock.connect(&cfg.target).await.map_err(|e| format!("target {}: {e}", cfg.target))?;
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).map_err(|e| e.to_string())?;
    loop {
        let s = session(&cfg, &url, &sock, &mut sealer, &mut bucket, &counters);
        tokio::select! {
            r = s => {
                counters.add("cmqt_flush_amqp_errors_total", 1);
                eprintln!("cmqt flush: session ended: {}; reconnecting in 5 s", r.err().unwrap_or_default());
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            _ = tokio::signal::ctrl_c() => return Ok(()),
            _ = term.recv() => return Ok(()),
        }
    }
}

async fn session(cfg: &Config, url: &str, sock: &UdpSocket, sealer: &mut Sealer, bucket: &mut Bucket, c: &Counters) -> Result<(), String> {
    let conn = Connection::connect(url, ConnectionProperties::default()).await.map_err(|e| e.to_string())?;
    let ch = conn.create_channel().await.map_err(|e| e.to_string())?;
    ch.basic_qos(cfg.prefetch, BasicQosOptions::default()).await.map_err(|e| e.to_string())?;
    let mut consumer = ch
        .basic_consume(cfg.queue.as_str().into(), "cmqt-flush".into(), BasicConsumeOptions::default(), FieldTable::default())
        .await
        .map_err(|e| e.to_string())?;
    eprintln!("cmqt flush: consuming {}", cfg.queue);
    while let Some(d) = consumer.next().await {
        let d = d.map_err(|e| e.to_string())?;
        let datagrams = match prepare(&d.data, c) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("cmqt flush: dropping message: {e}");
                c.add("cmqt_flush_bad_messages_total", 1);
                d.ack(BasicAckOptions::default()).await.map_err(|e| e.to_string())?;
                continue;
            }
        };
        let mut failed = false;
        for p in &datagrams {
            let wait = bucket.take();
            if !wait.is_zero() {
                tokio::time::sleep(wait).await;
            }
            let sealed = sealer.seal(p);
            match sock.send(&sealed).await {
                Ok(_) => {
                    c.add("cmqt_flush_datagrams_total", 1);
                    c.add("cmqt_flush_bytes_total", sealed.len() as u64);
                }
                Err(e) => {
                    eprintln!("cmqt flush: send: {e}");
                    c.add("cmqt_flush_send_errors_total", 1);
                    failed = true;
                    break;
                }
            }
        }
        if failed {
            d.nack(BasicNackOptions { requeue: true, ..Default::default() }).await.map_err(|e| e.to_string())?;
            tokio::time::sleep(Duration::from_secs(1)).await;
        } else {
            c.add("cmqt_flush_messages_total", 1);
            d.ack(BasicAckOptions::default()).await.map_err(|e| e.to_string())?;
        }
    }
    Err("consumer stream ended".into())
}

/// One request body to compressed datagram payloads (each an independent zstd frame, at most
/// `MAX_PAYLOAD` bytes compressed).
pub fn prepare_zstd(body: &[u8], c: &Counters, cache: Option<&std::sync::Mutex<crate::cache::Cache>>) -> Result<Vec<Vec<u8>>, String> {
    let req = otlp::decode(body)?;
    let mut lines = Vec::new();
    let t = influx::convert(&req, &mut lines);
    if let Some(cache) = cache {
        let mut g = cache.lock().unwrap_or_else(|e| e.into_inner());
        for l in &lines {
            g.insert_line(l);
        }
    }
    let (packed, oversize) = influx::pack_zstd(&lines, MAX_PAYLOAD);
    c.add("cmqt_flush_points_total", t.points);
    c.add("cmqt_flush_skipped_points_total", t.skipped);
    c.add("cmqt_flush_exp_histogram_buckets_dropped_total", t.exp_buckets_dropped);
    c.add("cmqt_flush_oversize_lines_total", oversize);
    Ok(packed)
}

/// One queue message to datagram payloads (plaintext line protocol).
pub fn prepare(body: &[u8], c: &Counters) -> Result<Vec<Vec<u8>>, String> {
    let req = otlp::decode(body)?;
    let mut lines = Vec::new();
    let t = influx::convert(&req, &mut lines);
    let (packed, oversize) = influx::pack(&lines, MAX_PAYLOAD);
    c.add("cmqt_flush_points_total", t.points);
    c.add("cmqt_flush_skipped_points_total", t.skipped);
    c.add("cmqt_flush_exp_histogram_buckets_dropped_total", t.exp_buckets_dropped);
    c.add("cmqt_flush_oversize_lines_total", oversize);
    Ok(packed)
}

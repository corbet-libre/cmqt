//! The relay as the watcher of the box: subscribes to the tile trees over MQTT/WebSocket and sends
//! one ntfy alert when nothing live has arrived for `silence_s`, or when the box's `_online` has said
//! `degraded` for as long (it publishes, but VictoriaMetrics, vmalert or the alert path is broken), and one
//! message when it is back.

use rumqttc::{Client, Event, MqttOptions, Packet, QoS, Transport};
use serde::Deserialize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// `wss://host/path` of the relay's Mosquitto (public web TLS), login with read rights.
    pub mqtt_url: String,
    pub user: String,
    pub password_file: String,
    /// File holding the ntfy topic (the topic is the credential).
    pub ntfy_topic_file: String,
    #[serde(default = "default_ntfy")]
    pub ntfy_base: String,
    #[serde(default = "default_silence")]
    pub silence_s: u64,
}
fn default_ntfy() -> String {
    "https://ntfy.sh".into()
}
fn default_silence() -> u64 {
    300
}

/// Subsystems the foveal collection knows (the `subsystem` label of the attention rules).
pub const SUBSYSTEMS: &[&str] = &["cpu", "memory", "disk-io", "disk-space", "network"];
/// node_exporter collectors per subsystem: the device-level discovery answer lists one target per
/// collector, because a Prometheus scrape takes one `collect[]` value per relabelled target. Must equal
/// `collectors` in infra lib/foveal-collection.nix.
pub const COLLECTORS: &[(&str, &[&str])] = &[
    ("cpu", &["cpu", "loadavg"]),
    ("memory", &["meminfo", "vmstat"]),
    ("disk-io", &["diskstats", "pressure"]),
    ("disk-space", &["filesystem"]),
    ("network", &["netdev", "netstat"]),
];

/// The device-level discovery body: one target group per collector of every subsystem in focus,
/// labelled `subsystem` and `collector` (the collector's relabelling reads and drops both).
pub fn sd_device_body(focus: &Focus, now: Instant, device: &str) -> String {
    let groups: Vec<serde_json::Value> = COLLECTORS
        .iter()
        .filter(|(s, _)| focus.in_focus(now, device, s))
        .flat_map(|(s, cols)| cols.iter().map(move |c| serde_json::json!({"targets": ["focus"], "labels": {"subsystem": s, "collector": c}})))
        .collect();
    serde_json::Value::Array(groups).to_string()
}
const HOLD: Duration = Duration::from_secs(60);
/// The box publishes `internal/_focus` only when the set changes, and `_online` every 120 s: any
/// live message from the box is the heartbeat. With none for this long nothing is in focus.
const ALIVE: Duration = Duration::from_secs(300);

/// Focus state in RAM (contract section 8): the pairs of the last `_focus` report are in focus,
/// and stay in focus for 60 s after a report dropped them (hold-down against flapping); with no
/// live message from the box at all for 180 s nothing is in focus (stale focus is never kept).
#[derive(Default)]
pub struct Focus {
    alive: Option<Instant>,
    current: std::collections::HashSet<(String, String)>,
    dropped: std::collections::HashMap<(String, String), Instant>,
}
impl Focus {
    /// Any live message from the box.
    pub fn heartbeat(&mut self, now: Instant) {
        self.alive = Some(now);
    }
    /// The retained set read after a (re)subscribe: it is the last report, but proves nothing about
    /// the box being alive now, so only the set is taken and only while the box is alive.
    pub fn restore(&mut self, now: Instant, pairs: Vec<(String, String)>) {
        if self.current.is_empty() && self.dropped.is_empty() {
            self.current = pairs.into_iter().collect();
            self.alive.get_or_insert(now);
        }
    }
    /// A `_focus` report: the complete set of firing pairs.
    pub fn report(&mut self, now: Instant, pairs: Vec<(String, String)>) {
        self.alive = Some(now);
        let new: std::collections::HashSet<_> = pairs.into_iter().collect();
        for p in self.current.difference(&new) {
            self.dropped.insert(p.clone(), now);
        }
        for p in &new {
            self.dropped.remove(p);
        }
        self.dropped.retain(|_, t| now.duration_since(*t) < HOLD);
        self.current = new;
    }
    pub fn in_focus(&self, now: Instant, device: &str, subsystem: &str) -> bool {
        let k = (device.to_string(), subsystem.to_string());
        self.alive.is_some_and(|a| now.duration_since(a) < ALIVE)
            && (self.current.contains(&k)
                || self
                    .dropped
                    .get(&k)
                    .is_some_and(|t| now.duration_since(*t) < HOLD))
    }
}
pub type SharedFocus = Arc<Mutex<Focus>>;

fn parse_focus(payload: &[u8]) -> Option<Vec<(String, String)>> {
    let v: serde_json::Value = serde_json::from_slice(payload).ok()?;
    Some(
        v["firing"]
            .as_array()?
            .iter()
            .filter_map(|p| Some((p[0].as_str()?.to_string(), p[1].as_str()?.to_string())))
            .collect(),
    )
}

/// What the watcher sends.
#[derive(Debug, PartialEq, Eq)]
pub enum Alarm {
    /// nothing live from the box for the limit
    Silent,
    /// the box publishes, but its `_online` has said `degraded` for the limit
    Degraded,
    /// neither any more
    Back,
}

/// Alert state machine: pure, so it is testable. `tick` returns the message to send, if any. A degraded box counts
/// like a silent one (charter D69: one message, one on recovery); silence and degradation never page twice.
pub struct Silence {
    limit: Duration,
    silent: bool,
}
impl Silence {
    pub fn new(limit: Duration) -> Self {
        Silence {
            limit,
            silent: false,
        }
    }
    /// Forget the alarm state (maintenance): nothing is paged or announced as back while it holds, and
    /// when it ends a box that is still silent is paged again after the threshold.
    pub fn reset(&mut self) {
        self.silent = false;
    }
    /// `since_last`: time since the last live message; `degraded_for`: how long `_online` has said `degraded`.
    pub fn tick(&mut self, since_last: Duration, degraded_for: Option<Duration>) -> Option<Alarm> {
        let quiet = since_last.max(degraded_for.unwrap_or_default());
        if !self.silent && quiet >= self.limit {
            self.silent = true;
            Some(if since_last >= self.limit {
                Alarm::Silent
            } else {
                Alarm::Degraded
            })
        } else if self.silent && quiet < self.limit {
            self.silent = false;
            Some(Alarm::Back)
        } else {
            None
        }
    }
}

/// The degradation `_online` reports (cmqt `Online.degraded`; the words are `cmqt::degraded`), in a sentence.
pub fn degraded_text(reason: &str) -> &'static str {
    match reason {
        "query" => "VictoriaMetrics does not answer the board server",
        "rules" => {
            "VictoriaMetrics holds no recorded tile value: vmalert does not evaluate the rules"
        }
        "check" => {
            "the box's pipeline check fires (Alertmanager down or vmalert cannot send alerts)"
        }
        _ => "the board server reports a broken pipeline",
    }
}

/// The `degraded` word of a live `_online` payload, if any.
fn parse_degraded(payload: &[u8]) -> Option<String> {
    let v: serde_json::Value = serde_json::from_slice(payload).ok()?;
    (v["state"] == "online")
        .then(|| v["degraded"].as_str().map(str::to_string))
        .flatten()
}

/// Since when, and why, the box has said `degraded` (both trees carry the same `_online`).
type Degraded = Arc<Mutex<Option<(Instant, String)>>>;

fn note_online(degraded: &Degraded, now: Instant, payload: &[u8]) {
    let mut d = degraded.lock().unwrap_or_else(|e| e.into_inner());
    *d = match (parse_degraded(payload), d.take()) {
        (Some(r), Some((since, _))) => Some((since, r)),
        (Some(r), None) => Some((now, r)),
        (None, _) => None,
    };
}

fn read_line(path: &str) -> Result<String, String> {
    let s = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
    Ok(s.lines().next().unwrap_or("").trim().to_string())
}

fn notify(cfg: &Config, topic: &str, title: &str, body: &str) {
    let url = format!("{}/{}", cfg.ntfy_base.trim_end_matches('/'), topic);
    let r = ureq::post(&url)
        .header("Title", title)
        .send(body.as_bytes());
    if let Err(e) = r {
        eprintln!("cmqt flush: ntfy: {e}");
    }
}

/// Spawn the watcher threads. Errors (missing files) are returned; later failures are retried.
pub fn spawn(cfg: Config, focus: SharedFocus) -> Result<(), String> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let pw = read_line(&cfg.password_file)?;
    let topic = read_line(&cfg.ntfy_topic_file)?;
    let last = Arc::new(Mutex::new(Instant::now()));
    let degraded: Degraded = Arc::new(Mutex::new(None));
    let (l2, c2, d2) = (last.clone(), cfg.clone(), degraded.clone());
    let focus = focus.clone();
    std::thread::spawn(move || {
        loop {
            let mut o = MqttOptions::new(
                format!("cmqt-watch-{}", std::process::id()),
                &c2.mqtt_url,
                443,
            );
            o.set_keep_alive(Duration::from_secs(30));
            o.set_credentials(&c2.user, &pw);
            o.set_transport(Transport::wss_with_default_config());
            let (client, mut conn) = Client::new(o, 64);
            for t in ["public/#", "internal/#"] {
                let _ = client.subscribe(t, QoS::AtMostOnce);
            }
            for ev in conn.iter() {
                match ev {
                    // Live forwards have retain = false; the retained set replayed on subscribe is not news.
                    // The retained current set (read on subscribe): state, but not a heartbeat.
                    Ok(Event::Incoming(Packet::Publish(p)))
                        if p.retain && p.topic == "internal/_focus" =>
                    {
                        crate::http::cache_put(&p.topic, &p.payload);
                        if let Some(pairs) = parse_focus(&p.payload) {
                            focus
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .restore(Instant::now(), pairs);
                        }
                    }
                    Ok(Event::Incoming(Packet::Publish(p))) if !p.retain => {
                        crate::http::cache_put(&p.topic, &p.payload);
                        *l2.lock().unwrap_or_else(|e| e.into_inner()) = Instant::now();
                        if p.topic.ends_with("/_online") {
                            focus
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .heartbeat(Instant::now());
                            note_online(&d2, Instant::now(), &p.payload);
                        }
                        if p.topic == "internal/_focus"
                            && let Some(pairs) = parse_focus(&p.payload)
                        {
                            focus
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .report(Instant::now(), pairs);
                        }
                    }
                    // Retained tiles and states: the box's current output, stored (deduplicated) too.
                    Ok(Event::Incoming(Packet::Publish(p))) => {
                        crate::http::cache_put(&p.topic, &p.payload)
                    }
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("cmqt flush: watch mqtt: {e}");
                        break;
                    }
                }
            }
            std::thread::sleep(Duration::from_secs(10));
        }
    });
    std::thread::spawn(move || {
        let mut s = Silence::new(Duration::from_secs(cfg.silence_s));
        loop {
            std::thread::sleep(Duration::from_secs(10));
            let since = last.lock().unwrap_or_else(|e| e.into_inner()).elapsed();
            let deg = degraded
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_ref()
                .map(|(at, r)| (at.elapsed(), r.clone()));
            if crate::http::maintenance().is_some() {
                s.reset();
                continue;
            }
            match s.tick(since, deg.as_ref().map(|(d, _)| *d)) {
                Some(Alarm::Silent) => notify(
                    &cfg,
                    &topic,
                    "Observability box silent",
                    &format!(
                        "Nothing has arrived from the box over the tile link for {} s.",
                        since.as_secs()
                    ),
                ),
                Some(Alarm::Degraded) => {
                    let (d, r) = deg.unwrap_or_default();
                    notify(
                        &cfg,
                        &topic,
                        "Observability box degraded",
                        &format!(
                            "The box publishes, but {} ({r}, for {} s): its own alerts may not reach you.",
                            degraded_text(&r),
                            d.as_secs()
                        ),
                    )
                }
                Some(Alarm::Back) => notify(
                    &cfg,
                    &topic,
                    "Observability box back",
                    "The box is publishing tiles again, its pipeline healthy.",
                ),
                None => {}
            }
        }
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn focus_follows_reports_holds_60s_and_clears_when_the_box_goes_silent() {
        let t0 = Instant::now();
        let mut f = Focus::default();
        let s = |n| Duration::from_secs(n);
        assert!(!f.in_focus(t0, "srv", "cpu")); // never seen
        f.report(t0, vec![("srv".into(), "cpu".into())]);
        assert!(f.in_focus(t0 + s(1), "srv", "cpu"));
        assert!(!f.in_focus(t0 + s(1), "srv", "memory"));
        f.heartbeat(t0 + s(120)); // no change reported: the set stands
        assert!(f.in_focus(t0 + s(121), "srv", "cpu"));
        f.report(t0 + s(130), vec![]); // the alert resolved
        assert!(f.in_focus(t0 + s(189), "srv", "cpu")); // 60 s hold-down
        assert!(!f.in_focus(t0 + s(191), "srv", "cpu"));
        f.report(t0 + s(200), vec![("srv".into(), "cpu".into())]);
        assert!(!f.in_focus(t0 + s(501), "srv", "cpu")); // no _online for 300 s
        assert_eq!(
            parse_focus(br#"{"t":1,"firing":[["srv","cpu"],["a","b"]]}"#)
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn device_discovery_lists_one_target_per_collector_of_each_focused_subsystem() {
        let t0 = Instant::now();
        let mut f = Focus::default();
        assert_eq!(sd_device_body(&f, t0, "srv"), "[]");
        f.report(
            t0,
            vec![
                ("srv".into(), "cpu".into()),
                ("srv".into(), "disk-space".into()),
                ("other".into(), "network".into()),
            ],
        );
        let v: serde_json::Value = serde_json::from_str(&sd_device_body(&f, t0, "srv")).unwrap();
        let got: Vec<(String, String)> = v
            .as_array()
            .unwrap()
            .iter()
            .map(|g| {
                (
                    g["labels"]["subsystem"].as_str().unwrap().into(),
                    g["labels"]["collector"].as_str().unwrap().into(),
                )
            })
            .collect();
        assert_eq!(
            got,
            vec![
                ("cpu".into(), "cpu".into()),
                ("cpu".into(), "loadavg".into()),
                ("disk-space".into(), "filesystem".into())
            ]
        );
        assert_eq!(sd_device_body(&f, t0, "nobody"), "[]");
        assert!(
            SUBSYSTEMS
                .iter()
                .all(|s| COLLECTORS.iter().any(|(c, _)| c == s))
        );
    }

    #[test]
    fn maintenance_reset_mutes_and_a_still_silent_box_pages_again_after_it() {
        let secs = Duration::from_secs;
        let mut s = Silence::new(secs(300));
        assert_eq!(s.tick(secs(400), None), Some(Alarm::Silent));
        s.reset(); // maintenance begins: nothing more, and no "back" either
        assert_eq!(s.tick(secs(10), None), None);
        s.reset();
        s.reset(); // maintenance still on, box silent again: still nothing is sent by the loop
        // maintenance cleared while the box stays silent past the threshold: paged again
        assert_eq!(s.tick(secs(500), None), Some(Alarm::Silent));
        assert_eq!(s.tick(secs(600), None), None);
        assert_eq!(s.tick(secs(5), None), Some(Alarm::Back));
    }

    #[test]
    fn one_alert_one_recovery_no_repeats() {
        let mut s = Silence::new(Duration::from_secs(300));
        let secs = |n| Duration::from_secs(n);
        assert_eq!(s.tick(secs(10), None), None);
        assert_eq!(s.tick(secs(299), None), None);
        assert_eq!(s.tick(secs(300), None), Some(Alarm::Silent));
        assert_eq!(s.tick(secs(900), None), None);
        assert_eq!(s.tick(secs(9000), None), None);
        assert_eq!(s.tick(secs(5), None), Some(Alarm::Back));
        assert_eq!(s.tick(secs(6), None), None);
    }

    /// Bug hunt 2026-11-16, finding 3: a box whose VictoriaMetrics died kept publishing `_online` and was never paged.
    #[test]
    fn a_degraded_box_pages_like_a_silent_one_once_and_once_back() {
        let mut s = Silence::new(Duration::from_secs(300));
        let secs = |n| Duration::from_secs(n);
        // messages keep arriving (since_last small) while `_online` says degraded
        assert_eq!(s.tick(secs(5), Some(secs(100))), None);
        assert_eq!(s.tick(secs(5), Some(secs(300))), Some(Alarm::Degraded));
        assert_eq!(s.tick(secs(5), Some(secs(900))), None);
        // it goes silent as well: no second page
        assert_eq!(s.tick(secs(400), Some(secs(1200))), None);
        assert_eq!(s.tick(secs(2), None), Some(Alarm::Back));
        // a short degradation pages nothing
        assert_eq!(s.tick(secs(2), Some(secs(299))), None);
        assert_eq!(s.tick(secs(2), None), None);
    }

    #[test]
    fn online_payloads_set_and_clear_the_degradation_keeping_its_start() {
        let d: Degraded = Arc::new(Mutex::new(None));
        let t0 = Instant::now();
        let s = Duration::from_secs;
        note_online(&d, t0, br#"{"state":"online","t":1,"interval_s":30}"#);
        assert!(d.lock().unwrap().is_none());
        note_online(&d, t0 + s(30), br#"{"state":"online","degraded":"query"}"#);
        note_online(&d, t0 + s(150), br#"{"state":"online","degraded":"rules"}"#);
        assert_eq!(
            d.lock().unwrap().clone(),
            Some((t0 + s(30), "rules".to_string()))
        );
        note_online(&d, t0 + s(200), br#"{"state":"generation-mismatch"}"#);
        assert!(d.lock().unwrap().is_none());
        note_online(&d, t0 + s(210), b"not json");
        assert!(d.lock().unwrap().is_none());
        assert!(degraded_text("query").contains("VictoriaMetrics"));
    }
}

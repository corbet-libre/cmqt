//! The relay as the watcher of the box: subscribes to the tile trees over MQTT/WebSocket and sends
//! one ntfy alert when nothing live has arrived for `silence_s`, one message when it is back.

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

/// Alert state machine: pure, so it is testable. `tick` returns the message to send, if any.
pub struct Silence {
    limit: Duration,
    silent: bool,
}
impl Silence {
    pub fn new(limit: Duration) -> Self {
        Silence { limit, silent: false }
    }
    pub fn tick(&mut self, since_last: Duration) -> Option<&'static str> {
        if !self.silent && since_last >= self.limit {
            self.silent = true;
            Some("silent")
        } else if self.silent && since_last < self.limit {
            self.silent = false;
            Some("back")
        } else {
            None
        }
    }
}

fn read_line(path: &str) -> Result<String, String> {
    let s = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
    Ok(s.lines().next().unwrap_or("").trim().to_string())
}

fn notify(cfg: &Config, topic: &str, title: &str, body: &str) {
    let url = format!("{}/{}", cfg.ntfy_base.trim_end_matches('/'), topic);
    let r = ureq::post(&url).header("Title", title).send(body.as_bytes());
    if let Err(e) = r {
        eprintln!("cmqt flush: ntfy: {e}");
    }
}

/// Spawn the watcher threads. Errors (missing files) are returned; later failures are retried.
pub fn spawn(cfg: Config) -> Result<(), String> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let pw = read_line(&cfg.password_file)?;
    let topic = read_line(&cfg.ntfy_topic_file)?;
    let last = Arc::new(Mutex::new(Instant::now()));
    let (l2, c2) = (last.clone(), cfg.clone());
    std::thread::spawn(move || {
        loop {
            let mut o = MqttOptions::new(format!("cmqt-watch-{}", std::process::id()), &c2.mqtt_url, 443);
            o.set_keep_alive(Duration::from_secs(120));
            o.set_credentials(&c2.user, &pw);
            o.set_transport(Transport::wss_with_default_config());
            let (client, mut conn) = Client::new(o, 64);
            for t in ["public/#", "internal/#"] {
                let _ = client.subscribe(t, QoS::AtMostOnce);
            }
            for ev in conn.iter() {
                match ev {
                    // Live forwards have retain = false; the retained set replayed on subscribe is not news.
                    Ok(Event::Incoming(Packet::Publish(p))) if !p.retain => {
                        *l2.lock().unwrap_or_else(|e| e.into_inner()) = Instant::now();
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
            match s.tick(since) {
                Some("silent") => notify(&cfg, &topic, "Observability box silent", &format!("Nothing has arrived from the box over the tile link for {} s.", since.as_secs())),
                Some(_) => notify(&cfg, &topic, "Observability box back", "The box is publishing tiles again."),
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
    fn one_alert_one_recovery_no_repeats() {
        let mut s = Silence::new(Duration::from_secs(300));
        let secs = |n| Duration::from_secs(n);
        assert_eq!(s.tick(secs(10)), None);
        assert_eq!(s.tick(secs(299)), None);
        assert_eq!(s.tick(secs(300)), Some("silent"));
        assert_eq!(s.tick(secs(900)), None);
        assert_eq!(s.tick(secs(9000)), None);
        assert_eq!(s.tick(secs(5)), Some("back"));
        assert_eq!(s.tick(secs(6)), None);
    }
}

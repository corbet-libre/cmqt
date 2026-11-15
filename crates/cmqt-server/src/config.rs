//! Server configuration (TOML).

use std::path::{Path, PathBuf};

use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Directory holding `manifest.json`, `internal/` and `public/`.
    pub dashboards_dir: PathBuf,
    /// VictoriaMetrics base URL, e.g. `http://127.0.0.1:8428`.
    pub victoria_url: String,
    /// Mosquitto local listener host.
    #[serde(default = "default_host")]
    pub broker_host: String,
    #[serde(default = "default_port")]
    pub broker_port: u16,
    /// MQTT over WebSocket instead of plain TCP, e.g. `wss://relay.example.net/mqtt`
    /// (TLS with the public web roots). Overrides `broker_host` and `broker_port`.
    #[serde(default)]
    pub broker_url: Option<String>,
    /// Broker login; the password comes from a file (first line).
    #[serde(default)]
    pub broker_user: Option<String>,
    #[serde(default)]
    pub broker_password_file: Option<PathBuf>,
    /// Evaluation interval; equals vmalert's evaluation interval.
    #[serde(default = "default_interval")]
    pub interval_s: u64,
    /// Period of the `_online` heartbeat in seconds (default 120: the relay's box-silence watcher waits 5 minutes). Its message expiry is 3 x this.
    #[serde(default = "default_online_every")]
    pub online_every_s: u64,
    /// cprm's `prometheus/manifest.json`; when set, its generation must match
    /// the dashboards' (contract §1).
    #[serde(default)]
    pub prometheus_manifest: Option<PathBuf>,
}

fn default_host() -> String {
    "127.0.0.1".into()
}
fn default_port() -> u16 {
    1883
}
fn default_online_every() -> u64 {
    120
}
fn default_interval() -> u64 {
    30
}

impl Config {
    pub fn load(path: &Path) -> Result<Config, String> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
        Config::parse(&text)
    }

    pub fn parse(text: &str) -> Result<Config, String> {
        let c: Config = toml::from_str(text).map_err(|e| e.to_string())?;
        if c.interval_s == 0 {
            return Err("interval_s must be > 0".into());
        }
        if c.online_every_s == 0 {
            return Err("online_every_s must be > 0".into());
        }
        Ok(c)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_apply() {
        let c = Config::parse("dashboards_dir='/d'\nvictoria_url='http://x'").unwrap();
        assert_eq!(c.interval_s, 30);
        assert_eq!(c.broker_port, 1883);
    }

    #[test]
    fn rejects_bad_values() {
        assert!(Config::parse("dashboards_dir='/d'\nvictoria_url='x'\ninterval_s=0").is_err());
        assert!(
            Config::parse("dashboards_dir='/d'\nvictoria_url='x'\nrefresh_intervals=3").is_err()
        );
        assert!(Config::parse("dashboards_dir='/d'\nvictoria_url='x'\nbogus=1").is_err());
    }
}

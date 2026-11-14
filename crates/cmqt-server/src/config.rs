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
    /// Evaluation interval; equals vmalert's evaluation interval.
    #[serde(default = "default_interval")]
    pub interval_s: u64,
    /// cprm's `prometheus/manifest.json`; when set, its generation must match
    /// the dashboards' (contract §1).
    #[serde(default)]
    pub prometheus_manifest: Option<PathBuf>,
    /// Unchanged tiles are re-published every this many intervals so that the
    /// 3 x interval message expiry and staleness rule never trip on a quiet
    /// board. Must stay below 3. See README (CONTRACT QUESTION 1).
    #[serde(default = "default_refresh")]
    pub refresh_intervals: u64,
}

fn default_host() -> String {
    "127.0.0.1".into()
}
fn default_port() -> u16 {
    1883
}
fn default_interval() -> u64 {
    30
}
fn default_refresh() -> u64 {
    2
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
        if c.refresh_intervals == 0 || c.refresh_intervals >= 3 {
            return Err("refresh_intervals must be 1 or 2".into());
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
        assert_eq!(c.refresh_intervals, 2);
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

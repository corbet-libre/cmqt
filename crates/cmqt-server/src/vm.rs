//! Two instant queries against VictoriaMetrics.

use std::time::Duration;

use serde_json::Value;

use crate::engine::{Alert, Firing, Samples};

pub const TILE_QUERY: &str = "corbet:tile:value";
pub const ALERT_QUERY: &str = r#"ALERTS{alertstate="firing"}"#;

/// Run a blocking instant query and return the `data.result` array.
fn instant(agent: &ureq::Agent, base: &str, query: &str) -> Result<Vec<Value>, String> {
    let url = format!("{}/api/v1/query", base.trim_end_matches('/'));
    let mut resp = agent
        .get(&url)
        .query("query", query)
        .call()
        .map_err(|e| e.to_string())?;
    let body: Value = resp.body_mut().read_json().map_err(|e| e.to_string())?;
    parse_result(&body)
}

/// `(device, subsystem)` of every firing `DeviceAttention` alert (the foveal collection's focus).
pub fn attention(base: &str) -> Result<Vec<(String, String)>, String> {
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(10)))
        .build()
        .into();
    let r = instant(
        &agent,
        base,
        r#"ALERTS{alertname="DeviceAttention",alertstate="firing"}"#,
    )?;
    Ok(attention_from(&r))
}

pub fn attention_from(result: &[Value]) -> Vec<(String, String)> {
    let mut v: Vec<(String, String)> = result
        .iter()
        .filter_map(|r| {
            Some((
                r["metric"]["device"].as_str()?.to_string(),
                r["metric"]["subsystem"].as_str()?.to_string(),
            ))
        })
        .collect();
    v.sort();
    v.dedup();
    v
}

pub fn parse_result(body: &Value) -> Result<Vec<Value>, String> {
    if body.get("status").and_then(Value::as_str) != Some("success") {
        return Err("query status is not success".into());
    }
    body.pointer("/data/result")
        .and_then(Value::as_array)
        .cloned()
        .ok_or_else(|| "no data.result".to_string())
}

pub fn samples_from(result: &[Value]) -> Samples {
    let mut s = Samples::default();
    for r in result {
        let m = &r["metric"];
        let (Some(screen), Some(tile)) = (m["screen"].as_str(), m["tile"].as_str()) else {
            continue;
        };
        let Some(v) = r
            .pointer("/value/1")
            .and_then(Value::as_str)
            .and_then(|x| x.parse::<f64>().ok())
        else {
            continue;
        };
        s.insert(screen, tile, v);
    }
    s
}

pub fn firing_from(result: &[Value]) -> Firing {
    let alerts = result
        .iter()
        .filter_map(|r| {
            let labels = r["metric"]
                .as_object()?
                .iter()
                .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string())))
                .collect();
            Some(Alert { labels })
        })
        .collect();
    Firing { alerts }
}

/// Both queries must succeed, otherwise the whole evaluation counts as
/// "VictoriaMetrics unreachable" (tiles become `unknown`).
pub fn fetch(base: &str) -> Result<(Samples, Firing), String> {
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(10)))
        .build()
        .into();
    let tiles = instant(&agent, base, TILE_QUERY)?;
    let alerts = instant(&agent, base, ALERT_QUERY)?;
    Ok((samples_from(&tiles), firing_from(&alerts)))
}

/// The configured pipeline check: `Ok(true)` when its result holds any series.
pub fn check(base: &str, query: &str) -> Result<bool, String> {
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(10)))
        .build()
        .into();
    Ok(!instant(&agent, base, query)?.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_vector_results() {
        let body = json!({"status":"success","data":{"resultType":"vector","result":[
            {"metric":{"screen":"srv","tile":"ram"},"value":[1.0,"0.63"]},
            {"metric":{"screen":"srv","tile":"cpu"},"value":[1.0,"NaN"]},
            {"metric":{"tile":"orphan"},"value":[1.0,"1"]}
        ]}});
        let r = parse_result(&body).unwrap();
        let s = samples_from(&r);
        assert_eq!(s.get("srv", "ram"), Some(0.63));
        assert!(s.get("srv", "cpu").is_some_and(f64::is_nan));
        assert_eq!(s.get("", "orphan"), None);
    }

    #[test]
    fn rejects_error_status() {
        assert!(parse_result(&json!({"status":"error"})).is_err());
        assert!(parse_result(&json!({"status":"success","data":{}})).is_err());
    }

    #[test]
    fn reads_attention_pairs() {
        let r = vec![
            serde_json::json!({"metric":{"device":"srv","subsystem":"cpu"}}),
            serde_json::json!({"metric":{"device":"srv","subsystem":"cpu"}}),
            serde_json::json!({"metric":{"device":"x"}}),
        ];
        assert_eq!(
            attention_from(&r),
            vec![("srv".to_string(), "cpu".to_string())]
        );
    }

    #[test]
    fn reads_firing_alerts() {
        let r = vec![
            json!({"metric":{"alertname":"DeviceDown","host_name":"srv","alertstate":"firing"},"value":[1,"1"]}),
        ];
        let f = firing_from(&r);
        assert_eq!(f.alerts[0].labels["host_name"], "srv");
        assert_eq!(f.alerts[0].labels["alertname"], "DeviceDown");
    }
}

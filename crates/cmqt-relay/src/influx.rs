//! OTLP metrics to InfluxDB line protocol, in the shape VictoriaMetrics'
//! influx ingestion expects with `-influxSkipSingleField` (single field
//! `value`, so the stored series name is the measurement name).
//!
//! | OTLP | lines |
//! |---|---|
//! | gauge, sum | `name value=X` (cumulative or delta, passed through) |
//! | histogram | `name_bucket,le=B` (cumulative), `name_sum`, `name_count` |
//! | summary | `name,quantile=Q`, `name_sum`, `name_count` |
//! | exponential histogram | `name_sum`, `name_count`; buckets dropped and counted |
//!
//! Metric names and attribute keys are mapped to the Prometheus charset
//! (`[A-Za-z0-9_:]`, other characters become `_`). Resource attributes are
//! tags too; a point attribute wins over a resource attribute of the same key.

use crate::otlp::{AnyValue, KeyValue, Request};
use std::collections::BTreeMap;

#[derive(Default, Debug, PartialEq, Eq)]
pub struct Tally {
    pub points: u64,
    /// Points skipped: no value, NaN/inf, or the "no recorded value" flag.
    pub skipped: u64,
    /// Exponential histogram points whose buckets were dropped.
    pub exp_buckets_dropped: u64,
}

const NO_RECORDED_VALUE: u32 = 1;

pub fn sanitize(s: &str) -> String {
    let mut o: String = s
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == ':' { c } else { '_' })
        .collect();
    if o.is_empty() || o.as_bytes()[0].is_ascii_digit() {
        o.insert(0, '_');
    }
    o
}

fn esc(s: &str) -> String {
    let mut o = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            ',' | ' ' | '=' | '\\' => {
                o.push('\\');
                o.push(c);
            }
            '\n' | '\r' => o.push(' '),
            c => o.push(c),
        }
    }
    o
}

fn text(v: &AnyValue) -> Option<String> {
    if let Some(s) = &v.string_value {
        Some(s.clone())
    } else if let Some(b) = v.bool_value {
        Some(b.to_string())
    } else if let Some(i) = v.int_value {
        Some(i.to_string())
    } else {
        v.double_value.map(|d| d.to_string())
    }
}

type Tags = BTreeMap<String, String>;

fn add_attrs(tags: &mut Tags, attrs: &[KeyValue]) {
    for kv in attrs {
        if let Some(v) = kv.value.as_ref().and_then(text).filter(|v| !v.is_empty()) {
            tags.insert(sanitize(&kv.key), v);
        }
    }
}

fn line(out: &mut Vec<String>, name: &str, tags: &Tags, extra: Option<(&str, String)>, v: f64, ts: u64) {
    let mut l = String::with_capacity(96);
    l.push_str(name);
    let mut all: Vec<(&str, &str)> = tags.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let extra_v;
    if let Some((k, v)) = &extra {
        extra_v = v.clone();
        all.retain(|(kk, _)| kk != k);
        all.push((k, &extra_v));
        all.sort();
    }
    for (k, v) in all {
        l.push(',');
        l.push_str(&esc(k));
        l.push('=');
        l.push_str(&esc(v));
    }
    l.push_str(" value=");
    l.push_str(&v.to_string());
    if ts != 0 {
        l.push(' ');
        l.push_str(&ts.to_string());
    }
    out.push(l);
}

fn bound(b: f64) -> String {
    if b.is_infinite() { "+Inf".into() } else { b.to_string() }
}

pub fn convert(req: &Request, out: &mut Vec<String>) -> Tally {
    let mut t = Tally::default();
    for rm in &req.resource_metrics {
        let mut base = Tags::new();
        if let Some(r) = &rm.resource {
            add_attrs(&mut base, &r.attributes);
        }
        for sm in &rm.scope_metrics {
            for m in &sm.metrics {
                let name = sanitize(&m.name);
                for g in m.gauge.iter().chain(m.sum.iter()) {
                    for p in &g.data_points {
                        let v = p.as_double.or(p.as_int.map(|i| i as f64));
                        match v {
                            Some(v) if v.is_finite() && p.flags & NO_RECORDED_VALUE == 0 => {
                                let mut tags = base.clone();
                                add_attrs(&mut tags, &p.attributes);
                                line(out, &name, &tags, None, v, p.time_unix_nano);
                                t.points += 1;
                            }
                            _ => t.skipped += 1,
                        }
                    }
                }
                if let Some(h) = &m.histogram {
                    for p in &h.data_points {
                        if p.flags & NO_RECORDED_VALUE != 0 {
                            t.skipped += 1;
                            continue;
                        }
                        let mut tags = base.clone();
                        add_attrs(&mut tags, &p.attributes);
                        let bucket = format!("{name}_bucket");
                        let mut cum = 0u64;
                        for (i, c) in p.bucket_counts.iter().enumerate() {
                            cum += c;
                            let le = p.explicit_bounds.get(i).copied().unwrap_or(f64::INFINITY);
                            line(out, &bucket, &tags, Some(("le", bound(le))), cum as f64, p.time_unix_nano);
                        }
                        if let Some(s) = p.sum.filter(|s| s.is_finite()) {
                            line(out, &format!("{name}_sum"), &tags, None, s, p.time_unix_nano);
                        }
                        line(out, &format!("{name}_count"), &tags, None, p.count as f64, p.time_unix_nano);
                        t.points += 1;
                    }
                }
                if let Some(h) = &m.exponential_histogram {
                    for p in &h.data_points {
                        if p.flags & NO_RECORDED_VALUE != 0 {
                            t.skipped += 1;
                            continue;
                        }
                        let mut tags = base.clone();
                        add_attrs(&mut tags, &p.attributes);
                        if let Some(s) = p.sum.filter(|s| s.is_finite()) {
                            line(out, &format!("{name}_sum"), &tags, None, s, p.time_unix_nano);
                        }
                        line(out, &format!("{name}_count"), &tags, None, p.count as f64, p.time_unix_nano);
                        t.points += 1;
                        t.exp_buckets_dropped += 1;
                    }
                }
                if let Some(s) = &m.summary {
                    for p in &s.data_points {
                        if p.flags & NO_RECORDED_VALUE != 0 {
                            t.skipped += 1;
                            continue;
                        }
                        let mut tags = base.clone();
                        add_attrs(&mut tags, &p.attributes);
                        for q in p.quantile_values.iter().filter(|q| q.value.is_finite()) {
                            line(out, &name, &tags, Some(("quantile", q.quantile.to_string())), q.value, p.time_unix_nano);
                        }
                        line(out, &format!("{name}_sum"), &tags, None, p.sum, p.time_unix_nano);
                        line(out, &format!("{name}_count"), &tags, None, p.count as f64, p.time_unix_nano);
                        t.points += 1;
                    }
                }
            }
        }
    }
    t
}

/// Pack lines into payloads of at most `max` bytes (one line never splits;
/// a single line longer than `max` is dropped and counted by the caller via
/// the returned count).
pub fn pack(lines: &[String], max: usize) -> (Vec<Vec<u8>>, u64) {
    let mut out = Vec::new();
    let mut buf: Vec<u8> = Vec::new();
    let mut oversize = 0;
    for l in lines {
        let n = l.len() + 1;
        if n > max {
            oversize += 1;
            continue;
        }
        if buf.len() + n > max {
            out.push(std::mem::take(&mut buf));
        }
        buf.extend_from_slice(l.as_bytes());
        buf.push(b'\n');
    }
    if !buf.is_empty() {
        out.push(buf);
    }
    (out, oversize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::otlp::decode;
    use prost::Message;

    const JSON: &str = r#"{"resourceMetrics":[{"resource":{"attributes":[{"key":"host.name","value":{"stringValue":"a b"}}]},
"scopeMetrics":[{"metrics":[
 {"name":"sys.cpu","gauge":{"dataPoints":[{"attributes":[{"key":"cpu","value":{"intValue":"3"}}],"timeUnixNano":"100","asDouble":0.5}]}},
 {"name":"reqs_total","sum":{"dataPoints":[{"timeUnixNano":"100","asInt":"7"}],"isMonotonic":true}},
 {"name":"lat","histogram":{"dataPoints":[{"timeUnixNano":"100","count":"6","sum":9.5,"bucketCounts":["1","2","3"],"explicitBounds":[1,5]}]}},
 {"name":"eh","exponentialHistogram":{"dataPoints":[{"timeUnixNano":"100","count":"2","sum":3.0}]}},
 {"name":"sm","summary":{"dataPoints":[{"timeUnixNano":"100","count":"4","sum":8.0,"quantileValues":[{"quantile":0.5,"value":2.0}]}]}}
]}]}]}"#;

    fn lines(r: &Request) -> (Vec<String>, Tally) {
        let mut o = vec![];
        let t = convert(r, &mut o);
        (o, t)
    }

    #[test]
    fn all_types_json() {
        let (l, t) = lines(&decode(JSON.as_bytes()).unwrap());
        assert_eq!(l[0], r"sys_cpu,cpu=3,host_name=a\ b value=0.5 100");
        assert_eq!(l[1], r"reqs_total,host_name=a\ b value=7 100");
        assert_eq!(l[2], r"lat_bucket,host_name=a\ b,le=1 value=1 100");
        assert_eq!(l[3], r"lat_bucket,host_name=a\ b,le=5 value=3 100");
        assert_eq!(l[4], r"lat_bucket,host_name=a\ b,le=+Inf value=6 100");
        assert_eq!(l[5], r"lat_sum,host_name=a\ b value=9.5 100");
        assert_eq!(l[6], r"lat_count,host_name=a\ b value=6 100");
        assert_eq!(l[7], r"eh_sum,host_name=a\ b value=3 100");
        assert_eq!(l[8], r"eh_count,host_name=a\ b value=2 100");
        assert_eq!(l[9], r"sm,host_name=a\ b,quantile=0.5 value=2 100");
        assert_eq!(l.len(), 12);
        assert_eq!(t, Tally { points: 5, skipped: 0, exp_buckets_dropped: 1 });
    }

    #[test]
    fn protobuf_equals_json() {
        let r = decode(JSON.as_bytes()).unwrap();
        let bytes = r.encode_to_vec();
        let back = decode(&bytes).unwrap();
        assert_eq!(r, back);
        assert_eq!(lines(&r).0, lines(&back).0);
    }

    #[test]
    fn skips_nan_and_no_value() {
        let j = r#"{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{"name":"g","gauge":{"dataPoints":[
          {"timeUnixNano":"1"},{"timeUnixNano":"1","asDouble":1,"flags":1}]}}]}]}]}"#;
        let (l, t) = lines(&decode(j.as_bytes()).unwrap());
        assert!(l.is_empty());
        assert_eq!(t.skipped, 2);
    }

    #[test]
    fn garbage_is_error() {
        assert!(decode(b"{not json").is_err());
        assert!(decode(&[0xff, 0xff, 0xff]).is_err());
    }

    #[test]
    fn pack_respects_limit() {
        let ls: Vec<String> = (0..50).map(|i| format!("m{i} value=1 1")).collect();
        let (p, o) = pack(&ls, 100);
        assert_eq!(o, 0);
        assert!(p.iter().all(|d| d.len() <= 100));
        assert_eq!(p.concat().iter().filter(|b| **b == b'\n').count(), 50);
        let (_, o) = pack(&["x".repeat(200)], 100);
        assert_eq!(o, 1);
    }
}

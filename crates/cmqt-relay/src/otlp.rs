//! Minimal OTLP metrics model: only the fields the line-protocol conversion needs.
//! The same structs decode protobuf (prost) and OTLP JSON (serde), so both
//! `otlp_encoding` protocols of the Collector share one code path. Protobuf
//! `oneof` groups are declared as plain optional fields (wire compatible).

use prost::Message;
use serde::{Deserialize, Deserializer};

/// 64-bit integers are JSON strings in OTLP JSON; accept both forms.
fn lenient_u64<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum V {
        N(u64),
        S(String),
    }
    Ok(match V::deserialize(d)? {
        V::N(n) => n,
        V::S(s) => s.parse().map_err(serde::de::Error::custom)?,
    })
}

fn lenient_i64<'de, D: Deserializer<'de>>(d: D) -> Result<Option<i64>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum V {
        N(i64),
        S(String),
    }
    Ok(match Option::<V>::deserialize(d)? {
        None => None,
        Some(V::N(n)) => Some(n),
        Some(V::S(s)) => Some(s.parse().map_err(serde::de::Error::custom)?),
    })
}

fn lenient_u64_vec<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u64>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum V {
        N(u64),
        S(String),
    }
    Vec::<V>::deserialize(d)?
        .into_iter()
        .map(|v| match v {
            V::N(n) => Ok(n),
            V::S(s) => s.parse().map_err(serde::de::Error::custom),
        })
        .collect()
}

#[derive(Clone, PartialEq, Message, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Request {
    #[prost(message, repeated, tag = "1")]
    pub resource_metrics: Vec<ResourceMetrics>,
}

#[derive(Clone, PartialEq, Message, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ResourceMetrics {
    #[prost(message, optional, tag = "1")]
    pub resource: Option<Resource>,
    #[prost(message, repeated, tag = "2")]
    pub scope_metrics: Vec<ScopeMetrics>,
}

#[derive(Clone, PartialEq, Message, Deserialize)]
#[serde(default)]
pub struct Resource {
    #[prost(message, repeated, tag = "1")]
    pub attributes: Vec<KeyValue>,
}

#[derive(Clone, PartialEq, Message, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ScopeMetrics {
    #[prost(message, repeated, tag = "2")]
    pub metrics: Vec<Metric>,
}

#[derive(Clone, PartialEq, Message, Deserialize)]
#[serde(default)]
pub struct KeyValue {
    #[prost(string, tag = "1")]
    pub key: String,
    #[prost(message, optional, tag = "2")]
    pub value: Option<AnyValue>,
}

#[derive(Clone, PartialEq, Message, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct AnyValue {
    #[prost(string, optional, tag = "1")]
    pub string_value: Option<String>,
    #[prost(bool, optional, tag = "2")]
    pub bool_value: Option<bool>,
    #[prost(int64, optional, tag = "3")]
    #[serde(deserialize_with = "lenient_i64")]
    pub int_value: Option<i64>,
    #[prost(double, optional, tag = "4")]
    pub double_value: Option<f64>,
}

#[derive(Clone, PartialEq, Message, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Metric {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(message, optional, tag = "5")]
    pub gauge: Option<Gauge>,
    #[prost(message, optional, tag = "7")]
    pub sum: Option<Gauge>,
    #[prost(message, optional, tag = "9")]
    pub histogram: Option<Histogram>,
    #[prost(message, optional, tag = "10")]
    pub exponential_histogram: Option<ExpHistogram>,
    #[prost(message, optional, tag = "11")]
    pub summary: Option<Summary>,
}

/// Gauge and Sum share field 1 (data points); Sum's extra fields are not needed.
#[derive(Clone, PartialEq, Message, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Gauge {
    #[prost(message, repeated, tag = "1")]
    pub data_points: Vec<NumberPoint>,
}

#[derive(Clone, PartialEq, Message, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct NumberPoint {
    #[prost(fixed64, tag = "3")]
    #[serde(deserialize_with = "lenient_u64")]
    pub time_unix_nano: u64,
    #[prost(double, optional, tag = "4")]
    pub as_double: Option<f64>,
    #[prost(sfixed64, optional, tag = "6")]
    #[serde(deserialize_with = "lenient_i64")]
    pub as_int: Option<i64>,
    #[prost(message, repeated, tag = "7")]
    pub attributes: Vec<KeyValue>,
    #[prost(uint32, tag = "8")]
    pub flags: u32,
}

#[derive(Clone, PartialEq, Message, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Histogram {
    #[prost(message, repeated, tag = "1")]
    pub data_points: Vec<HistogramPoint>,
}

#[derive(Clone, PartialEq, Message, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct HistogramPoint {
    #[prost(fixed64, tag = "3")]
    #[serde(deserialize_with = "lenient_u64")]
    pub time_unix_nano: u64,
    #[prost(fixed64, tag = "4")]
    #[serde(deserialize_with = "lenient_u64")]
    pub count: u64,
    #[prost(double, optional, tag = "5")]
    pub sum: Option<f64>,
    #[prost(fixed64, repeated, tag = "6")]
    #[serde(deserialize_with = "lenient_u64_vec")]
    pub bucket_counts: Vec<u64>,
    #[prost(double, repeated, tag = "7")]
    pub explicit_bounds: Vec<f64>,
    #[prost(message, repeated, tag = "9")]
    pub attributes: Vec<KeyValue>,
    #[prost(uint32, tag = "10")]
    pub flags: u32,
}

#[derive(Clone, PartialEq, Message, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ExpHistogram {
    #[prost(message, repeated, tag = "1")]
    pub data_points: Vec<ExpHistogramPoint>,
}

#[derive(Clone, PartialEq, Message, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ExpHistogramPoint {
    #[prost(message, repeated, tag = "1")]
    pub attributes: Vec<KeyValue>,
    #[prost(fixed64, tag = "3")]
    #[serde(deserialize_with = "lenient_u64")]
    pub time_unix_nano: u64,
    #[prost(fixed64, tag = "4")]
    #[serde(deserialize_with = "lenient_u64")]
    pub count: u64,
    #[prost(double, optional, tag = "5")]
    pub sum: Option<f64>,
    #[prost(uint32, tag = "10")]
    pub flags: u32,
}

#[derive(Clone, PartialEq, Message, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Summary {
    #[prost(message, repeated, tag = "1")]
    pub data_points: Vec<SummaryPoint>,
}

#[derive(Clone, PartialEq, Message, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SummaryPoint {
    #[prost(fixed64, tag = "3")]
    #[serde(deserialize_with = "lenient_u64")]
    pub time_unix_nano: u64,
    #[prost(fixed64, tag = "4")]
    #[serde(deserialize_with = "lenient_u64")]
    pub count: u64,
    #[prost(double, tag = "5")]
    pub sum: f64,
    #[prost(message, repeated, tag = "6")]
    pub quantile_values: Vec<Quantile>,
    #[prost(message, repeated, tag = "7")]
    pub attributes: Vec<KeyValue>,
    #[prost(uint32, tag = "8")]
    pub flags: u32,
}

#[derive(Clone, PartialEq, Message, Deserialize)]
#[serde(default)]
pub struct Quantile {
    #[prost(double, tag = "1")]
    pub quantile: f64,
    #[prost(double, tag = "2")]
    pub value: f64,
}

/// Decode one queue message. `{` as first non-blank byte means OTLP JSON.
pub fn decode(body: &[u8]) -> Result<Request, String> {
    let first = body.iter().find(|b| !b.is_ascii_whitespace());
    if first == Some(&b'{') {
        serde_json::from_slice(body).map_err(|e| format!("json: {e}"))
    } else {
        Request::decode(body).map_err(|e| format!("protobuf: {e}"))
    }
}

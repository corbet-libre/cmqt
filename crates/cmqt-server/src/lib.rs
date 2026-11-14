//! cmqt-server: dashboards and VictoriaMetrics in, MQTT topics out.

#![forbid(unsafe_code)]

pub mod config;
pub mod dashboard;
pub mod engine;
pub mod format;
pub mod service;
pub mod vm;

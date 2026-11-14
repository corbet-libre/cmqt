//! Value display (`d`) and threshold colour (`c`).

use crate::dashboard::Step;

fn num(v: f64, decimals: Option<u32>, default_max: usize) -> String {
    let mut s = match decimals {
        Some(n) => format!("{v:.n$}", n = n as usize),
        None => {
            let s = format!("{v:.n$}", n = default_max);
            if s.contains('.') {
                s.trim_end_matches('0').trim_end_matches('.').to_string()
            } else {
                s
            }
        }
    };
    if s.starts_with('-') && s[1..].chars().all(|c| c == '0' || c == '.') {
        s.remove(0);
    }
    s
}

fn scaled(mut v: f64, base: f64, units: &[&str], decimals: Option<u32>) -> String {
    let mut i = 0;
    while v.abs() >= base && i + 1 < units.len() {
        v /= base;
        i += 1;
    }
    format!("{} {}", num(v, decimals, 1), units[i])
}

/// Format a value per Grafana unit and `decimals`. Non-finite values give `n/a`.
pub fn format_value(v: f64, unit: Option<&str>, decimals: Option<u32>) -> String {
    if !v.is_finite() {
        return "n/a".into();
    }
    match unit.unwrap_or("") {
        "percent" => format!("{} %", num(v, decimals, 0)),
        "percentunit" => format!("{} %", num(v * 100.0, decimals, 0)),
        "bytes" => scaled(
            v,
            1024.0,
            &["B", "KiB", "MiB", "GiB", "TiB", "PiB"],
            decimals,
        ),
        "decbytes" => scaled(v, 1000.0, &["B", "kB", "MB", "GB", "TB", "PB"], decimals),
        "Bps" => format!(
            "{}/s",
            scaled(v, 1024.0, &["B", "KiB", "MiB", "GiB", "TiB"], decimals)
        ),
        "ms" => {
            if v.abs() >= 1000.0 {
                format!("{} s", num(v / 1000.0, decimals, 1))
            } else {
                format!("{} ms", num(v, decimals, 1))
            }
        }
        "s" => {
            let a = v.abs();
            if a < 1.0 && a > 0.0 {
                format!("{} ms", num(v * 1000.0, decimals, 0))
            } else if a < 60.0 {
                format!("{} s", num(v, decimals, 1))
            } else if a < 3600.0 {
                format!("{} min", num(v / 60.0, decimals, 1))
            } else if a < 86400.0 {
                format!("{} h", num(v / 3600.0, decimals, 1))
            } else {
                format!("{} d", num(v / 86400.0, decimals, 1))
            }
        }
        "short" => {
            let a = v.abs();
            if a >= 1e9 {
                format!("{} G", num(v / 1e9, decimals, 1))
            } else if a >= 1e6 {
                format!("{} M", num(v / 1e6, decimals, 1))
            } else if a >= 1e3 {
                format!("{} k", num(v / 1e3, decimals, 1))
            } else {
                num(v, decimals, 2)
            }
        }
        "" | "none" => num(v, decimals, 2),
        "reqps" => format!("{} req/s", num(v, decimals, 2)),
        "ops" => format!("{} ops/s", num(v, decimals, 2)),
        other => format!("{} {other}", num(v, decimals, 2)),
    }
}

/// Threshold colour: the last step whose value is `<= v`; a `null` value is
/// the base step. No steps at all gives `grey`.
pub fn threshold_colour(v: f64, steps: &[Step]) -> String {
    let mut best: Option<(f64, &str)> = None;
    for s in steps {
        let at = s.value.unwrap_or(f64::NEG_INFINITY);
        if at <= v && best.is_none_or(|(b, _)| at >= b) {
            best = Some((at, s.color.as_str()));
        }
    }
    best.map_or("grey", |(_, c)| c).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn st(color: &str, value: Option<f64>) -> Step {
        Step {
            color: color.into(),
            value,
        }
    }

    #[test]
    fn percent_units() {
        assert_eq!(format_value(0.63, Some("percentunit"), Some(0)), "63 %");
        assert_eq!(format_value(0.634, Some("percentunit"), Some(1)), "63.4 %");
        assert_eq!(format_value(63.0, Some("percent"), None), "63 %");
        assert_eq!(format_value(0.0, Some("percentunit"), None), "0 %");
    }

    #[test]
    fn bytes_scale_and_decimals() {
        assert_eq!(format_value(1536.0, Some("bytes"), None), "1.5 KiB");
        assert_eq!(format_value(2e9, Some("decbytes"), Some(0)), "2 GB");
        assert_eq!(format_value(12.0, Some("bytes"), None), "12 B");
    }

    #[test]
    fn time_and_plain() {
        assert_eq!(format_value(0.25, Some("s"), None), "250 ms");
        assert_eq!(format_value(90.0, Some("s"), None), "1.5 min");
        assert_eq!(format_value(1500.0, Some("ms"), None), "1.5 s");
        assert_eq!(format_value(3.14259, None, None), "3.14");
        assert_eq!(format_value(3.0, Some("none"), None), "3");
        assert_eq!(format_value(12500.0, Some("short"), None), "12.5 k");
        assert_eq!(format_value(2.0, Some("celsius"), Some(1)), "2.0 celsius");
    }

    #[test]
    fn non_finite_and_negative_zero() {
        assert_eq!(format_value(f64::NAN, Some("percent"), None), "n/a");
        assert_eq!(format_value(f64::INFINITY, None, None), "n/a");
        assert_eq!(format_value(-0.001, Some("percent"), Some(0)), "0 %");
    }

    #[test]
    fn thresholds_pick_highest_reached_step() {
        let steps = [
            st("green", None),
            st("orange", Some(0.7)),
            st("red", Some(0.9)),
        ];
        assert_eq!(threshold_colour(0.1, &steps), "green");
        assert_eq!(threshold_colour(0.7, &steps), "orange");
        assert_eq!(threshold_colour(0.95, &steps), "red");
        assert_eq!(threshold_colour(-5.0, &steps), "green");
        assert_eq!(threshold_colour(1.0, &[]), "grey");
        // order in the file must not matter
        let shuffled = [
            st("red", Some(0.9)),
            st("green", None),
            st("orange", Some(0.7)),
        ];
        assert_eq!(threshold_colour(0.8, &shuffled), "orange");
    }
}

//! `cmqt flush` / `cmqt unseal`: one-way sealed-UDP relay of OTLP metrics.

mod cache;
mod flush;
mod http;
mod watch;
mod influx;
mod metrics;
mod otlp;
mod unseal;
mod wire;

fn load<T: serde::de::DeserializeOwned>(path: &str) -> Result<T, String> {
    let s = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
    toml::from_str(&s).map_err(|e| format!("{path}: {e}"))
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let r = match (a.get(1).map(String::as_str), a.get(2)) {
        (Some("flush"), Some(p)) => load::<flush::Config>(p).and_then(|mut c| {
            if flush::resolve_http(&mut c)? {
                return http::run(c);
            }
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?
                .block_on(flush::run(c))
        }),
        (Some("unseal"), Some(p)) => load(p).and_then(unseal::run),
        _ => Err("usage: cmqt flush <config.toml> | cmqt unseal <config.toml>".into()),
    };
    if let Err(e) = r {
        eprintln!("cmqt: {e}");
        std::process::exit(1);
    }
}

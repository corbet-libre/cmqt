#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::process::ExitCode;

use cmqt_server::{config::Config, service};

#[tokio::main]
async fn main() -> ExitCode {
    let Some(path) = std::env::args_os().nth(1).map(PathBuf::from) else {
        eprintln!("usage: cmqt-server <config.toml>");
        return ExitCode::from(2);
    };
    let config = match Config::load(&path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("cmqt-server: config: {e}");
            return ExitCode::from(2);
        }
    };
    match service::run(config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("cmqt-server: {e}");
            ExitCode::FAILURE
        }
    }
}

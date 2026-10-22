use std::env::{self, VarError};

use realtime_gateway::{auth::parse_tokens, run};
use tokio::net::TcpListener;
use tracing::{info, warn};

const DEFAULT_ADDR: &str = "127.0.0.1:3000";

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "realtime_gateway=info".into()),
        )
        .init();

    let raw_tokens = match env::var("GATEWAY_AUTH_TOKENS") {
        Ok(raw) => raw,
        Err(VarError::NotPresent) => String::new(),
        Err(VarError::NotUnicode(_)) => {
            eprintln!("invalid GATEWAY_AUTH_TOKENS: the value is not valid UTF-8");
            std::process::exit(1);
        }
    };
    let tokens = match parse_tokens(&raw_tokens) {
        Ok(tokens) => tokens,
        Err(error) => {
            eprintln!("invalid GATEWAY_AUTH_TOKENS: {error}");
            std::process::exit(1);
        }
    };
    if tokens.is_empty() {
        warn!("authentication is off: any client can connect");
    }

    let addr = env::var("GATEWAY_ADDR").unwrap_or_else(|_| DEFAULT_ADDR.to_string());
    let listener = TcpListener::bind(&addr).await?;

    info!("gateway listening on ws://{}/ws", listener.local_addr()?);

    run(listener, tokens, shutdown_signal()).await
}

async fn shutdown_signal() {
    if let Err(e) = tokio::signal::ctrl_c().await {
        tracing::error!("failed to listen for ctrl-c: {e}");
        return;
    }
    info!("shutdown signal received");
}

use std::env;

use realtime_gateway::run;
use tokio::net::TcpListener;
use tracing::info;

const DEFAULT_ADDR: &str = "127.0.0.1:3000";

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "realtime_gateway=info".into()),
        )
        .init();

    let addr = env::var("GATEWAY_ADDR").unwrap_or_else(|_| DEFAULT_ADDR.to_string());
    let listener = TcpListener::bind(&addr).await?;

    info!("gateway listening on ws://{}/ws", listener.local_addr()?);

    run(listener, shutdown_signal()).await
}

async fn shutdown_signal() {
    if let Err(e) = tokio::signal::ctrl_c().await {
        tracing::error!("failed to listen for ctrl-c: {e}");
        return;
    }
    info!("shutdown signal received");
}

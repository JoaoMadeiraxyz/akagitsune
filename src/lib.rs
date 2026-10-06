pub mod protocol;
pub mod registry;
pub mod state;
mod ws;

use std::sync::Arc;

use axum::{Router, routing::get};
use tokio::net::TcpListener;

use crate::state::AppState;

pub fn app() -> Router {
    Router::new()
        .route("/ws", get(ws::websocket_handler))
        .with_state(Arc::new(AppState::new()))
}

pub async fn run<F>(listener: TcpListener, shutdown: F) -> std::io::Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    axum::serve(listener, app())
        .with_graceful_shutdown(shutdown)
        .await
}

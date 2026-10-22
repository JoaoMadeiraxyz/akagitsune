pub mod auth;
pub mod protocol;
pub mod registry;
pub mod state;
mod ws;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{Router, routing::get};
use tokio::net::TcpListener;

use crate::auth::Tokens;
use crate::state::AppState;

pub fn app(tokens: Tokens) -> Router {
    Router::new()
        .route("/ws", get(ws::websocket_handler))
        .with_state(Arc::new(AppState::new(tokens)))
}

pub async fn run<F>(listener: TcpListener, tokens: Tokens, shutdown: F) -> std::io::Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    axum::serve(
        listener,
        app(tokens).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown)
    .await
}

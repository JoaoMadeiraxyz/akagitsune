use std::sync::Arc;
use std::sync::atomic::Ordering;

use axum::{
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::Response,
};
use futures_util::{sink::SinkExt, stream::StreamExt};
use serde_json::value::RawValue;
use tokio::sync::{broadcast, mpsc};
use tracing::{info, warn};
use uuid::Uuid;

use crate::protocol::ServerMessage;
use crate::state::{AppState, BroadcastMessage};

const MAX_MESSAGE_SIZE: usize = 64 * 1024;
const LOCAL_QUEUE_SIZE: usize = 32;
const INVALID_PAYLOAD: &str = "text frames must contain valid JSON; use binary frames otherwise";

pub async fn websocket_handler(
    State(state): State<Arc<AppState>>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.max_message_size(MAX_MESSAGE_SIZE)
        .on_upgrade(move |socket| handle_socket(socket, state))
}

fn to_text(msg: &ServerMessage<'_>) -> Message {
    let json = serde_json::to_string(msg).expect("ServerMessage is infallible to serialize");
    Message::Text(json.into())
}

async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>) {
    let my_id = Uuid::new_v4();
    let mut rx_global = state.tx.subscribe();

    if socket
        .send(to_text(&ServerMessage::Welcome { id: my_id }))
        .await
        .is_err()
    {
        return;
    }

    let connections = state.connections.fetch_add(1, Ordering::Relaxed) + 1;
    info!("{my_id} connected (total: {connections})");

    let (mut sender, mut receiver) = socket.split();
    let (tx_local, mut rx_local) = mpsc::channel::<Message>(LOCAL_QUEUE_SIZE);
    let tx_global = state.tx.clone();

    let mut writer_task = tokio::spawn(async move {
        while let Some(msg) = rx_local.recv().await {
            if sender.send(msg).await.is_err() {
                break;
            }
        }
    });

    let tx_to_local = tx_local.clone();
    let mut global_to_local_task = tokio::spawn(async move {
        loop {
            let msg = match rx_global.recv().await {
                Ok(broadcast) if broadcast.source_id == my_id => continue,
                Ok(broadcast) => broadcast.payload,
                Err(broadcast::error::RecvError::Lagged(dropped)) => {
                    warn!("{my_id} lagged by {dropped} messages");
                    to_text(&ServerMessage::Warning { dropped })
                }
                Err(broadcast::error::RecvError::Closed) => break,
            };

            if tx_to_local.send(msg).await.is_err() {
                break;
            }
        }
    });

    let mut reader_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            let payload = match msg {
                Message::Text(text) => match serde_json::from_str::<&RawValue>(&text) {
                    Ok(data) => to_text(&ServerMessage::Message { from: my_id, data }),
                    Err(_) => {
                        let _ = tx_local
                            .send(to_text(&ServerMessage::Error {
                                message: INVALID_PAYLOAD,
                            }))
                            .await;
                        continue;
                    }
                },
                Message::Binary(bytes) => Message::Binary(bytes),
                Message::Close(_) => break,
                _ => continue,
            };

            match tx_global.send(BroadcastMessage {
                source_id: my_id,
                payload,
            }) {
                Ok(n) => info!("{my_id} broadcast to {n} receivers"),
                Err(e) => warn!("{my_id} broadcast failed: {e}"),
            }
        }
    });

    tokio::select! {
        _ = &mut writer_task => (),
        _ = &mut reader_task => (),
        _ = &mut global_to_local_task => (),
    }

    writer_task.abort();
    reader_task.abort();
    global_to_local_task.abort();

    let connections = state.connections.fetch_sub(1, Ordering::Relaxed) - 1;
    info!("{my_id} disconnected (total: {connections})");
}

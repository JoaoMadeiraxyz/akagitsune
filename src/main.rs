use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        ConnectInfo,
    },
    response::Response,
    routing::get,
    Router,
};
use futures_util::{sink::SinkExt, stream::StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tracing::info;
use uuid::Uuid;

#[derive(Clone, Debug)]
struct BroadcastMessage {
    source_id: Uuid,
    source_username: String,
    content: String,
}

struct UserInfo {
    username: String,
    addr: SocketAddr,
}

struct AppState {
    tx: broadcast::Sender<BroadcastMessage>,
    users: RwLock<HashMap<Uuid, UserInfo>>,
}

#[derive(Deserialize)]
struct ClientMessage {
    #[serde(rename = "type")]
    kind: String,
    username: Option<String>,
    content: Option<String>,
}

#[derive(Serialize)]
struct ServerMessage<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    username: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    from: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<&'a str>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let (tx, _) = broadcast::channel(100);

    let state = Arc::new(AppState {
        tx,
        users: RwLock::new(HashMap::new()),
    });

    let app = Router::new()
        .route("/ws", get(websocket_handler))
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();

    info!("gateway listening on ws://{addr}/ws");

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .unwrap();
}

async fn websocket_handler(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, state, addr))
}

fn to_text(msg: &ServerMessage<'_>) -> Message {
    Message::Text(serde_json::to_string(msg).unwrap().into())
}

async fn wait_for_join(socket: &mut WebSocket) -> Option<String> {
    while let Some(Ok(msg)) = socket.next().await {
        match msg {
            Message::Text(text) => {
                let parsed: Result<ClientMessage, _> = serde_json::from_str(&text);
                if let Ok(ClientMessage {
                    kind,
                    username: Some(username),
                    ..
                }) = parsed
                {
                    if kind == "join" {
                        let username = username.trim().to_string();
                        if !username.is_empty() {
                            return Some(username);
                        }
                    }
                }

                let _ = socket
                    .send(to_text(&ServerMessage {
                        kind: "error",
                        username: None,
                        from: None,
                        content: None,
                        message: Some("expected {\"type\":\"join\",\"username\":\"...\"}"),
                    }))
                    .await;
            }
            Message::Close(_) => return None,
            _ => {}
        }
    }
    None
}

async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>, addr: SocketAddr) {
    let my_id = Uuid::new_v4();

    let Some(username) = wait_for_join(&mut socket).await else {
        return;
    };

    if socket
        .send(to_text(&ServerMessage {
            kind: "joined",
            username: Some(&username),
            from: None,
            content: None,
            message: None,
        }))
        .await
        .is_err()
    {
        return;
    }

    {
        let mut users = state.users.write().unwrap();
        users.insert(
            my_id,
            UserInfo {
                username: username.clone(),
                addr,
            },
        );
        info!(
            "User {} ({}) connected (total: {})",
            username,
            my_id,
            users.len()
        );
    }

    let (mut sender, mut receiver) = socket.split();
    let (tx_local, mut rx_local) = tokio::sync::mpsc::channel::<Message>(32);

    let mut rx_global = state.tx.subscribe();
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
            match rx_global.recv().await {
                Ok(broadcast) => {
                    if broadcast.source_id == my_id {
                        continue;
                    }

                    let payload = ServerMessage {
                        kind: "message",
                        username: None,
                        from: Some(&broadcast.source_username),
                        content: Some(&broadcast.content),
                        message: None,
                    };
                    let msg = to_text(&payload);
                    if tx_to_local.send(msg).await.is_err() {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    info!("lagged by {n}, continuing");
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    let my_username = username.clone();
    let mut reader_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            if let Message::Text(text) = msg {
                let content = match serde_json::from_str::<ClientMessage>(&text) {
                    Ok(ClientMessage {
                        kind,
                        content: Some(c),
                        ..
                    }) if kind == "message" => c,
                    Ok(ClientMessage { kind, .. }) if kind == "join" => continue,
                    Ok(_) => continue,
                    Err(_) => text.to_string(),
                };

                if content.trim().is_empty() {
                    continue;
                }

                match tx_global.send(BroadcastMessage {
                    source_id: my_id,
                    source_username: my_username.clone(),
                    content,
                }) {
                    Ok(n) => info!("Broadcast sent to {} receivers", n),
                    Err(e) => info!("Broadcast failed: {}", e),
                }
            } else if let Message::Close(_) = msg {
                break;
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

    {
        let mut users = state.users.write().unwrap();
        if let Some(user) = users.remove(&my_id) {
            info!(
                "User {} ({}) disconnected from {} (total: {})",
                user.username,
                my_id,
                user.addr,
                users.len()
            );
        }
    }
}

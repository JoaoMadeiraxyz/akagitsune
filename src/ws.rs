use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::{
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::Response,
};
use futures_util::{
    sink::SinkExt,
    stream::{SplitSink, SplitStream, StreamExt},
};
use tokio::sync::{
    broadcast::{self, error::RecvError, error::TryRecvError},
    mpsc,
};
use tracing::info;
use uuid::Uuid;

use crate::protocol::{BinaryHeader, ClientFrame, ServerMessage, is_valid_topic};
use crate::registry::{Subscriber, Subscriptions};
use crate::state::AppState;

const MAX_MESSAGE_SIZE: usize = 64 * 1024;
const INBOX_CAPACITY: usize = 256;
const CONTROL_QUEUE_CAPACITY: usize = 16;
const WRITE_BATCH_SIZE: usize = 32;
const INVALID_FRAME: &str = "text frames must be subscribe, unsubscribe or publish control frames";
const INVALID_TOPIC: &str = "topics must be 1 to 255 bytes of UTF-8";
const INVALID_BINARY: &str =
    "binary frames must start with a topic length and that many topic bytes";
const SUBSCRIPTION_LIMIT: &str = "a connection can hold at most 64 subscriptions";

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

fn error_frame(topic: Option<&str>, message: &str) -> Message {
    to_text(&ServerMessage::Error { topic, message })
}

async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>) {
    let my_id = Uuid::new_v4();

    if socket
        .send(to_text(&ServerMessage::Welcome { id: my_id }))
        .await
        .is_err()
    {
        return;
    }

    let connections = state.connections.fetch_add(1, Ordering::Relaxed) + 1;
    info!("{my_id} connected (total: {connections})");

    let (sink, stream) = socket.split();
    let (inbox, inbox_rx) = broadcast::channel::<Message>(INBOX_CAPACITY);
    let (control_tx, control_rx) = mpsc::channel::<Message>(CONTROL_QUEUE_CAPACITY);
    let lagged_total = Arc::new(AtomicU64::new(0));

    let subscriptions =
        Subscriptions::new(Arc::clone(&state.registry), Subscriber { id: my_id, inbox });

    let mut writer_task = tokio::spawn(write_loop(
        sink,
        control_rx,
        inbox_rx,
        Arc::clone(&lagged_total),
    ));
    let mut reader_task = tokio::spawn(read_loop(stream, subscriptions, control_tx));

    tokio::select! {
        _ = &mut writer_task => (),
        _ = &mut reader_task => (),
    }

    writer_task.abort();
    reader_task.abort();

    let connections = state.connections.fetch_sub(1, Ordering::Relaxed) - 1;
    let lagged_total = lagged_total.load(Ordering::Relaxed);
    info!("{my_id} disconnected (total: {connections}, lagged: {lagged_total})");
}

enum Pending {
    Frame(Message),
    Lagged(u64),
}

async fn feed_inbox(
    sink: &mut SplitSink<WebSocket, Message>,
    pending: Pending,
    lagged_total: &AtomicU64,
) -> bool {
    let msg = match pending {
        Pending::Frame(msg) => msg,
        Pending::Lagged(dropped) => {
            lagged_total.fetch_add(dropped, Ordering::Relaxed);
            to_text(&ServerMessage::Warning { dropped })
        }
    };
    sink.feed(msg).await.is_ok()
}

async fn write_loop(
    mut sink: SplitSink<WebSocket, Message>,
    mut control: mpsc::Receiver<Message>,
    mut inbox: broadcast::Receiver<Message>,
    lagged_total: Arc<AtomicU64>,
) {
    loop {
        tokio::select! {
            biased;
            first = control.recv() => {
                let Some(first) = first else { return };
                if sink.feed(first).await.is_err() {
                    return;
                }
                for _ in 1..WRITE_BATCH_SIZE {
                    let Ok(next) = control.try_recv() else { break };
                    if sink.feed(next).await.is_err() {
                        return;
                    }
                }
            }
            first = inbox.recv() => {
                let first = match first {
                    Ok(msg) => Pending::Frame(msg),
                    Err(RecvError::Lagged(dropped)) => Pending::Lagged(dropped),
                    Err(RecvError::Closed) => return,
                };
                if !feed_inbox(&mut sink, first, &lagged_total).await {
                    return;
                }
                for _ in 1..WRITE_BATCH_SIZE {
                    let next = match inbox.try_recv() {
                        Ok(msg) => Pending::Frame(msg),
                        Err(TryRecvError::Lagged(dropped)) => Pending::Lagged(dropped),
                        Err(TryRecvError::Empty | TryRecvError::Closed) => break,
                    };
                    if !feed_inbox(&mut sink, next, &lagged_total).await {
                        return;
                    }
                }
            }
        }
        if sink.flush().await.is_err() {
            return;
        }
    }
}

async fn read_loop(
    mut stream: SplitStream<WebSocket>,
    mut subscriptions: Subscriptions,
    control: mpsc::Sender<Message>,
) {
    let from = subscriptions.id();
    while let Some(Ok(msg)) = stream.next().await {
        let reply = match msg {
            Message::Text(text) => handle_text(&text, &mut subscriptions, from),
            Message::Binary(bytes) => handle_binary(&bytes, &subscriptions, from),
            Message::Close(_) => break,
            _ => continue,
        };
        if let Some(reply) = reply
            && control.send(reply).await.is_err()
        {
            break;
        }
    }
}

fn handle_text(text: &str, subscriptions: &mut Subscriptions, from: Uuid) -> Option<Message> {
    let Ok(frame) = serde_json::from_str::<ClientFrame>(text) else {
        return Some(error_frame(None, INVALID_FRAME));
    };
    let topic = &*frame.topic;
    let kind = &*frame.kind;
    if !matches!(kind, "subscribe" | "unsubscribe" | "publish") {
        return Some(error_frame(Some(topic), INVALID_FRAME));
    }
    if !is_valid_topic(topic) {
        return Some(error_frame(Some(topic), INVALID_TOPIC));
    }
    match kind {
        "subscribe" => Some(match subscriptions.subscribe(topic) {
            Ok(()) => to_text(&ServerMessage::Subscribed { topic }),
            Err(_) => error_frame(Some(topic), SUBSCRIPTION_LIMIT),
        }),
        "unsubscribe" => {
            subscriptions.unsubscribe(topic);
            Some(to_text(&ServerMessage::Unsubscribed { topic }))
        }
        _ => {
            let Some(data) = frame.data else {
                return Some(error_frame(Some(topic), INVALID_FRAME));
            };
            subscriptions.fanout(topic, || {
                to_text(&ServerMessage::Message { topic, from, data })
            });
            None
        }
    }
}

fn handle_binary(bytes: &[u8], subscriptions: &Subscriptions, from: Uuid) -> Option<Message> {
    match BinaryHeader::parse(bytes) {
        Ok(header) => {
            subscriptions.fanout(header.topic, || Message::Binary(header.delivered(from)));
            None
        }
        Err(topic) => Some(error_frame(topic, INVALID_BINARY)),
    }
}

use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::Response;
use axum::routing::get;
use axum::{Router, extract::State};
use futures_util::{SinkExt, StreamExt};
use serde_json::value::RawValue;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc};
use tokio::time::{Instant as TokioInstant, sleep_until};
use uuid::Uuid;

const MAX_MESSAGE_SIZE: usize = 64 * 1024;
const BUS_CAPACITY: usize = 4096;
const QUEUE_CAPACITY: usize = 4096;
const USAGE: &str = "\
refserver — deterministic reference server for calibrating loadgen

Speaks the same wire protocol as the gateway (welcome, message, source filter)
but with behaviour chosen on purpose, so every metric loadgen reports has a known
correct answer. It is a measuring instrument, not a gateway implementation.

USAGE:
    cargo run --release --example refserver -- [OPTIONS]

OPTIONS:
    --port <N>             listen port  [default: 3100]
    --delay-ms <D>         hold every delivery for exactly D ms  [default: 0]
    --drop-1-in <K>        discard every Kth delivery per subscriber  [default: 0]
    --stall-at <S>         seconds after startup at which to freeze  [default: 0]
    --stall-ms <M>         how long the freeze lasts; 0 disables  [default: 0]
    --help                 show this message

A freeze stops reading from sockets as well as delivering, so it back-pressures
publishers the way a real overloaded server does.

Cumulative deliveries are printed to stderr once a second as
`deliveries <n>`; the last line is the server's own count, independent of the
client's.";

struct Behaviour {
    delay: Duration,
    drop_1_in: u64,
    stall_from: Option<Instant>,
    stall_until: Option<Instant>,
}

impl Behaviour {
    async fn wait_out_stall(&self) {
        let (Some(from), Some(until)) = (self.stall_from, self.stall_until) else {
            return;
        };
        let now = Instant::now();
        if now >= from && now < until {
            sleep_until(TokioInstant::from_std(until)).await;
        }
    }
}

#[derive(Clone)]
struct Fanout {
    source: Uuid,
    frame: Message,
}

struct RefState {
    tx: broadcast::Sender<Fanout>,
    delivered: Arc<AtomicU64>,
    behaviour: Behaviour,
}

fn flag<T: FromStr>(name: &str, default: T) -> T {
    let args: Vec<String> = std::env::args().collect();
    match args.iter().position(|a| a == name) {
        Some(i) => match args.get(i + 1).and_then(|v| v.parse().ok()) {
            Some(value) => value,
            None => {
                eprintln!("{name}: missing or invalid value");
                std::process::exit(2);
            }
        },
        None => default,
    }
}

fn to_text(json: String) -> Message {
    Message::Text(json.into())
}

async fn handler(State(state): State<Arc<RefState>>, ws: WebSocketUpgrade) -> Response {
    ws.max_message_size(MAX_MESSAGE_SIZE)
        .on_upgrade(move |socket| serve(socket, state))
}

async fn serve(mut socket: WebSocket, state: Arc<RefState>) {
    let id = Uuid::new_v4();
    let mut bus = state.tx.subscribe();

    if socket
        .send(to_text(format!("{{\"type\":\"welcome\",\"id\":\"{id}\"}}")))
        .await
        .is_err()
    {
        return;
    }

    let (mut sink, mut source) = socket.split();
    let tx = state.tx.clone();
    let delivered = Arc::clone(&state.delivered);
    let bridge_state = Arc::clone(&state);
    let (queue_tx, mut queue_rx) = mpsc::channel::<(Instant, Message)>(QUEUE_CAPACITY);

    let mut bridge = tokio::spawn(async move {
        let mut seen: u64 = 0;
        loop {
            let fanout = match bus.recv().await {
                Ok(fanout) if fanout.source == id => continue,
                Ok(fanout) => fanout,
                Err(broadcast::error::RecvError::Lagged(dropped)) => {
                    let warning =
                        to_text(format!("{{\"type\":\"warning\",\"dropped\":{dropped}}}"));
                    if queue_tx.send((Instant::now(), warning)).await.is_err() {
                        return;
                    }
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => return,
            };

            let arrival = Instant::now();
            seen += 1;

            let behaviour = &bridge_state.behaviour;
            if behaviour.drop_1_in > 0 && seen.is_multiple_of(behaviour.drop_1_in) {
                continue;
            }

            if queue_tx.send((arrival, fanout.frame)).await.is_err() {
                return;
            }
        }
    });

    let writer_state = Arc::clone(&state);
    let mut writer = tokio::spawn(async move {
        while let Some((arrival, frame)) = queue_rx.recv().await {
            let behaviour = &writer_state.behaviour;
            behaviour.wait_out_stall().await;

            if !behaviour.delay.is_zero() {
                sleep_until(TokioInstant::from_std(arrival + behaviour.delay)).await;
            }

            if sink.send(frame).await.is_err() {
                return;
            }
            delivered.fetch_add(1, Ordering::Relaxed);
        }
    });

    let read_state = Arc::clone(&state);
    let mut reader = tokio::spawn(async move {
        loop {
            read_state.behaviour.wait_out_stall().await;

            let Some(Ok(message)) = source.next().await else {
                break;
            };

            let frame = match message {
                Message::Text(text) => match serde_json::from_str::<&RawValue>(&text) {
                    Ok(data) => to_text(format!(
                        "{{\"type\":\"message\",\"from\":\"{id}\",\"data\":{data}}}"
                    )),
                    Err(_) => continue,
                },
                Message::Binary(bytes) => Message::Binary(bytes),
                Message::Close(_) => break,
                _ => continue,
            };

            let _ = tx.send(Fanout { source: id, frame });
        }
    });

    tokio::select! {
        _ = &mut writer => (),
        _ = &mut bridge => (),
        _ = &mut reader => (),
    }

    writer.abort();
    bridge.abort();
    reader.abort();
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    if std::env::args().any(|a| a == "--help" || a == "-h") {
        println!("{USAGE}");
        return Ok(());
    }

    let port: u16 = flag("--port", 3100);
    let delay_ms: u64 = flag("--delay-ms", 0);
    let drop_1_in: u64 = flag("--drop-1-in", 0);
    let stall_at: u64 = flag("--stall-at", 0);
    let stall_ms: u64 = flag("--stall-ms", 0);

    let origin = Instant::now();
    let stall_from = (stall_ms > 0).then(|| origin + Duration::from_secs(stall_at));
    let stall_until = stall_from.map(|from| from + Duration::from_millis(stall_ms));

    let delivered = Arc::new(AtomicU64::new(0));
    let (tx, _) = broadcast::channel(BUS_CAPACITY);
    let state = Arc::new(RefState {
        tx,
        delivered: Arc::clone(&delivered),
        behaviour: Behaviour {
            delay: Duration::from_millis(delay_ms),
            drop_1_in,
            stall_from,
            stall_until,
        },
    });

    let counter = Arc::clone(&delivered);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        loop {
            ticker.tick().await;
            eprintln!("deliveries {}", counter.load(Ordering::Relaxed));
        }
    });

    let listener = TcpListener::bind(("127.0.0.1", port)).await?;
    eprintln!(
        "refserver on ws://127.0.0.1:{port}/ws  delay={delay_ms}ms drop_1_in={drop_1_in} \
         stall={stall_ms}ms@{stall_at}s"
    );

    let app = Router::new().route("/ws", get(handler)).with_state(state);
    axum::serve(listener, app).await
}

use std::borrow::Cow;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::Response;
use axum::routing::get;
use axum::{Router, extract::State};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Deserializer};
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

Speaks the gateway's wire protocol, either the legacy one (every frame to every
other connection) or the topic one (subscribe, unsubscribe, publish, binary topic
header), but with behaviour chosen on purpose, so every metric loadgen reports
has a known correct answer. It is a measuring instrument, not a gateway
implementation: topics are routed through one bus with a per-connection filter,
deliberately unlike the gateway's registry.

USAGE:
    cargo run --release --example refserver -- [OPTIONS]

OPTIONS:
    --port <N>             listen port  [default: 3100]
    --protocol <P>         legacy or topics  [default: legacy]
    --delay-ms <D>         hold every delivery for exactly D ms  [default: 0]
    --drop-1-in <K>        discard every Kth delivery per subscriber  [default: 0]
    --warn-drops           report each discarded delivery with a warning frame
    --ignore-topics        topics only: deliver every publish to every connection
    --drop-subscribe-acks <K>    topics only: withhold every Kth subscribed reply,
                                 counted across all connections  [default: 0]
    --drop-unsubscribe-acks <K>  topics only: withhold every Kth unsubscribed reply,
                                 counted across all connections  [default: 0]

Under topics, a connection that falls behind the internal bus aborts the
process: the bus carries every topic, so its lag count cannot say how many of
the skipped frames were meant for that connection, and a reference that
reported a wrong count would be worse than one that stops.
    --stall-at <S>         seconds after startup at which to freeze  [default: 0]
    --stall-ms <M>         how long the freeze lasts; 0 disables  [default: 0]
    --help                 show this message

A freeze stops reading from sockets as well as delivering, so it back-pressures
publishers the way a real overloaded server does.

Cumulative deliveries are printed to stderr once a second as
`deliveries <n>`; the last line is the server's own count, independent of the
client's. With --delay-ms, the time each delivery was actually held (the OS
timer can overshoot the requested delay) is printed as `hold_p50_ms` and
`hold_p99_ms`, so calibration compares the client against what the server did.";

#[derive(Clone, Copy, PartialEq, Eq)]
enum Protocol {
    Legacy,
    Topics,
}

struct Behaviour {
    protocol: Protocol,
    delay: Duration,
    drop_1_in: u64,
    warn_drops: bool,
    ignore_topics: bool,
    drop_subscribe_acks: u64,
    drop_unsubscribe_acks: u64,
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
    topic: Option<Arc<str>>,
    frame: Message,
}

enum Change {
    Subscribe(Arc<str>),
    Unsubscribe(Arc<str>),
}

fn present<'de, D: Deserializer<'de>>(d: D) -> Result<Option<&'de RawValue>, D::Error> {
    <&'de RawValue>::deserialize(d).map(Some)
}

#[derive(Deserialize)]
struct ClientFrame<'a> {
    #[serde(rename = "type", borrow)]
    kind: Cow<'a, str>,
    #[serde(borrow)]
    topic: Cow<'a, str>,
    #[serde(borrow, default, deserialize_with = "present")]
    data: Option<&'a RawValue>,
}

struct RefState {
    tx: broadcast::Sender<Fanout>,
    delivered: Arc<AtomicU64>,
    holds: std::sync::Mutex<Vec<u32>>,
    subscribe_acks: AtomicU64,
    unsubscribe_acks: AtomicU64,
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

fn json_string(value: &str) -> String {
    serde_json::to_string(value).expect("a str always serializes")
}

fn ack(kind: &str, topic: &str) -> Message {
    to_text(format!(
        "{{\"type\":\"{kind}\",\"topic\":{}}}",
        json_string(topic)
    ))
}

fn warning(dropped: u64) -> Message {
    to_text(format!("{{\"type\":\"warning\",\"dropped\":{dropped}}}"))
}

fn binary_fanout(id: Uuid, bytes: &[u8]) -> Option<(Arc<str>, Message)> {
    let len = *bytes.first()? as usize;
    if len == 0 {
        return None;
    }
    let topic = std::str::from_utf8(bytes.get(1..1 + len)?).ok()?;
    let mut out = Vec::with_capacity(bytes.len() + 16);
    out.extend_from_slice(&bytes[..1 + len]);
    out.extend_from_slice(id.as_bytes());
    out.extend_from_slice(&bytes[1 + len..]);
    Some((Arc::from(topic), Message::Binary(out.into())))
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
    let (change_tx, mut change_rx) = mpsc::channel::<Change>(QUEUE_CAPACITY);

    let mut bridge = tokio::spawn(async move {
        let behaviour = &bridge_state.behaviour;
        let mut topics: HashSet<Arc<str>> = HashSet::new();
        let mut seen: u64 = 0;
        loop {
            let fanout = tokio::select! {
                biased;
                Some(change) = change_rx.recv() => {
                    let (reply, counter, every) = match change {
                        Change::Subscribe(topic) => {
                            let reply = ack("subscribed", &topic);
                            topics.insert(topic);
                            (reply, &bridge_state.subscribe_acks, behaviour.drop_subscribe_acks)
                        }
                        Change::Unsubscribe(topic) => {
                            topics.remove(&topic);
                            (
                                ack("unsubscribed", &topic),
                                &bridge_state.unsubscribe_acks,
                                behaviour.drop_unsubscribe_acks,
                            )
                        }
                    };
                    let nth = counter.fetch_add(1, Ordering::Relaxed) + 1;
                    if every > 0 && nth.is_multiple_of(every) {
                        continue;
                    }
                    if queue_tx.send((Instant::now(), reply)).await.is_err() {
                        return;
                    }
                    continue;
                }
                received = bus.recv() => match received {
                    Ok(fanout) if fanout.source == id => continue,
                    Ok(fanout) => fanout,
                    Err(broadcast::error::RecvError::Lagged(dropped)) => {
                        if behaviour.protocol == Protocol::Topics {
                            eprintln!(
                                "refserver: a connection fell {dropped} frames behind the internal bus; \
                                 the calibration load is too high for this reference"
                            );
                            std::process::exit(3);
                        }
                        if queue_tx.send((Instant::now(), warning(dropped))).await.is_err() {
                            return;
                        }
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                },
            };

            if behaviour.protocol == Protocol::Topics && !behaviour.ignore_topics {
                let subscribed = fanout
                    .topic
                    .as_ref()
                    .is_some_and(|topic| topics.contains(topic));
                if !subscribed {
                    continue;
                }
            }

            let arrival = Instant::now();
            seen += 1;

            if behaviour.drop_1_in > 0 && seen.is_multiple_of(behaviour.drop_1_in) {
                if behaviour.warn_drops && queue_tx.send((arrival, warning(1))).await.is_err() {
                    return;
                }
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
                let held = arrival.elapsed().as_micros() as u32;
                if let Ok(mut holds) = writer_state.holds.lock() {
                    holds.push(held);
                }
            }

            if sink.send(frame).await.is_err() {
                return;
            }
            delivered.fetch_add(1, Ordering::Relaxed);
        }
    });

    let read_state = Arc::clone(&state);
    let mut reader = tokio::spawn(async move {
        let protocol = read_state.behaviour.protocol;
        loop {
            read_state.behaviour.wait_out_stall().await;

            let Some(Ok(message)) = source.next().await else {
                break;
            };

            let fanout = match (protocol, message) {
                (_, Message::Close(_)) => break,
                (Protocol::Legacy, Message::Text(text)) => {
                    match serde_json::from_str::<&RawValue>(&text) {
                        Ok(data) => Fanout {
                            source: id,
                            topic: None,
                            frame: to_text(format!(
                                "{{\"type\":\"message\",\"from\":\"{id}\",\"data\":{data}}}"
                            )),
                        },
                        Err(_) => continue,
                    }
                }
                (Protocol::Legacy, Message::Binary(bytes)) => Fanout {
                    source: id,
                    topic: None,
                    frame: Message::Binary(bytes),
                },
                (Protocol::Topics, Message::Text(text)) => {
                    let Ok(frame) = serde_json::from_str::<ClientFrame>(&text) else {
                        continue;
                    };
                    let topic: Arc<str> = Arc::from(frame.topic.as_ref());
                    let change = match (frame.kind.as_ref(), frame.data) {
                        ("subscribe", _) => Change::Subscribe(topic),
                        ("unsubscribe", _) => Change::Unsubscribe(topic),
                        ("publish", Some(data)) => {
                            let envelope = format!(
                                "{{\"type\":\"message\",\"topic\":{},\"from\":\"{id}\",\"data\":{data}}}",
                                json_string(&topic)
                            );
                            let _ = tx.send(Fanout {
                                source: id,
                                topic: Some(topic),
                                frame: to_text(envelope),
                            });
                            continue;
                        }
                        _ => continue,
                    };
                    if change_tx.send(change).await.is_err() {
                        break;
                    }
                    continue;
                }
                (Protocol::Topics, Message::Binary(bytes)) => {
                    let Some((topic, frame)) = binary_fanout(id, &bytes) else {
                        continue;
                    };
                    Fanout {
                        source: id,
                        topic: Some(topic),
                        frame,
                    }
                }
                _ => continue,
            };

            let _ = tx.send(fanout);
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
    let protocol = match flag("--protocol", "legacy".to_string()).as_str() {
        "legacy" => Protocol::Legacy,
        "topics" => Protocol::Topics,
        other => {
            eprintln!("--protocol: expected legacy or topics, got {other:?}");
            std::process::exit(2);
        }
    };
    let switch = |name: &str| std::env::args().any(|a| a == name);
    let warn_drops = switch("--warn-drops");
    let ignore_topics = switch("--ignore-topics");
    let drop_subscribe_acks: u64 = flag("--drop-subscribe-acks", 0);
    let drop_unsubscribe_acks: u64 = flag("--drop-unsubscribe-acks", 0);
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
        holds: std::sync::Mutex::new(Vec::new()),
        subscribe_acks: AtomicU64::new(0),
        unsubscribe_acks: AtomicU64::new(0),
        behaviour: Behaviour {
            protocol,
            delay: Duration::from_millis(delay_ms),
            drop_1_in,
            warn_drops,
            ignore_topics,
            drop_subscribe_acks,
            drop_unsubscribe_acks,
            stall_from,
            stall_until,
        },
    });

    let counter = Arc::clone(&delivered);
    let stats_state = Arc::clone(&state);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        loop {
            ticker.tick().await;
            eprintln!("deliveries {}", counter.load(Ordering::Relaxed));
            let mut holds = match stats_state.holds.lock() {
                Ok(holds) => holds.clone(),
                Err(_) => continue,
            };
            if holds.is_empty() {
                continue;
            }
            holds.sort_unstable();
            let at = |p: f64| holds[((holds.len() - 1) as f64 * p) as usize] as f64 / 1000.0;
            eprintln!("hold_p50_ms {:.3}", at(0.50));
            eprintln!("hold_p99_ms {:.3}", at(0.99));
        }
    });

    let listener = TcpListener::bind(("127.0.0.1", port)).await?;
    eprintln!(
        "refserver on ws://127.0.0.1:{port}/ws  protocol={} delay={delay_ms}ms drop_1_in={drop_1_in} \
         warn_drops={warn_drops} ignore_topics={ignore_topics} stall={stall_ms}ms@{stall_at}s",
        match protocol {
            Protocol::Legacy => "legacy",
            Protocol::Topics => "topics",
        }
    );

    let app = Router::new().route("/ws", get(handler)).with_state(state);
    axum::serve(listener, app).await
}

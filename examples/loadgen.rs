use std::collections::HashMap;
use std::fmt::Write as _;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt, future::join_all};
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::time::{Instant as TokioInstant, sleep, sleep_until, timeout_at};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

const SETTLE: Duration = Duration::from_millis(250);
const SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(10);
const DRAIN_QUIET_MICROS: u64 = 500_000;
const DRAIN_POLL: Duration = Duration::from_millis(20);
const ARRIVAL_STAMP_STEP_MICROS: u64 = 10_000;
const EXTRA_TOPIC: &str = "t-extra";
const CHURN_TOPIC: &str = "t-0";
const LEGACY_TOPIC: &str = "";
const USAGE: &str = "\
loadgen — measure realtime-gateway throughput, latency, delivery, routing and backpressure

USAGE:
    cargo run --release --example loadgen -- [OPTIONS]

OPTIONS:
    --url <URL>               gateway endpoint  [default: ws://127.0.0.1:3000/ws]
    --protocol <P>            legacy (bare JSON, global relay) or topics  [default: legacy]
    --connections <N>         measured sockets to open  [default: 100]
    --topics <T>              topics the measured sockets are split across; topics only  [default: 1]
    --senders <N>             how many measured sockets publish  [default: 5]
    --rate <N>                messages per second per sender  [default: 50]
    --seconds <N>             total send window, warmup included  [default: 15]
    --warmup <N>              seconds discarded at the start  [default: 2]
    --payload-bytes <N>       opaque padding added to each payload  [default: 0]
    --binary                  publish binary frames instead of JSON text
    --extra-topic-rate <R>    every measured socket also joins t-extra, fed by one
                              publish-only socket at R/s; topics only  [default: 0, off]
    --churn <N>               extra sockets that alternately subscribe to and
                              unsubscribe from t-0 during the window; topics only  [default: 0]
    --churn-rate <R>          operations per second per churn socket  [default: 1]
    --drain-max-ms <MS>       give up draining after this long  [default: 30000]
    --json                    emit one JSON line instead of the report
    --help                    show this message

Connection i belongs to topic t-(i mod T) and senders are connections 0..N, so
each sender publishes to its own topic. Every socket is opened, subscribed and
acknowledged before the clock starts. A frame belongs to the measured window by
the timestamp its publisher stamped on it, not by when it arrived.

After the window the generator keeps reading until no frame has arrived for
500 ms, or until --drain-max-ms passes. `drained` says which happened.

Two latencies are reported, because one number cannot tell the truth alone:

    service    arrival − the instant the frame actually left the publisher.
    response   arrival − the instant the schedule said the frame was due.

Correctness meters:

    misrouted    frames on a topic the receiver did not subscribe to; must be 0
    dropped      the sum of every warning's dropped count
    unaccounted  expected − received − dropped over the whole run, for sockets
                 that stayed open (legacy: only sockets that do not publish);
                 null unless drained; must be 0
    subscribe_failed  setup subscriptions never acknowledged; must be 0
    churn_failed      churn operations never acknowledged after a full drain

Keep --senders small and --connections large to isolate fanout cost.
Always measure in --release; a debug number is meaningless.";

const SUB_BITS: u32 = 8;
const LINEAR: usize = 1 << SUB_BITS;
const SUB_SLOTS: usize = LINEAR / 2;
const OCTAVES: usize = 28;
const SLOTS: usize = LINEAR + OCTAVES * SUB_SLOTS;

struct Histogram {
    slots: Vec<u32>,
    count: u64,
    sum: u64,
    max: u64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            slots: vec![0; SLOTS],
            count: 0,
            sum: 0,
            max: 0,
        }
    }
}

fn slot_of(value: u64) -> usize {
    if value < LINEAR as u64 {
        return value as usize;
    }
    let exponent = 63 - value.leading_zeros();
    let octave = (exponent - SUB_BITS) as usize;
    if octave >= OCTAVES {
        return SLOTS - 1;
    }
    let shift = exponent - (SUB_BITS - 1);
    LINEAR + octave * SUB_SLOTS + ((value >> shift) as usize - SUB_SLOTS)
}

fn slot_value(slot: usize) -> u64 {
    if slot < LINEAR {
        return slot as u64;
    }
    let relative = slot - LINEAR;
    let shift = (relative / SUB_SLOTS) as u32 + 1;
    let lower = ((SUB_SLOTS + relative % SUB_SLOTS) as u64) << shift;
    lower + (1u64 << shift) / 2
}

impl Histogram {
    fn record(&mut self, value: u64) {
        self.slots[slot_of(value)] += 1;
        self.count += 1;
        self.sum += value;
        self.max = self.max.max(value);
    }

    fn merge(&mut self, other: &Histogram) {
        for (slot, count) in self.slots.iter_mut().zip(&other.slots) {
            *slot += *count;
        }
        self.count += other.count;
        self.sum += other.sum;
        self.max = self.max.max(other.max);
    }

    fn percentile(&self, p: f64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let target = ((p * self.count as f64).ceil() as u64).clamp(1, self.count);
        let mut seen = 0u64;
        for (slot, count) in self.slots.iter().enumerate() {
            seen += *count as u64;
            if seen >= target {
                return slot_value(slot).min(self.max);
            }
        }
        self.max
    }

    fn mean(&self) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        self.sum as f64 / self.count as f64
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Protocol {
    Legacy,
    Topics,
}

impl FromStr for Protocol {
    type Err = ();

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw {
            "legacy" => Ok(Self::Legacy),
            "topics" => Ok(Self::Topics),
            _ => Err(()),
        }
    }
}

impl Protocol {
    fn name(self) -> &'static str {
        match self {
            Self::Legacy => "legacy",
            Self::Topics => "topics",
        }
    }
}

#[derive(Default)]
struct Received {
    measured: u64,
    warmup: u64,
    outside: u64,
    extra_measured: u64,
    all: u64,
    warnings: u64,
    dropped: u64,
    errors: u64,
    malformed: u64,
    misrouted: u64,
    acks: u64,
    closed_early: bool,
    service: Histogram,
    response: Histogram,
}

#[derive(Default, Clone, Copy)]
struct Published {
    measured: u64,
    warmup: u64,
    slip_sum: u64,
    slip_max: u64,
    closed_early: bool,
}

impl Published {
    fn all(&self) -> u64 {
        self.measured + self.warmup
    }
}

#[derive(Deserialize)]
struct Frame<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    #[serde(default, borrow)]
    topic: Option<&'a str>,
    #[serde(default)]
    data: Option<Stamp>,
    #[serde(default)]
    dropped: Option<u64>,
}

#[derive(Deserialize, Clone, Copy)]
struct Stamp {
    t: u64,
    s: u64,
}

#[derive(Clone, Copy)]
struct Clock {
    start: Instant,
    warmup_micros: u64,
    send_end_micros: u64,
}

impl Clock {
    fn micros_since_start(&self, at: Instant) -> u64 {
        at.saturating_duration_since(self.start).as_micros() as u64
    }
}

struct Shared {
    clock: Clock,
    last_arrival: AtomicU64,
    writers_active: AtomicUsize,
}

struct Config {
    url: String,
    protocol: Protocol,
    connections: usize,
    topics: usize,
    senders: usize,
    rate: u64,
    seconds: u64,
    warmup: u64,
    payload_bytes: usize,
    binary: bool,
    extra_topic_rate: u64,
    churn: usize,
    churn_rate: u64,
    drain_max_ms: u64,
    json: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            url: "ws://127.0.0.1:3000/ws".to_string(),
            protocol: Protocol::Legacy,
            connections: 100,
            topics: 1,
            senders: 5,
            rate: 50,
            seconds: 15,
            warmup: 2,
            payload_bytes: 0,
            binary: false,
            extra_topic_rate: 0,
            churn: 0,
            churn_rate: 1,
            drain_max_ms: 30_000,
            json: false,
        }
    }
}

fn next_value<I: Iterator<Item = String>>(args: &mut I, name: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("{name} requires a value"))
}

fn parse_value<T: FromStr, I: Iterator<Item = String>>(
    args: &mut I,
    name: &str,
) -> Result<T, String> {
    let raw = next_value(args, name)?;
    raw.parse()
        .map_err(|_| format!("{name}: cannot parse {raw:?}"))
}

fn parse_args() -> Result<Config, String> {
    let mut config = Config::default();
    let mut args = std::env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--url" => config.url = next_value(&mut args, "--url")?,
            "--protocol" => config.protocol = parse_value(&mut args, "--protocol")?,
            "--connections" => config.connections = parse_value(&mut args, "--connections")?,
            "--topics" => config.topics = parse_value(&mut args, "--topics")?,
            "--senders" => config.senders = parse_value(&mut args, "--senders")?,
            "--rate" => config.rate = parse_value(&mut args, "--rate")?,
            "--seconds" => config.seconds = parse_value(&mut args, "--seconds")?,
            "--warmup" => config.warmup = parse_value(&mut args, "--warmup")?,
            "--payload-bytes" => config.payload_bytes = parse_value(&mut args, "--payload-bytes")?,
            "--binary" => config.binary = true,
            "--extra-topic-rate" => {
                config.extra_topic_rate = parse_value(&mut args, "--extra-topic-rate")?
            }
            "--churn" => config.churn = parse_value(&mut args, "--churn")?,
            "--churn-rate" => config.churn_rate = parse_value(&mut args, "--churn-rate")?,
            "--drain-max-ms" => config.drain_max_ms = parse_value(&mut args, "--drain-max-ms")?,
            "--json" => config.json = true,
            other => return Err(format!("unknown argument {other:?}")),
        }
    }

    validate(&config)?;
    Ok(config)
}

fn validate(config: &Config) -> Result<(), String> {
    if config.seconds <= config.warmup {
        return Err("--seconds must be greater than --warmup".to_string());
    }
    if config.connections < 2 {
        return Err("--connections must be at least 2".to_string());
    }
    if config.senders == 0 || config.senders > config.connections {
        return Err("--senders must be between 1 and --connections".to_string());
    }
    if config.rate == 0 {
        return Err("--rate must be at least 1".to_string());
    }
    if config.topics == 0 || !config.connections.is_multiple_of(config.topics) {
        return Err("--topics must be at least 1 and divide --connections".to_string());
    }
    if config.churn > 0 && config.churn_rate == 0 {
        return Err("--churn-rate must be at least 1".to_string());
    }
    if config.protocol == Protocol::Legacy {
        if config.topics != 1 {
            return Err("--topics needs --protocol topics".to_string());
        }
        if config.churn > 0 {
            return Err("--churn needs --protocol topics".to_string());
        }
        if config.extra_topic_rate > 0 {
            return Err("--extra-topic-rate needs --protocol topics".to_string());
        }
    }
    Ok(())
}

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

fn frame_kind(text: &str) -> Option<&str> {
    serde_json::from_str::<Frame>(text).ok().map(|f| f.kind)
}

async fn connect_one(url: &str) -> Result<Socket, String> {
    let (mut socket, _) = connect_async(url).await.map_err(|e| e.to_string())?;

    match socket.next().await {
        Some(Ok(Message::Text(text))) => match frame_kind(&text) {
            Some("welcome") => Ok(socket),
            _ => Err(format!("first frame was not a welcome: {text}")),
        },
        Some(Ok(other)) => Err(format!("first frame was not text: {other:?}")),
        Some(Err(e)) => Err(e.to_string()),
        None => Err("closed before welcome".to_string()),
    }
}

fn subscribe_frame(topic: &str) -> Message {
    Message::text(format!("{{\"type\":\"subscribe\",\"topic\":\"{topic}\"}}"))
}

fn unsubscribe_frame(topic: &str) -> Message {
    Message::text(format!(
        "{{\"type\":\"unsubscribe\",\"topic\":\"{topic}\"}}"
    ))
}

struct Joined {
    socket: Socket,
    acked: Vec<String>,
    ack_micros: Vec<u64>,
    failed: u64,
}

async fn open(url: String, topics: Vec<String>) -> Result<Joined, String> {
    let mut socket = connect_one(&url).await?;
    let sent_at = Instant::now();
    for topic in &topics {
        socket
            .send(subscribe_frame(topic))
            .await
            .map_err(|e| e.to_string())?;
    }

    let deadline = TokioInstant::from_std(sent_at + SUBSCRIBE_TIMEOUT);
    let mut pending = topics;
    let mut acked = Vec::new();
    let mut ack_micros = Vec::new();
    while !pending.is_empty() {
        let Ok(Some(Ok(frame))) = timeout_at(deadline, socket.next()).await else {
            break;
        };
        let Message::Text(text) = frame else {
            continue;
        };
        let Ok(parsed) = serde_json::from_str::<Frame>(&text) else {
            continue;
        };
        if parsed.kind != "subscribed" {
            continue;
        }
        let Some(topic) = parsed.topic else {
            continue;
        };
        if let Some(at) = pending.iter().position(|p| p == topic) {
            acked.push(pending.swap_remove(at));
            ack_micros.push(sent_at.elapsed().as_micros() as u64);
        }
    }

    Ok(Joined {
        failed: pending.len() as u64,
        socket,
        acked,
        ack_micros,
    })
}

enum Membership {
    Legacy,
    Topics { home: Option<String>, extra: bool },
    Churn,
}

#[derive(PartialEq, Eq, Debug)]
enum Class {
    Home,
    Extra,
    Misrouted,
    Ignored,
}

fn classify(membership: &Membership, topic: Option<&[u8]>) -> Class {
    match membership {
        Membership::Legacy => Class::Home,
        Membership::Topics { home, extra } => match topic {
            Some(topic) if home.as_deref().map(str::as_bytes) == Some(topic) => Class::Home,
            Some(topic) if *extra && topic == EXTRA_TOPIC.as_bytes() => Class::Extra,
            _ => Class::Misrouted,
        },
        Membership::Churn => match topic {
            Some(topic) if topic == CHURN_TOPIC.as_bytes() => Class::Ignored,
            _ => Class::Misrouted,
        },
    }
}

fn read_stamp(bytes: &[u8], at: usize) -> Option<Stamp> {
    let t = u64::from_le_bytes(bytes.get(at..at + 8)?.try_into().ok()?);
    let s = u64::from_le_bytes(bytes.get(at + 8..at + 16)?.try_into().ok()?);
    Some(Stamp { t, s })
}

fn parse_binary(protocol: Protocol, bytes: &[u8]) -> Option<(Option<&[u8]>, Stamp)> {
    match protocol {
        Protocol::Legacy => Some((None, read_stamp(bytes, 0)?)),
        Protocol::Topics => {
            let len = *bytes.first()? as usize;
            let topic = bytes.get(1..1 + len)?;
            Some((Some(topic), read_stamp(bytes, 1 + len + 16)?))
        }
    }
}

fn record_delivery(stats: &mut Received, clock: &Clock, class: Class, stamp: Stamp, arrived: u64) {
    let in_window = stamp.t >= clock.warmup_micros && stamp.t < clock.send_end_micros;
    match class {
        Class::Home => {
            stats.all += 1;
            if stamp.t < clock.warmup_micros {
                stats.warmup += 1;
            } else if stamp.t >= clock.send_end_micros {
                stats.outside += 1;
            } else {
                stats.measured += 1;
                stats.service.record(arrived.saturating_sub(stamp.s));
                stats.response.record(arrived.saturating_sub(stamp.t));
            }
        }
        Class::Extra => {
            stats.all += 1;
            if in_window {
                stats.extra_measured += 1;
            }
        }
        Class::Misrouted => {
            if in_window {
                stats.misrouted += 1;
            }
        }
        Class::Ignored => {}
    }
}

async fn read_loop(
    mut source: SplitStream<Socket>,
    shared: Arc<Shared>,
    protocol: Protocol,
    membership: Membership,
    mut stop: watch::Receiver<bool>,
) -> Received {
    let mut stats = Received::default();
    let clock = shared.clock;
    let mut last_stored: u64 = 0;

    loop {
        tokio::select! {
            _ = stop.changed() => break,

            frame = source.next() => {
                let arrived = clock.micros_since_start(Instant::now());
                if arrived >= last_stored + ARRIVAL_STAMP_STEP_MICROS {
                    shared.last_arrival.fetch_max(arrived, Ordering::Relaxed);
                    last_stored = arrived;
                }

                let Some(Ok(frame)) = frame else {
                    stats.closed_early = true;
                    break;
                };

                match frame {
                    Message::Text(text) => {
                        let Ok(parsed) = serde_json::from_str::<Frame>(&text) else {
                            stats.malformed += 1;
                            continue;
                        };
                        match parsed.kind {
                            "message" => {
                                let Some(stamp) = parsed.data else {
                                    stats.malformed += 1;
                                    continue;
                                };
                                let class = classify(&membership, parsed.topic.map(str::as_bytes));
                                record_delivery(&mut stats, &clock, class, stamp, arrived);
                            }
                            "warning" => {
                                stats.warnings += 1;
                                stats.dropped += parsed.dropped.unwrap_or(0);
                            }
                            "error" => stats.errors += 1,
                            "subscribed" | "unsubscribed" => stats.acks += 1,
                            _ => {}
                        }
                    }
                    Message::Binary(bytes) => {
                        let Some((topic, stamp)) = parse_binary(protocol, &bytes) else {
                            stats.malformed += 1;
                            continue;
                        };
                        let class = classify(&membership, topic);
                        record_delivery(&mut stats, &clock, class, stamp, arrived);
                    }
                    Message::Close(_) => {
                        stats.closed_early = true;
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    stats
}

struct Publisher {
    topic: Option<String>,
    binary: bool,
    period_micros: u64,
    padding: String,
}

impl Publisher {
    fn frame(&self, due: u64, sent: u64, seq: u64, buffer: &mut String) -> Message {
        if self.binary {
            let topic_len = self.topic.as_ref().map_or(0, |t| 1 + t.len());
            let mut bytes = Vec::with_capacity(topic_len + 16 + self.padding.len());
            if let Some(topic) = &self.topic {
                bytes.push(topic.len() as u8);
                bytes.extend_from_slice(topic.as_bytes());
            }
            bytes.extend_from_slice(&due.to_le_bytes());
            bytes.extend_from_slice(&sent.to_le_bytes());
            bytes.extend_from_slice(self.padding.as_bytes());
            return Message::binary(bytes);
        }

        buffer.clear();
        let padding = &self.padding;
        let _ = match &self.topic {
            Some(topic) => write!(
                buffer,
                "{{\"type\":\"publish\",\"topic\":\"{topic}\",\"data\":\
                 {{\"t\":{due},\"s\":{sent},\"seq\":{seq},\"pad\":\"{padding}\"}}}}"
            ),
            None => write!(
                buffer,
                "{{\"t\":{due},\"s\":{sent},\"seq\":{seq},\"pad\":\"{padding}\"}}"
            ),
        };
        Message::text(buffer.as_str())
    }
}

async fn write_loop(
    mut sink: SplitSink<Socket, Message>,
    clock: Clock,
    publisher: Publisher,
) -> Published {
    let mut stats = Published::default();
    let mut buffer = String::with_capacity(publisher.padding.len() + 128);
    let mut seq: u64 = 0;

    loop {
        let due_micros = seq * publisher.period_micros;
        if due_micros >= clock.send_end_micros {
            break;
        }

        let due = clock.start + Duration::from_micros(due_micros);
        sleep_until(TokioInstant::from_std(due)).await;

        let awake = Instant::now();
        let sent_micros = clock.micros_since_start(awake);
        let slip = awake.saturating_duration_since(due).as_micros() as u64;

        let frame = publisher.frame(due_micros, sent_micros, seq, &mut buffer);
        if sink.send(frame).await.is_err() {
            stats.closed_early = true;
            break;
        }

        if due_micros >= clock.warmup_micros {
            stats.measured += 1;
            stats.slip_sum += slip;
            stats.slip_max = stats.slip_max.max(slip);
        } else {
            stats.warmup += 1;
        }

        seq += 1;
    }

    let _ = sink.flush().await;
    stats
}

async fn churn_loop(
    mut sink: SplitSink<Socket, Message>,
    clock: Clock,
    period_micros: u64,
    offset_micros: u64,
) -> u64 {
    let mut ops: u64 = 0;
    loop {
        let due_micros = clock.warmup_micros + offset_micros + ops * period_micros;
        if due_micros >= clock.send_end_micros {
            break;
        }
        sleep_until(TokioInstant::from_std(
            clock.start + Duration::from_micros(due_micros),
        ))
        .await;
        let frame = if ops.is_multiple_of(2) {
            subscribe_frame(CHURN_TOPIC)
        } else {
            unsubscribe_frame(CHURN_TOPIC)
        };
        if sink.send(frame).await.is_err() {
            break;
        }
        ops += 1;
    }
    ops
}

enum Action {
    Publish(Publisher),
    Churn {
        period_micros: u64,
        offset_micros: u64,
    },
    Idle,
}

struct Outcome {
    published: Published,
    received: Received,
    churn_ops: u64,
}

async fn run_socket(
    socket: Socket,
    shared: Arc<Shared>,
    protocol: Protocol,
    membership: Membership,
    action: Action,
    mut stop: watch::Receiver<bool>,
) -> Outcome {
    let (sink, source) = socket.split();
    let reader = tokio::spawn(read_loop(
        source,
        Arc::clone(&shared),
        protocol,
        membership,
        stop.clone(),
    ));
    let clock = shared.clock;

    let (published, churn_ops) = match action {
        Action::Publish(publisher) => {
            let published = write_loop(sink, clock, publisher).await;
            shared.writers_active.fetch_sub(1, Ordering::Relaxed);
            (published, 0)
        }
        Action::Churn {
            period_micros,
            offset_micros,
        } => {
            let ops = churn_loop(sink, clock, period_micros, offset_micros).await;
            shared.writers_active.fetch_sub(1, Ordering::Relaxed);
            (Published::default(), ops)
        }
        Action::Idle => {
            let _ = stop.changed().await;
            drop(sink);
            (Published::default(), 0)
        }
    };

    Outcome {
        published,
        received: reader.await.unwrap_or_default(),
        churn_ops,
    }
}

fn drain_state(
    now: u64,
    drain_start: u64,
    last_arrival: u64,
    quiet: u64,
    limit: u64,
) -> Option<bool> {
    if now.saturating_sub(drain_start.max(last_arrival)) >= quiet {
        return Some(true);
    }
    if now.saturating_sub(drain_start) >= limit {
        return Some(false);
    }
    None
}

async fn coordinate(
    shared: Arc<Shared>,
    stop: watch::Sender<bool>,
    limit_micros: u64,
) -> (bool, f64) {
    let clock = shared.clock;
    let writers_deadline = clock.send_end_micros + limit_micros;
    loop {
        let now = clock.micros_since_start(Instant::now());
        if shared.writers_active.load(Ordering::Relaxed) == 0 {
            break;
        }
        if now >= writers_deadline {
            let _ = stop.send(true);
            return (false, (now - clock.send_end_micros) as f64 / 1e6);
        }
        sleep(DRAIN_POLL).await;
    }

    let drain_start = clock.micros_since_start(Instant::now());
    loop {
        sleep(DRAIN_POLL).await;
        let now = clock.micros_since_start(Instant::now());
        let last_arrival = shared.last_arrival.load(Ordering::Relaxed);
        if let Some(drained) = drain_state(
            now,
            drain_start,
            last_arrival,
            DRAIN_QUIET_MICROS,
            limit_micros,
        ) {
            let _ = stop.send(true);
            return (drained, (now - drain_start) as f64 / 1e6);
        }
    }
}

#[derive(Default, Clone, Copy)]
struct Sent {
    measured: u64,
    all: u64,
}

struct ReceiverExpectation {
    home_measured: u64,
    extra_measured: u64,
    all: u64,
}

fn expectation(
    topics: &[String],
    own: Option<(&str, Published)>,
    sent: &HashMap<String, Sent>,
) -> ReceiverExpectation {
    let mut expectation = ReceiverExpectation {
        home_measured: 0,
        extra_measured: 0,
        all: 0,
    };
    for topic in topics {
        let total = sent.get(topic).copied().unwrap_or_default();
        let (own_measured, own_all) = match own {
            Some((own_topic, published)) if own_topic == topic => {
                (published.measured, published.all())
            }
            _ => (0, 0),
        };
        let measured = total.measured.saturating_sub(own_measured);
        if topic == EXTRA_TOPIC {
            expectation.extra_measured += measured;
        } else {
            expectation.home_measured += measured;
        }
        expectation.all += total.all.saturating_sub(own_all);
    }
    expectation
}

fn in_unaccounted_scope(protocol: Protocol, publishes: bool, closed_early: bool) -> bool {
    !closed_early && (protocol == Protocol::Topics || !publishes)
}

enum Role {
    Measured {
        topics: Vec<String>,
        publishes_to: Option<String>,
    },
    ExtraPublisher,
    Churner,
}

fn div_ceil(a: u64, b: u64) -> u64 {
    a.div_ceil(b)
}

fn ms(micros: u64) -> f64 {
    micros as f64 / 1000.0
}

fn json_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "null".to_string(), |v| v.to_string())
}

fn json_i64(value: Option<i64>) -> String {
    value.map_or_else(|| "null".to_string(), |v| v.to_string())
}

fn json_f64(value: Option<f64>, decimals: usize) -> String {
    value.map_or_else(|| "null".to_string(), |v| format!("{v:.decimals$}"))
}

fn text_value(value: &str) -> &str {
    if value == "null" { "n/a" } else { value }
}

fn home_topic(protocol: Protocol, index: usize, topics: usize) -> String {
    match protocol {
        Protocol::Legacy => LEGACY_TOPIC.to_string(),
        Protocol::Topics => format!("t-{}", index % topics),
    }
}

#[tokio::main]
async fn main() {
    if std::env::args().any(|a| a == "--help" || a == "-h") {
        println!("{USAGE}");
        return;
    }

    let config = match parse_args() {
        Ok(config) => config,
        Err(e) => {
            eprintln!("{e}\n\n{USAGE}");
            std::process::exit(2);
        }
    };

    let protocol = config.protocol;
    let period_micros = 1_000_000 / config.rate;
    let effective_rate = 1_000_000.0 / period_micros as f64;
    let measured_seconds = (config.seconds - config.warmup) as f64;
    let warmup_micros = config.warmup * 1_000_000;
    let send_end_micros = config.seconds * 1_000_000;
    let padding = "x".repeat(config.payload_bytes);
    let has_extra = config.extra_topic_rate > 0;

    if !config.json {
        eprintln!(
            "opening {} sockets over {} topic(s) ({} publishing at {:.1}/s, {} B padding, {} protocol{})",
            config.connections,
            config.topics,
            config.senders,
            effective_rate,
            config.payload_bytes,
            protocol.name(),
            if config.binary { ", binary" } else { "" }
        );
    }

    let mut roles = Vec::new();
    let mut joins = Vec::new();
    for i in 0..config.connections {
        let home = home_topic(protocol, i, config.topics);
        let mut topics = Vec::new();
        if protocol == Protocol::Topics {
            topics.push(home.clone());
            if has_extra {
                topics.push(EXTRA_TOPIC.to_string());
            }
        }
        roles.push(Role::Measured {
            topics: if protocol == Protocol::Topics {
                topics.clone()
            } else {
                vec![LEGACY_TOPIC.to_string()]
            },
            publishes_to: (i < config.senders).then_some(home),
        });
        joins.push(open(config.url.clone(), topics));
    }
    if has_extra {
        roles.push(Role::ExtraPublisher);
        joins.push(open(config.url.clone(), Vec::new()));
    }
    for _ in 0..config.churn {
        roles.push(Role::Churner);
        joins.push(open(config.url.clone(), Vec::new()));
    }

    let attempts = join_all(joins).await;

    let mut failed: u64 = 0;
    let mut first_failure = None;
    let mut subscribe_failed: u64 = 0;
    let mut ack_hist = Histogram::default();
    let mut plans = Vec::new();
    for (role, attempt) in roles.into_iter().zip(attempts) {
        match attempt {
            Ok(joined) => {
                subscribe_failed += joined.failed;
                for micros in &joined.ack_micros {
                    ack_hist.record(*micros);
                }
                let role = match role {
                    Role::Measured {
                        topics,
                        publishes_to,
                    } if protocol == Protocol::Topics => Role::Measured {
                        topics: topics
                            .into_iter()
                            .filter(|t| joined.acked.contains(t))
                            .collect(),
                        publishes_to,
                    },
                    other => other,
                };
                plans.push((role, joined.socket));
            }
            Err(e) => {
                failed += 1;
                first_failure.get_or_insert(e);
            }
        }
    }

    let established = plans
        .iter()
        .filter(|(role, _)| matches!(role, Role::Measured { .. }))
        .count() as u64;
    if established < 2 {
        eprintln!("only {established} measured sockets established; nothing to measure");
        if let Some(e) = first_failure {
            eprintln!("first failure: {e}");
        }
        std::process::exit(1);
    }

    tokio::time::sleep(SETTLE).await;

    let clock = Clock {
        start: Instant::now(),
        warmup_micros,
        send_end_micros,
    };
    let writers = plans
        .iter()
        .filter(|(role, _)| match role {
            Role::Measured { publishes_to, .. } => publishes_to.is_some(),
            Role::ExtraPublisher | Role::Churner => true,
        })
        .count();
    let shared = Arc::new(Shared {
        clock,
        last_arrival: AtomicU64::new(0),
        writers_active: AtomicUsize::new(writers),
    });
    let (stop_tx, stop_rx) = watch::channel(false);

    if !config.json {
        eprintln!(
            "{established} measured sockets established, {failed} failed, {subscribe_failed} subscriptions unacknowledged; \
             {}s warmup + {}s measured, then drain",
            config.warmup, measured_seconds as u64
        );
    }

    let churn_period = 1_000_000 / config.churn_rate.max(1);
    let mut churn_index: u64 = 0;
    let mut metas = Vec::new();
    let mut tasks = Vec::new();
    for (role, socket) in plans {
        let (membership, action) = match &role {
            Role::Measured {
                topics,
                publishes_to,
            } => {
                let membership = match protocol {
                    Protocol::Legacy => Membership::Legacy,
                    Protocol::Topics => Membership::Topics {
                        home: topics.iter().find(|t| *t != EXTRA_TOPIC).cloned(),
                        extra: topics.iter().any(|t| t == EXTRA_TOPIC),
                    },
                };
                let action = match publishes_to {
                    Some(topic) => Action::Publish(Publisher {
                        topic: (protocol == Protocol::Topics).then(|| topic.clone()),
                        binary: config.binary,
                        period_micros,
                        padding: padding.clone(),
                    }),
                    None => Action::Idle,
                };
                (membership, action)
            }
            Role::ExtraPublisher => (
                Membership::Topics {
                    home: None,
                    extra: false,
                },
                Action::Publish(Publisher {
                    topic: Some(EXTRA_TOPIC.to_string()),
                    binary: config.binary,
                    period_micros: 1_000_000 / config.extra_topic_rate,
                    padding: padding.clone(),
                }),
            ),
            Role::Churner => {
                let offset = churn_period * churn_index / config.churn.max(1) as u64;
                churn_index += 1;
                (
                    Membership::Churn,
                    Action::Churn {
                        period_micros: churn_period,
                        offset_micros: offset,
                    },
                )
            }
        };
        metas.push(role);
        tasks.push(tokio::spawn(run_socket(
            socket,
            Arc::clone(&shared),
            protocol,
            membership,
            action,
            stop_rx.clone(),
        )));
    }
    drop(stop_rx);

    let (drained, drain_seconds) =
        coordinate(Arc::clone(&shared), stop_tx, config.drain_max_ms * 1000).await;
    let outcomes = join_all(tasks).await;

    let mut sent_by_topic: HashMap<String, Sent> = HashMap::new();
    let mut home_sent = Published::default();
    let mut extra_sent = Sent::default();
    let mut results = Vec::new();
    let mut closed_early: u64 = 0;
    for (role, outcome) in metas.into_iter().zip(outcomes) {
        let Ok(outcome) = outcome else {
            closed_early += 1;
            continue;
        };
        let published = outcome.published;
        match &role {
            Role::Measured {
                publishes_to: Some(topic),
                ..
            } => {
                let entry = sent_by_topic.entry(topic.clone()).or_default();
                entry.measured += published.measured;
                entry.all += published.all();
                home_sent.measured += published.measured;
                home_sent.warmup += published.warmup;
                home_sent.slip_sum += published.slip_sum;
                home_sent.slip_max = home_sent.slip_max.max(published.slip_max);
            }
            Role::ExtraPublisher => {
                let entry = sent_by_topic.entry(EXTRA_TOPIC.to_string()).or_default();
                entry.measured += published.measured;
                entry.all += published.all();
                extra_sent = *entry;
            }
            _ => {}
        }
        if published.closed_early || outcome.received.closed_early {
            closed_early += 1;
        }
        results.push((role, outcome));
    }

    let mut got = Received::default();
    let mut expected: u64 = 0;
    let mut extra_expected: u64 = 0;
    let mut scope: u64 = 0;
    let mut scope_expected: u64 = 0;
    let mut unaccounted_sum: i64 = 0;
    let mut churn_ops: u64 = 0;
    let mut churn_acks: u64 = 0;
    for (role, outcome) in &results {
        let received = &outcome.received;
        got.warnings += received.warnings;
        got.dropped += received.dropped;
        got.errors += received.errors;
        got.malformed += received.malformed;
        got.misrouted += received.misrouted;
        match role {
            Role::Measured {
                topics,
                publishes_to,
            } => {
                got.measured += received.measured;
                got.warmup += received.warmup;
                got.outside += received.outside;
                got.extra_measured += received.extra_measured;
                got.service.merge(&received.service);
                got.response.merge(&received.response);
                let own = publishes_to
                    .as_deref()
                    .map(|topic| (topic, outcome.published));
                let expect = expectation(topics, own, &sent_by_topic);
                expected += expect.home_measured;
                extra_expected += expect.extra_measured;
                let closed = outcome.published.closed_early || received.closed_early;
                if in_unaccounted_scope(protocol, publishes_to.is_some(), closed) {
                    scope += 1;
                    scope_expected += expect.all;
                    unaccounted_sum +=
                        expect.all as i64 - received.all as i64 - received.dropped as i64;
                }
            }
            Role::Churner => {
                churn_ops += outcome.churn_ops;
                churn_acks += received.acks;
            }
            Role::ExtraPublisher => {}
        }
    }

    let topics_mode = protocol == Protocol::Topics;
    let sent_expected = (div_ceil(send_end_micros, period_micros)
        - div_ceil(warmup_micros, period_micros))
        * config.senders as u64;
    let delivery = if expected == 0 {
        0.0
    } else {
        got.measured as f64 * 100.0 / expected as f64
    };
    let throughput = got.measured as f64 / measured_seconds;
    let slip_mean = if home_sent.measured == 0 {
        0.0
    } else {
        home_sent.slip_sum as f64 / home_sent.measured as f64
    };
    let unaccounted = (drained && scope > 0).then_some(unaccounted_sum);
    let churn_failed = (topics_mode && drained).then(|| churn_ops.saturating_sub(churn_acks));
    let subscribe_failed_out = topics_mode.then_some(subscribe_failed);
    let misrouted_out = topics_mode.then_some(got.misrouted);
    let ack_p50 = (topics_mode && ack_hist.count > 0).then(|| ms(ack_hist.percentile(0.50)));
    let ack_p99 = (topics_mode && ack_hist.count > 0).then(|| ms(ack_hist.percentile(0.99)));
    let extra_sent_out = has_extra.then_some(extra_sent.measured);
    let extra_expected_out = has_extra.then_some(extra_expected);
    let extra_received_out = has_extra.then_some(got.extra_measured);
    let extra_delivery = (has_extra && extra_expected > 0)
        .then(|| got.extra_measured as f64 * 100.0 / extra_expected as f64);

    if config.json {
        println!(
            "{{\"connections\":{},\"senders\":{},\"rate\":{},\"payload_bytes\":{},\
             \"seconds\":{},\"warmup\":{},\"measured_seconds\":{},\
             \"established\":{established},\"failed_conns\":{failed},\"closed_early\":{closed_early},\
             \"sent\":{},\"sent_expected\":{sent_expected},\"received\":{},\"expected\":{expected},\
             \"received_all\":{},\
             \"delivery_pct\":{:.4},\"throughput_msg_s\":{:.1},\
             \"service_p50_ms\":{:.3},\"service_p99_ms\":{:.3},\"service_p999_ms\":{:.3},\
             \"service_max_ms\":{:.3},\"service_mean_ms\":{:.3},\
             \"response_p50_ms\":{:.3},\"response_p99_ms\":{:.3},\"response_p999_ms\":{:.3},\
             \"response_max_ms\":{:.3},\"response_mean_ms\":{:.3},\
             \"samples\":{},\"slip_mean_ms\":{:.3},\"slip_max_ms\":{:.3},\
             \"warnings\":{},\"errors\":{},\"malformed\":{},\"outside_window\":{},\
             \"protocol\":\"{}\",\"topics\":{},\"binary\":{},\"churn\":{},\"churn_rate\":{},\
             \"extra_topic_rate\":{},\"subscribe_failed\":{},\"subscribe_ack_p50_ms\":{},\
             \"subscribe_ack_p99_ms\":{},\"misrouted\":{},\"dropped\":{},\"unaccounted\":{},\
             \"unaccounted_scope\":{scope},\"scope_expected\":{scope_expected},\
             \"drained\":{drained},\"drain_seconds\":{drain_seconds:.3},\"churn_failed\":{},\
             \"extra_sent\":{},\"extra_expected\":{},\"extra_received\":{},\"extra_delivery_pct\":{}}}",
            config.connections,
            config.senders,
            config.rate,
            config.payload_bytes,
            config.seconds,
            config.warmup,
            measured_seconds,
            home_sent.measured,
            got.measured,
            got.measured + got.warmup + got.outside,
            delivery,
            throughput,
            ms(got.service.percentile(0.50)),
            ms(got.service.percentile(0.99)),
            ms(got.service.percentile(0.999)),
            ms(got.service.max),
            got.service.mean() / 1000.0,
            ms(got.response.percentile(0.50)),
            ms(got.response.percentile(0.99)),
            ms(got.response.percentile(0.999)),
            ms(got.response.max),
            got.response.mean() / 1000.0,
            got.service.count,
            slip_mean / 1000.0,
            ms(home_sent.slip_max),
            got.warnings,
            got.errors,
            got.malformed,
            got.outside,
            protocol.name(),
            config.topics,
            config.binary,
            config.churn,
            config.churn_rate,
            config.extra_topic_rate,
            json_u64(subscribe_failed_out),
            json_f64(ack_p50, 3),
            json_f64(ack_p99, 3),
            json_u64(misrouted_out),
            got.dropped,
            json_i64(unaccounted),
            json_u64(churn_failed),
            json_u64(extra_sent_out),
            json_u64(extra_expected_out),
            json_u64(extra_received_out),
            json_f64(extra_delivery, 4),
        );
        return;
    }

    println!();
    println!(
        "connections   {established} established, {failed} failed, {closed_early} closed early"
    );
    println!(
        "sent          {} frames (arithmetic {sent_expected})",
        home_sent.measured
    );
    println!(
        "received      {} of {expected} expected  ({delivery:.4}% delivered)",
        got.measured
    );
    println!("throughput    {throughput:.0} msg/s");

    if got.service.count == 0 {
        println!("latency       no samples");
    } else {
        for (label, hist) in [("service ", &got.service), ("response", &got.response)] {
            println!(
                "lat {label}  p50 {:.3}ms  p99 {:.3}ms  p999 {:.3}ms  max {:.3}ms  mean {:.3}ms",
                ms(hist.percentile(0.50)),
                ms(hist.percentile(0.99)),
                ms(hist.percentile(0.999)),
                ms(hist.max),
                hist.mean() / 1000.0
            );
        }
    }

    println!(
        "send slip     mean {:.3}ms  max {:.3}ms",
        slip_mean / 1000.0,
        ms(home_sent.slip_max)
    );
    println!(
        "warnings      {} (backpressure), {} frames reported dropped",
        got.warnings, got.dropped
    );
    println!("errors        {}", got.errors);
    println!(
        "routing       misrouted {}, subscribe_failed {}, subscribe ack p99 {}ms",
        text_value(&json_u64(misrouted_out)),
        text_value(&json_u64(subscribe_failed_out)),
        text_value(&json_f64(ack_p99, 3))
    );
    println!(
        "accounting    unaccounted {} over {scope} sockets ({scope_expected} expected), \
         drained {drained} after {drain_seconds:.3}s",
        text_value(&json_i64(unaccounted))
    );
    if has_extra {
        println!(
            "extra topic   {} of {extra_expected} expected  ({}% delivered)",
            got.extra_measured,
            text_value(&json_f64(extra_delivery, 4))
        );
    }
    if config.churn > 0 {
        println!(
            "churn         {churn_ops} operations, {} unacknowledged",
            text_value(&json_u64(churn_failed))
        );
    }
    println!(
        "discarded     {} warmup, {} outside the window, {} malformed",
        got.warmup, got.outside, got.malformed
    );

    if home_sent.measured != sent_expected {
        println!();
        println!(
            "the generator published {} frames where the schedule called for {sent_expected}; \
             it did not sustain the requested rate",
            home_sent.measured
        );
    }
    if got.warnings > 0 {
        println!();
        println!(
            "consumers fell behind and the gateway dropped messages for them; the throughput above is not clean"
        );
    }
    if !drained {
        println!();
        println!(
            "delivery had not settled when the drain limit was reached; unaccounted is not computed"
        );
    }
    if slip_mean > 1000.0 {
        println!();
        println!(
            "mean send slip is over 1ms: the generator was late leaving the gate, so that much of \
             the response latency is harness cost, not gateway cost"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn error_of(reported: u64, truth: u64) -> f64 {
        (reported as f64 - truth as f64).abs() / truth as f64
    }

    #[test]
    fn slots_round_trip_within_one_percent() {
        for value in 0..LINEAR as u64 {
            assert_eq!(slot_value(slot_of(value)), value);
        }
        for value in [256u64, 257, 511, 512, 1_000, 50_000, 1_000_000, 60_000_000] {
            assert!(error_of(slot_value(slot_of(value)), value) <= 0.01);
        }
    }

    #[test]
    fn constant_distribution_is_exact_within_bucket_error() {
        let mut hist = Histogram::default();
        for _ in 0..10_000 {
            hist.record(50_000);
        }
        assert_eq!(hist.count, 10_000);
        assert_eq!(hist.max, 50_000);
        for p in [0.5, 0.99, 0.999] {
            assert!(error_of(hist.percentile(p), 50_000) <= 0.01);
        }
    }

    #[test]
    fn uniform_distribution_matches_known_percentiles() {
        let mut hist = Histogram::default();
        for value in 1..=100_000u64 {
            hist.record(value);
        }
        assert!(error_of(hist.percentile(0.50), 50_000) <= 0.01);
        assert!(error_of(hist.percentile(0.99), 99_000) <= 0.01);
        assert!(error_of(hist.percentile(0.999), 99_900) <= 0.01);
        assert_eq!(hist.max, 100_000);
    }

    #[test]
    fn bimodal_distribution_separates_the_tail() {
        let mut hist = Histogram::default();
        for _ in 0..99_000 {
            hist.record(1_000);
        }
        for _ in 0..1_000 {
            hist.record(500_000);
        }
        assert!(error_of(hist.percentile(0.50), 1_000) <= 0.01);
        assert!(error_of(hist.percentile(0.99), 1_000) <= 0.01);
        assert!(error_of(hist.percentile(0.999), 500_000) <= 0.01);
        assert_eq!(hist.max, 500_000);
    }

    #[test]
    fn merge_preserves_percentiles() {
        let mut left = Histogram::default();
        let mut right = Histogram::default();
        for value in 1..=50_000u64 {
            left.record(value);
        }
        for value in 50_001..=100_000u64 {
            right.record(value);
        }
        left.merge(&right);
        assert_eq!(left.count, 100_000);
        assert!(error_of(left.percentile(0.50), 50_000) <= 0.01);
        assert!(error_of(left.percentile(0.99), 99_000) <= 0.01);
    }

    #[test]
    fn empty_histogram_reports_zero() {
        let hist = Histogram::default();
        assert_eq!(hist.percentile(0.5), 0);
        assert_eq!(hist.mean(), 0.0);
    }

    #[test]
    fn scheduled_sends_match_the_window() {
        let period = 1_000_000 / 50;
        let count = div_ceil(15 * 1_000_000, period) - div_ceil(2 * 1_000_000, period);
        assert_eq!(count, 650);
    }

    fn published(measured: u64, warmup: u64) -> Published {
        Published {
            measured,
            warmup,
            ..Published::default()
        }
    }

    fn topic_names(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    #[test]
    fn single_topic_reduces_to_the_old_formula() {
        let senders = 5u64;
        let connections = 200u64;
        let per_sender = published(500, 100);
        let mut sent = HashMap::new();
        sent.insert(
            LEGACY_TOPIC.to_string(),
            Sent {
                measured: senders * 500,
                all: senders * 600,
            },
        );
        let topics = topic_names(&[LEGACY_TOPIC]);
        let mut total = 0;
        for i in 0..connections {
            let own = (i < senders).then_some((LEGACY_TOPIC, per_sender));
            total += expectation(&topics, own, &sent).home_measured;
        }
        assert_eq!(total, senders * 500 * (connections - 1));
    }

    #[test]
    fn uneven_acknowledgements_shrink_only_their_topic() {
        let mut sent = HashMap::new();
        sent.insert(
            "t-0".to_string(),
            Sent {
                measured: 100,
                all: 120,
            },
        );
        sent.insert(
            "t-1".to_string(),
            Sent {
                measured: 100,
                all: 120,
            },
        );
        let sender = expectation(
            &topic_names(&["t-0"]),
            Some(("t-0", published(100, 20))),
            &sent,
        );
        assert_eq!(sender.home_measured, 0);
        assert_eq!(sender.all, 0);
        let receiver = expectation(&topic_names(&["t-0"]), None, &sent);
        assert_eq!(receiver.home_measured, 100);
        assert_eq!(receiver.all, 120);
        let unacknowledged = expectation(&[], None, &sent);
        assert_eq!(unacknowledged.home_measured, 0);
        assert_eq!(unacknowledged.all, 0);
    }

    #[test]
    fn extra_topic_is_counted_apart() {
        let mut sent = HashMap::new();
        sent.insert(
            "t-0".to_string(),
            Sent {
                measured: 100,
                all: 120,
            },
        );
        sent.insert(
            EXTRA_TOPIC.to_string(),
            Sent {
                measured: 10,
                all: 12,
            },
        );
        let receiver = expectation(&topic_names(&["t-0", EXTRA_TOPIC]), None, &sent);
        assert_eq!(receiver.home_measured, 100);
        assert_eq!(receiver.extra_measured, 10);
        assert_eq!(receiver.all, 132);
    }

    #[test]
    fn legacy_scope_excludes_publishers_and_closed_sockets() {
        assert!(in_unaccounted_scope(Protocol::Legacy, false, false));
        assert!(!in_unaccounted_scope(Protocol::Legacy, true, false));
        assert!(in_unaccounted_scope(Protocol::Topics, true, false));
        assert!(!in_unaccounted_scope(Protocol::Topics, false, true));
    }

    #[test]
    fn drain_waits_for_quiet_and_gives_up_at_the_limit() {
        assert_eq!(drain_state(100, 0, 0, 500, 1_000), None);
        assert_eq!(drain_state(500, 0, 0, 500, 1_000), Some(true));
        assert_eq!(drain_state(900, 0, 800, 500, 1_000), None);
        assert_eq!(drain_state(1_000, 0, 900, 500, 1_000), Some(false));
        assert_eq!(drain_state(1_300, 0, 800, 500, 1_000), Some(true));
    }

    #[test]
    fn frames_are_routed_by_membership() {
        let member = Membership::Topics {
            home: Some("t-1".to_string()),
            extra: true,
        };
        assert_eq!(classify(&member, Some(b"t-1")), Class::Home);
        assert_eq!(
            classify(&member, Some(EXTRA_TOPIC.as_bytes())),
            Class::Extra
        );
        assert_eq!(classify(&member, Some(b"t-2")), Class::Misrouted);
        assert_eq!(classify(&member, None), Class::Misrouted);
        assert_eq!(classify(&Membership::Legacy, None), Class::Home);
        assert_eq!(classify(&Membership::Churn, Some(b"t-0")), Class::Ignored);
        assert_eq!(classify(&Membership::Churn, Some(b"t-1")), Class::Misrouted);
    }

    #[test]
    fn binary_frames_round_trip() {
        let publisher = Publisher {
            topic: Some("t-7".to_string()),
            binary: true,
            period_micros: 1,
            padding: "xx".to_string(),
        };
        let Message::Binary(sent) = publisher.frame(11, 22, 0, &mut String::new()) else {
            panic!("expected binary");
        };
        let mut delivered = vec![sent[0]];
        delivered.extend_from_slice(&sent[1..4]);
        delivered.extend_from_slice(&[0u8; 16]);
        delivered.extend_from_slice(&sent[4..]);
        let (topic, stamp) = parse_binary(Protocol::Topics, &delivered).unwrap();
        assert_eq!(topic, Some(&b"t-7"[..]));
        assert_eq!((stamp.t, stamp.s), (11, 22));
        assert!(parse_binary(Protocol::Topics, &[5, b'k']).is_none());
    }
}

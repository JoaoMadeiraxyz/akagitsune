use std::fmt::Write as _;
use std::str::FromStr;
use std::time::{Duration, Instant};

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt, future::join_all};
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio::time::{Instant as TokioInstant, sleep_until};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

const DRAIN: Duration = Duration::from_secs(1);
const SETTLE: Duration = Duration::from_millis(250);
const USAGE: &str = "\
loadgen — measure realtime-gateway throughput, latency, delivery and backpressure

USAGE:
    cargo run --release --example loadgen -- [OPTIONS]

OPTIONS:
    --url <URL>            gateway endpoint  [default: ws://127.0.0.1:3000/ws]
    --connections <N>      total sockets to open  [default: 100]
    --senders <N>          how many of them publish  [default: 5]
    --rate <N>             messages per second per sender  [default: 50]
    --seconds <N>          total run, warmup included  [default: 15]
    --warmup <N>           seconds discarded at the start  [default: 2]
    --payload-bytes <N>    opaque padding added to each payload  [default: 0]
    --json                 emit one JSON line instead of the report
    --help                 show this message

All sockets are opened and acknowledged before the clock starts. A frame belongs
to the measured window by the timestamp the publisher stamped on it, not by when
it arrived, so the throughput denominator matches its numerator exactly and
in-flight frames are drained after the window instead of being lost.

Two latencies are reported, because one number cannot tell the truth alone:

    service    arrival − the instant the frame actually left the publisher.
               What the server did, with the generator's own lateness removed.
    response   arrival − the instant the schedule said the frame was due.
               Includes any delay in getting the frame out, so a publisher
               blocked by backpressure shows up here instead of disappearing.

Reading only `service` hides coordinated omission; reading only `response`
charges the gateway for the harness's timer. Report both.

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

#[derive(Default)]
struct Received {
    measured: u64,
    warmup: u64,
    outside: u64,
    warnings: u64,
    errors: u64,
    malformed: u64,
    closed_early: bool,
    service: Histogram,
    response: Histogram,
}

#[derive(Default)]
struct Published {
    measured: u64,
    warmup: u64,
    slip_sum: u64,
    slip_max: u64,
    closed_early: bool,
}

#[derive(Deserialize)]
struct Frame<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    #[serde(default)]
    data: Option<Stamp>,
}

#[derive(Deserialize)]
struct Stamp {
    t: u64,
    s: u64,
}

#[derive(Clone, Copy)]
struct Timeline {
    start: Instant,
    warmup_micros: u64,
    send_end_micros: u64,
    read_end: Instant,
}

struct Config {
    url: String,
    connections: usize,
    senders: usize,
    rate: u64,
    seconds: u64,
    warmup: u64,
    payload_bytes: usize,
    json: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            url: "ws://127.0.0.1:3000/ws".to_string(),
            connections: 100,
            senders: 5,
            rate: 50,
            seconds: 15,
            warmup: 2,
            payload_bytes: 0,
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
            "--connections" => config.connections = parse_value(&mut args, "--connections")?,
            "--senders" => config.senders = parse_value(&mut args, "--senders")?,
            "--rate" => config.rate = parse_value(&mut args, "--rate")?,
            "--seconds" => config.seconds = parse_value(&mut args, "--seconds")?,
            "--warmup" => config.warmup = parse_value(&mut args, "--warmup")?,
            "--payload-bytes" => config.payload_bytes = parse_value(&mut args, "--payload-bytes")?,
            "--json" => config.json = true,
            other => return Err(format!("unknown argument {other:?}")),
        }
    }

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

    Ok(config)
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

async fn read_loop(mut source: SplitStream<Socket>, timeline: Timeline) -> Received {
    let mut stats = Received::default();

    loop {
        tokio::select! {
            _ = sleep_until(TokioInstant::from_std(timeline.read_end)) => break,

            frame = source.next() => {
                let arrival = Instant::now();

                let Some(Ok(frame)) = frame else {
                    stats.closed_early = true;
                    break;
                };

                let text = match frame {
                    Message::Text(text) => text,
                    Message::Close(_) => {
                        stats.closed_early = true;
                        break;
                    }
                    _ => continue,
                };

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
                        if stamp.t < timeline.warmup_micros {
                            stats.warmup += 1;
                            continue;
                        }
                        if stamp.t >= timeline.send_end_micros {
                            stats.outside += 1;
                            continue;
                        }
                        stats.measured += 1;
                        let arrived =
                            arrival.saturating_duration_since(timeline.start).as_micros() as u64;
                        stats.service.record(arrived.saturating_sub(stamp.s));
                        stats.response.record(arrived.saturating_sub(stamp.t));
                    }
                    "warning" => stats.warnings += 1,
                    "error" => stats.errors += 1,
                    _ => {}
                }
            }
        }
    }

    stats
}

async fn write_loop(
    mut sink: SplitSink<Socket, Message>,
    timeline: Timeline,
    period_micros: u64,
    padding: &str,
) -> Published {
    let mut stats = Published::default();
    let mut buffer = String::with_capacity(padding.len() + 64);
    let mut seq: u64 = 0;

    loop {
        let due_micros = seq * period_micros;
        if due_micros >= timeline.send_end_micros {
            break;
        }

        let due = timeline.start + Duration::from_micros(due_micros);
        sleep_until(TokioInstant::from_std(due)).await;

        let awake = Instant::now();
        let sent_micros = awake.saturating_duration_since(timeline.start).as_micros() as u64;
        let slip = awake.saturating_duration_since(due).as_micros() as u64;

        buffer.clear();
        let _ = write!(
            buffer,
            "{{\"t\":{due_micros},\"s\":{sent_micros},\"seq\":{seq},\"pad\":\"{padding}\"}}"
        );

        if sink.send(Message::text(buffer.as_str())).await.is_err() {
            stats.closed_early = true;
            break;
        }

        if due_micros >= timeline.warmup_micros {
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

async fn idle(sink: SplitSink<Socket, Message>, until: Instant) -> Published {
    sleep_until(TokioInstant::from_std(until)).await;
    drop(sink);
    Published::default()
}

async fn run_connection(
    socket: Socket,
    timeline: Timeline,
    publish: Option<(u64, String)>,
) -> (Published, Received) {
    let (sink, source) = socket.split();
    let reader = tokio::spawn(read_loop(source, timeline));

    let published = match publish {
        Some((period_micros, padding)) => write_loop(sink, timeline, period_micros, &padding).await,
        None => idle(sink, timeline.read_end).await,
    };

    let received = reader.await.unwrap_or_default();
    (published, received)
}

fn div_ceil(a: u64, b: u64) -> u64 {
    a.div_ceil(b)
}

fn ms(micros: u64) -> f64 {
    micros as f64 / 1000.0
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

    let period_micros = 1_000_000 / config.rate;
    let effective_rate = 1_000_000.0 / period_micros as f64;
    let measured_seconds = (config.seconds - config.warmup) as f64;
    let warmup_micros = config.warmup * 1_000_000;
    let send_end_micros = config.seconds * 1_000_000;
    let padding = "x".repeat(config.payload_bytes);

    if !config.json {
        eprintln!(
            "opening {} sockets ({} publishing at {:.1}/s, {} B padding)",
            config.connections, config.senders, effective_rate, config.payload_bytes
        );
    }

    let attempts = join_all((0..config.connections).map(|_| connect_one(&config.url))).await;

    let mut sockets = Vec::with_capacity(config.connections);
    let mut failed: u64 = 0;
    let mut first_failure = None;
    for attempt in attempts {
        match attempt {
            Ok(socket) => sockets.push(socket),
            Err(e) => {
                failed += 1;
                first_failure.get_or_insert(e);
            }
        }
    }

    if sockets.len() < 2 {
        eprintln!(
            "only {} sockets established; nothing to measure",
            sockets.len()
        );
        if let Some(e) = first_failure {
            eprintln!("first failure: {e}");
        }
        std::process::exit(1);
    }

    let established = sockets.len() as u64;
    let senders = config.senders.min(sockets.len());

    tokio::time::sleep(SETTLE).await;

    let start = Instant::now();
    let timeline = Timeline {
        start,
        warmup_micros,
        send_end_micros,
        read_end: start + Duration::from_secs(config.seconds) + DRAIN,
    };

    if !config.json {
        eprintln!(
            "{established} established, {failed} failed; {}s warmup + {}s measured + {}s drain",
            config.warmup,
            measured_seconds as u64,
            DRAIN.as_secs()
        );
    }

    let runs = join_all(sockets.into_iter().enumerate().map(|(i, socket)| {
        let publish = (i < senders).then(|| (period_micros, padding.clone()));
        tokio::spawn(run_connection(socket, timeline, publish))
    }))
    .await;

    let mut sent = Published::default();
    let mut got = Received::default();
    let mut closed_early: u64 = 0;

    for run in runs {
        let Ok((published, received)) = run else {
            closed_early += 1;
            continue;
        };
        sent.measured += published.measured;
        sent.warmup += published.warmup;
        sent.slip_sum += published.slip_sum;
        sent.slip_max = sent.slip_max.max(published.slip_max);
        got.measured += received.measured;
        got.warmup += received.warmup;
        got.outside += received.outside;
        got.warnings += received.warnings;
        got.errors += received.errors;
        got.malformed += received.malformed;
        got.service.merge(&received.service);
        got.response.merge(&received.response);
        if published.closed_early || received.closed_early {
            closed_early += 1;
        }
    }

    let sent_expected = (div_ceil(send_end_micros, period_micros)
        - div_ceil(warmup_micros, period_micros))
        * senders as u64;
    let expected = sent.measured * (established - 1);
    let delivery = if expected == 0 {
        0.0
    } else {
        got.measured as f64 * 100.0 / expected as f64
    };
    let throughput = got.measured as f64 / measured_seconds;
    let slip_mean = if sent.measured == 0 {
        0.0
    } else {
        sent.slip_sum as f64 / sent.measured as f64
    };

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
             \"warnings\":{},\"errors\":{},\"malformed\":{},\"outside_window\":{}}}",
            config.connections,
            config.senders,
            config.rate,
            config.payload_bytes,
            config.seconds,
            config.warmup,
            measured_seconds,
            sent.measured,
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
            ms(sent.slip_max),
            got.warnings,
            got.errors,
            got.malformed,
            got.outside,
        );
        return;
    }

    println!();
    println!(
        "connections   {established} established, {failed} failed, {closed_early} closed early"
    );
    println!(
        "sent          {} frames (arithmetic {sent_expected})",
        sent.measured
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
        ms(sent.slip_max)
    );
    println!("warnings      {} (backpressure)", got.warnings);
    println!("errors        {}", got.errors);
    println!(
        "discarded     {} warmup, {} outside the window, {} malformed",
        got.warmup, got.outside, got.malformed
    );

    if sent.measured != sent_expected {
        println!();
        println!(
            "the generator published {} frames where the schedule called for {sent_expected}; \
             it did not sustain the requested rate",
            sent.measured
        );
    }
    if got.warnings > 0 {
        println!();
        println!(
            "consumers fell behind and the gateway dropped messages for them; the throughput above is not clean"
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
}

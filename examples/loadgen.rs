use std::str::FromStr;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt, future::join_all};
use serde_json::{Value, json};
use tokio::time::{Instant as TokioInstant, interval, sleep_until};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const WARMUP: Duration = Duration::from_secs(2);
const MAX_SAMPLES_PER_CONNECTION: usize = 20_000;
const USAGE: &str = "\
loadgen — measure realtime-gateway throughput, latency and backpressure

USAGE:
    cargo run --release --example loadgen -- [OPTIONS]

OPTIONS:
    --url <URL>            gateway endpoint  [default: ws://127.0.0.1:3000/ws]
    --connections <N>      total sockets to open  [default: 100]
    --senders <N>          how many of them publish  [default: 5]
    --rate <N>             messages per second per sender  [default: 50]
    --seconds <N>          measurement window, warmup included  [default: 15]
    --help                 show this message

Keep --senders small and --connections large to isolate fanout cost.
Always measure in --release; a debug number is meaningless.";

#[derive(Default)]
struct Stats {
    sent: u64,
    received: u64,
    warnings: u64,
    errors: u64,
    latencies: Vec<u64>,
}

fn flag<T: FromStr>(name: &str, default: T) -> T {
    let args: Vec<String> = std::env::args().collect();
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (p * (sorted.len() - 1) as f64).round() as usize;
    sorted[rank]
}

fn micros(d: Duration) -> u64 {
    d.as_micros() as u64
}

async fn run_connection(
    url: String,
    is_sender: bool,
    rate: u64,
    start: Instant,
    warmup_end: Instant,
    deadline: Instant,
) -> Result<Stats, String> {
    let (stream, _) = connect_async(&url).await.map_err(|e| e.to_string())?;
    let (mut sink, mut source) = stream.split();

    let mut stats = Stats::default();
    let mut seq: u64 = 0;
    let mut ticker = interval(Duration::from_micros(1_000_000 / rate.max(1)));

    loop {
        tokio::select! {
            _ = sleep_until(TokioInstant::from_std(deadline)) => break,

            _ = ticker.tick(), if is_sender => {
                let payload = json!({"t": micros(start.elapsed()), "seq": seq});
                if sink.send(Message::text(payload.to_string())).await.is_err() {
                    break;
                }
                seq += 1;
                stats.sent += 1;
            }

            frame = source.next() => {
                let Some(Ok(frame)) = frame else { break };
                let now = Instant::now();

                let text = match frame {
                    Message::Text(text) => text,
                    Message::Binary(_) => {
                        stats.received += 1;
                        continue;
                    }
                    Message::Close(_) => break,
                    _ => continue,
                };

                let Ok(value) = serde_json::from_str::<Value>(&text) else { continue };

                match value["type"].as_str() {
                    Some("message") => {
                        stats.received += 1;
                        if now < warmup_end || stats.latencies.len() >= MAX_SAMPLES_PER_CONNECTION {
                            continue;
                        }
                        if let Some(sent_at) = value["data"]["t"].as_u64() {
                            stats.latencies.push(micros(start.elapsed()).saturating_sub(sent_at));
                        }
                    }
                    Some("warning") => stats.warnings += 1,
                    Some("error") => stats.errors += 1,
                    _ => {}
                }
            }
        }
    }

    Ok(stats)
}

#[tokio::main]
async fn main() {
    if std::env::args().any(|a| a == "--help" || a == "-h") {
        println!("{USAGE}");
        return;
    }

    let url: String = flag("--url", "ws://127.0.0.1:3000/ws".to_string());
    let connections: usize = flag("--connections", 100);
    let senders: usize = flag("--senders", 5).min(connections);
    let rate: u64 = flag("--rate", 50);
    let seconds: u64 = flag("--seconds", 15);

    if seconds <= WARMUP.as_secs() {
        eprintln!(
            "--seconds must be greater than the {}s warmup",
            WARMUP.as_secs()
        );
        std::process::exit(1);
    }

    let measured = Duration::from_secs(seconds) - WARMUP;
    println!(
        "connecting {connections} sockets ({senders} sending at {rate}/s), \
         {}s warmup + {}s measured",
        WARMUP.as_secs(),
        measured.as_secs()
    );

    let start = Instant::now();
    let warmup_end = start + WARMUP;
    let deadline = start + Duration::from_secs(seconds);

    let results = join_all(
        (0..connections)
            .map(|i| run_connection(url.clone(), i < senders, rate, start, warmup_end, deadline)),
    )
    .await;

    let mut established: u64 = 0;
    let mut failed: u64 = 0;
    let mut total = Stats::default();

    for result in results {
        match result {
            Ok(stats) => {
                established += 1;
                total.sent += stats.sent;
                total.received += stats.received;
                total.warnings += stats.warnings;
                total.errors += stats.errors;
                total.latencies.extend(stats.latencies);
            }
            Err(_) => failed += 1,
        }
    }

    total.latencies.sort_unstable();
    let elapsed = measured.as_secs_f64();
    let expected = total.sent.saturating_mul(established.saturating_sub(1));

    println!();
    println!("connections   {established} established, {failed} failed");
    println!("sent          {} frames", total.sent);
    println!(
        "received      {} frames ({:.0}/s)",
        total.received,
        total.received as f64 / elapsed
    );
    println!("expected      {expected} deliveries over the full run");
    println!("samples       {}", total.latencies.len());

    if total.latencies.is_empty() {
        println!("latency       no samples");
    } else {
        println!(
            "latency       p50 {:.2}ms  p99 {:.2}ms  max {:.2}ms",
            percentile(&total.latencies, 0.50) as f64 / 1000.0,
            percentile(&total.latencies, 0.99) as f64 / 1000.0,
            total.latencies[total.latencies.len() - 1] as f64 / 1000.0
        );
    }

    println!("warnings      {} (backpressure)", total.warnings);
    println!("errors        {}", total.errors);

    if total.warnings > 0 {
        println!();
        println!(
            "consumers fell behind and the gateway dropped messages for them; throughput above is not clean"
        );
    }
}

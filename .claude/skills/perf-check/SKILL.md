---
name: perf-check
description: Measure realtime-gateway throughput, latency and backpressure with the loadgen harness before making any performance or scalability claim. Use when changing the hot path (src/ws.rs reader or bridge, src/state.rs BroadcastMessage, the broadcast bus), when tuning capacities, when investigating slowness or lag warnings, or when asked how fast or how scalable the gateway is.
---

# perf-check

No performance claim without a measurement. The gateway's cost model is
documented in `docs/architecture.md`, but nothing has been benchmarked — treat
any unmeasured number in conversation as a guess and say so.

## Invariants first

Before measuring, confirm the change did not break a structural invariant, since
a violation is a regression regardless of what the benchmark says:

- Payload never deserialized (`&RawValue` only).
- Envelope serialized once by the publisher, not per receiver.
- `BroadcastMessage` clone stays a refcount bump.
- No locks, allocations or formatting added to the fanout loop.

A microbenchmark will often fail to show these — at low connection counts the
per-receiver cost is invisible. They matter at scale, which is exactly where you
will not be measuring.

## Running the harness

```bash
cargo run --release &
cargo run --release --example loadgen -- \
  --url ws://127.0.0.1:3000/ws \
  --connections 200 --senders 5 --rate 50 --seconds 20
```

`--connections` total sockets, `--senders` how many of them publish, `--rate`
messages per second per sender, `--seconds` measurement window.

## Methodology

Most benchmarking mistakes here are methodology, not tooling.

- **Always `--release`.** A debug-build number is meaningless and must never be
  recorded or quoted.
- **Separate senders from receivers.** With every connection publishing, fanout
  is `senders × rate × (connections − 1)` and the load generator saturates before
  the gateway does. Keep `--senders` small and `--connections` large to isolate
  fanout cost; raise `--senders` only when measuring ingest.
- **Watch for a client-side bottleneck.** If throughput plateaus while the
  server's CPU is idle, you are measuring the harness. Reduce load or run the
  generator on another machine before drawing conclusions.
- **Warm up.** Discard the first seconds; the harness does this, but a very
  short `--seconds` makes the window mostly warmup.
- **Compare against a baseline, never an absolute.** Measure the current commit,
  then the change, on the same machine in the same session. A single number in
  isolation says nothing.
- **`warning` count is the backpressure signal.** Non-zero means consumers fell
  more than `BROADCAST_CAPACITY` behind and the gateway dropped messages for
  them. Throughput that looks good with warnings climbing is not good throughput
  — report both or neither.
- **Server memory is measured externally**, not by the harness:
  `ps -o rss= -p $(pgrep -f realtime-gateway)`. Take it with the gateway idle and
  again under load; divide the delta by connection count for a per-connection
  figure. Do not report a number the harness did not produce as if it had.

## Reporting

Report throughput, p50 and p99 latency, connection failures and `warning` count
together. Include the invocation, the build profile and the hardware — a number
without its conditions is not reproducible.

When a result is a meaningful new baseline, append it to the *Measured
baselines* table in `docs/architecture.md` with date, commit and conditions.

If the measurement contradicts an expectation, say so directly and investigate
rather than re-running until it agrees.

---
name: perf-check
description: Measure realtime-gateway throughput, latency and backpressure with the loadgen harness before making any performance or scalability claim. Use when changing the hot path (src/ws.rs reader or bridge, src/state.rs BroadcastMessage, the broadcast bus), when tuning capacities, when investigating slowness or lag warnings, or when asked how fast or how scalable the gateway is.
---

# perf-check

No performance claim without a measurement, and no measurement from an
uncalibrated harness. Treat any unmeasured number in conversation as a guess and
say so.

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

## Performance goal

Project target (see `docs/architecture.md`):

| Metric | Target |
|--------|--------|
| Throughput | ≥ 1 000 000 deliveries/s |
| service p99 | ≤ 20 ms (band 10–20 ms) |
| Delivery | ≥ 99.9% |
| Warnings | 0 |

Never claim the goal is met without a calibrated `scripts/bench.sh --goal` run
that shows at least one `goal-1m-*` scenario at offered load inside that SLO.
A cliff peak with delivery below 99.9% does not count. If the verdict is
`harness-bound`, say so — that measured the load generator, not the gateway.

## Running the harness

```bash
scripts/bench.sh              # calibrate, baseline sweep, write CSV + table
scripts/bench.sh --quick      # one 200-connection fanout run
scripts/bench.sh --goal       # 1M goal + stretch scenarios and pass/miss verdict
scripts/bench.sh --all        # baseline + goal + stretch, with verdict
scripts/calibrate.sh          # harness self-check only
```

`bench.sh` starts a fresh gateway per scenario, samples its RSS and CPU
alongside the run, writes `bench-results/<timestamp>.csv` (or `*-goal.csv` /
`*-all.csv`), and prints a table shaped for the *Measured baselines* section of
`docs/architecture.md`. It runs `calibrate.sh` first and refuses to produce
baselines if it fails.

A single run by hand:

```bash
cargo run --release &
cargo run --release --example loadgen -- \
  --connections 200 --senders 5 --rate 50 --seconds 15
```

## Reading the output

- **Two latencies, both required.** `service` is arrival minus the instant the
  frame actually left the publisher — what the gateway did. `response` is
  arrival minus the instant the schedule said it was due — it includes a
  publisher stalled by backpressure. Quoting only `service` hides coordinated
  omission; quoting only `response` charges the gateway for the harness's timer.
- **`slip_mean_ms` is the harness's confession.** It is how late the generator
  woke up to publish. On this machine it sits around 1.5-2.5 ms under load and
  accounts for the gap between the two latency columns. A large slip means the
  generator did not sustain the requested rate and the run describes a different
  load than the one you asked for.
- **`sent` versus `sent_expected`.** `sent_expected` is arithmetic:
  `senders x rate x measured_seconds`. If they differ, the generator fell behind
  and the run is not the experiment you specified.
- **`delivery_pct`, not just throughput.** In a clean run it is 100.0000% and
  throughput equals `senders x rate x (connections - 1)` exactly, because
  everything offered was delivered — that measures latency and cost at a known
  load, not capacity. Below 100% the run is past the cliff.
- **`warnings` is the backpressure signal.** Non-zero means consumers fell more
  than `BROADCAST_CAPACITY` behind and the gateway dropped messages for them.
  Throughput that looks good with warnings climbing is not good throughput.
- **Compare the CPU columns.** If `loadgen_cpu_pct` dwarfs `gateway_cpu_pct`,
  the harness saturated first and you measured the harness. This is what
  happens in the `cliff-300` scenario on a 14-core machine.

## Methodology

- **Always `--release`.** A debug-build number is meaningless and must never be
  recorded or quoted.
- **Separate senders from receivers.** Keep `--senders` small and
  `--connections` large to isolate fanout cost; raise `--senders` only when
  measuring ingest.
- **Compare against a baseline, never an absolute.** Measure the current commit,
  then the change, on the same machine in the same session.
- **Anything under about 1.5 ms of service latency is at the noise floor** of a
  same-machine loopback run — see the calibration section of
  `docs/architecture.md`.

## When the harness itself changes

`examples/loadgen.rs` and `examples/refserver.rs` are measuring instruments. A
change to either invalidates every baseline taken with the old one. Re-run
`scripts/calibrate.sh`, and if a case fails, fix the harness rather than widen
the tolerance — the predicted values are arithmetic, not preferences. Histogram
and schedule arithmetic are unit tested: `cargo test --examples`.

## Reporting

Report throughput, both latencies, delivery, connection failures and `warning`
count together, with the invocation, the build profile and the hardware. When a
result is a meaningful new baseline, append it to the *Measured baselines* table
in `docs/architecture.md`. Goal/stretch rows from `--goal` go in the performance
goal section of that doc, with the verdict (`pass` / `miss` / `harness-bound` /
`cliff` / `latency`).

If the measurement contradicts an expectation, say so directly and investigate
rather than re-running until it agrees.

# Architecture

## Connection lifecycle

`handle_socket` in `src/ws.rs` owns one connection from upgrade to close:

1. A `Uuid` is generated for the connection.
2. It subscribes to the broadcast bus **before** anything else, so messages
   published while the connection is still being set up are buffered rather than
   dropped.
3. It sends `{"type":"welcome","id":"<uuid>"}`.
4. The connection counter is incremented and three tasks are spawned.

There is no handshake. A client is a participant the moment it connects.

## The three tasks

The socket is split, and each direction gets its own task, with a bounded mpsc
queue between them:

| Task        | Job                                                                                                                                            |
|-------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| **reader**  | Reads frames from the socket. Text is validated as JSON and wrapped in an envelope; binary is passed through untouched. Publishes to the bus.  |
| **bridge**  | Receives from the bus, skips messages the connection itself published, and forwards to the local queue. Turns `Lagged` into a `warning` frame. |
| **writer**  | Drains the local queue in batches (`recv_many`) and issues one `flush` per batch, instead of one per message.                                  |

Only the writer touches the sink, so no locking is needed around it.

`tokio::select!` waits for the first of the three to finish, then aborts the
other two. Any termination reason — client close, socket error, bus closed —
tears the whole connection down through one path.

## Message flow

```
client A ──► reader ──► broadcast bus ──► bridge (B) ──► queue (B) ──► writer (B) ──► client B
                                     └──► bridge (C) ──► queue (C) ──► writer (C) ──► client C
```

The envelope is built **once**, by A's reader. What travels on the bus is the
finished `Message`, so B and C do no serialization work at all.

## Hot-path invariants

Each of these is a property of the current code. Breaking one is a regression
even if tests still pass.

- **The payload is never deserialized.** `src/ws.rs` parses text frames as
  `serde_json::from_str::<&RawValue>`, which validates that the bytes are JSON
  without building a `Value` tree, then splices them into the envelope verbatim.
  No `serde_json::Value`, no typed struct, no field access.
- **One serialization per message, not per receiver.** The envelope is built in
  the publishing connection's reader task.
- **Cloning a broadcast message is a refcount bump, not a copy.**
  `BroadcastMessage` in `src/state.rs` carries an `axum` `Message`, whose `Text`
  and `Binary` variants are backed by `Utf8Bytes`/`Bytes`. The bus clones once
  per subscriber; that clone must stay O(1).
- **No locks on the hot path.** The only shared mutable state is an
  `AtomicUsize` connection counter, touched twice per connection lifetime.
- **Never `.await` while holding a lock.** Currently trivial to satisfy — there
  are no locks. Keep it that way.
- **Bounded everywhere.** The bus holds `BROADCAST_CAPACITY` messages, each
  connection's local queue holds `LOCAL_QUEUE_SIZE`, and frames are capped at
  `MAX_MESSAGE_SIZE`. A slow client degrades into `warning` frames; it never
  grows memory without bound and never blocks a publisher.
- **No per-message logging in the fanout path.** Publishing and forwarding a
  message never formats a `Uuid` or any other per-frame data into a log line —
  that cost scales with fanout. Connect, disconnect, and the accumulated lag
  count are logged twice per connection lifetime, not per message.

## Backpressure

A slow consumer falls behind the bus. Once it is more than `BROADCAST_CAPACITY`
messages behind, `broadcast::Receiver::recv` returns `Lagged(n)`, the gateway
logs it and sends the client `{"type":"warning","dropped":n}`, and delivery
continues from the current position.

This is a deliberate trade: the gateway drops messages for the slow client
rather than slowing down every other client or buffering without limit. Clients
that cannot tolerate loss must detect `warning` and recover at their own layer.

## Scalability limits

Honest current state. None of these numbers have been measured yet — they are
structural properties of the design, not benchmark results.

| Limit                | Value                           | Consequence                                                                                                                                        |
|----------------------|---------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| Routing              | One global bus                  | Every connection receives every message. There is no way to address a subset.                                                                      |
| Fanout cost          | O(N²)                           | With `N` connections all publishing at rate `R`, the gateway performs `N × R × (N−1)` deliveries per second. Doubling connections quadruples work. |
| Bus depth            | `BROADCAST_CAPACITY = 256`      | A consumer more than 256 messages behind starts losing messages.                                                                                   |
| Per-connection queue | `LOCAL_QUEUE_SIZE = 32`         | Buffer between the bus and the socket.                                                                                                             |
| Message size         | `MAX_MESSAGE_SIZE = 64 KiB`     | Larger frames are rejected at the WebSocket layer and the connection is closed.                                                                    |
| Per-connection cost  | 3 tasks, 1 mpsc, 1 bus receiver | Task overhead dominates at high connection counts.                                                                                                 |
| Process model        | Single process, no backplane    | The ceiling is one machine. Two instances share nothing; clients on different instances cannot reach each other.                                   |

### What would have to change

- **Topics/rooms** is the highest-value next step. It replaces `O(N²)` global
  fanout with fanout bounded by topic size, and it is what makes targeted
  notifications and isolated game sessions possible. Until it exists, this
  gateway is a broadcaster, not a router.
- **Horizontal scale** needs a backplane (Redis pub/sub, NATS, a gossip mesh) so
  instances relay to each other. That decision is open; it interacts with topics
  and should not be made before them.
- **Very high connection counts** would justify revisiting three tasks per
  connection — the reader and bridge can be merged into one `select!` loop at
  the cost of some clarity. Do not do this without a measurement showing task
  overhead matters.

## Performance goal

The project target is a single process sustaining:

| Metric | Target |
|--------|--------|
| Throughput | ≥ **1 000 000** deliveries per second |
| service p99 | **10–20 ms** (passes at ≤ 20 ms; lower is better) |
| Delivery | ≥ **99.9%** |
| Warnings | **0** on a clean run |

A *delivery* is one framed message received by one subscriber — the same unit
`loadgen` reports as throughput. On a clean run that equals
`senders × rate × (connections / topics − 1)`, which is
`senders × rate × (connections − 1)` on today's single bus (`topics = 1`).
Frames on the extra quiet topic (`--extra-topic-rate`) are reported apart and
are not deliveries in this sense.

The goal is **sustained offered load within SLO**, not the peak number printed
while consumers are lagging. A cliff row that shows multi-million msg/s with
delivery well below 99.9% does not count. The latency bar is **service p99**
(arrival minus the instant the frame left the publisher), not response p99.

### Status

**Met on the current gateway.** Measured at `0bc858b` with the calibrated
harness (`--protocol legacy`, machine and conditions under *Measured
baselines*):

- **All three goal scenarios pass:** `goal-1m-fanout`, `goal-1m-ingest` and
  `goal-1m-mesh` sustain 1 000 000 deliveries/s at 100% delivery, zero
  warnings, and a service p99 of 5.1–6.0 ms.
- **Five of the six stretch scenarios pass,** up to `beyond-2m-fanout` and
  `beyond-2m-mesh` at 2 000 000 deliveries/s (service p99 10.9 ms and 4.2 ms).
- **`beyond-3m-explore` is the first row that does not hold:** 2 994 931
  deliveries/s at 99.831% delivery with 500 warnings, verdict `cliff`.
- **`cliff-300` holds** 3 588 000 deliveries/s at 100% with zero warnings, on
  a machine at load average 3.4-11. It is load-sensitive, see *How to read these*.

The earlier "not yet met" came from two things:

- **Never measured.** The goal scenarios had never been run on a calibrated
  harness.
- **A different `loadgen`.** The previous build lost about 28% of the frames
  on `cliff-300` in every run, and the rebuilt one does not (see *Superseded: measured with the previous
  harness*).

### Goal and stretch scenarios

`scripts/bench.sh --goal` runs only these. Offered load is exact arithmetic.
Default `scripts/bench.sh` keeps the cheaper baseline sweep.
`scripts/bench.sh --all` runs the baseline sweep followed by these goal and
stretch scenarios, and prints the same pass/miss verdict.

| Scenario | Shape | Invocation | Offered |
|----------|-------|------------|---------|
| `goal-1m-fanout` | few publishers, many receivers | `--connections 501 --senders 10 --rate 200` | 1 000 000 msg/s |
| `goal-1m-ingest` | many publishers, few receivers | `--connections 101 --senders 50 --rate 200` | 1 000 000 msg/s |
| `goal-1m-mesh` | many publishers and receivers | `--connections 201 --senders 50 --rate 100` | 1 000 000 msg/s |
| `beyond-1.5m-fanout` | fanout stretch | `--connections 501 --senders 15 --rate 200` | 1 500 000 msg/s |
| `beyond-1.5m-ingest` | ingest stretch | `--connections 101 --senders 50 --rate 300` | 1 500 000 msg/s |
| `beyond-1.5m-mesh` | mesh stretch | `--connections 251 --senders 50 --rate 120` | 1 500 000 msg/s |
| `beyond-2m-fanout` | fanout stretch | `--connections 1001 --senders 10 --rate 200` | 2 000 000 msg/s |
| `beyond-2m-mesh` | mesh stretch | `--connections 201 --senders 50 --rate 200` | 2 000 000 msg/s |
| `beyond-3m-explore` | aggressive explore | `--connections 301 --senders 50 --rate 200` | 3 000 000 msg/s |
| `goal-1m-topics` | 100 topics of 21; `--protocol topics` only | `--connections 2100 --topics 100 --senders 100 --rate 500` | 1 000 000 msg/s |

The 1M goal is **hit** when at least one `goal-1m-*` row sustains its offered
load with delivery ≥ 99.9%, service p99 ≤ 20 ms, zero warnings, and the
generator holding the requested publish rate. Stretch rows use the same SLO at
higher offered load; they measure how far past 1M the process still holds, they
are not required to pass for the goal to count.

On a same-machine run the load generator can saturate first (see the cliff row
below). A `harness-bound` verdict means the measurement described the harness,
not the gateway ceiling — do not quote it as either a hit or a gateway failure.

Results from `scripts/bench.sh --all --protocol legacy` at `0bc858b`, same
machine and conditions as *Measured baselines*. `goal-1m-topics` needs
`--protocol topics` and was skipped:

| Scenario | Offered | Throughput | service p99 | Delivery | Warnings | Verdict | Gateway CPU | Loadgen CPU | Peak RSS | RSS per connection |
|----------|---------|------------|-------------|----------|----------|---------|-------------|-------------|----------|--------------------|
| goal-1m-fanout | 1 000 000 | 1 000 000 | 6.000 ms | 100% | 0 | pass | 315% | 219% | 75.1 MiB | 147.2 KiB |
| goal-1m-ingest | 1 000 000 | 1 000 000 | 5.072 ms | 100% | 0 | pass | 219% | 173% | 18.6 MiB | 158.2 KiB |
| goal-1m-mesh | 1 000 000 | 1 000 000 | 5.744 ms | 100% | 0 | pass | 202% | 160% | 32.5 MiB | 150.3 KiB |
| beyond-1.5m-fanout | 1 500 000 | 1 500 000 | 5.872 ms | 100% | 0 | pass | 352% | 242% | 75.4 MiB | 148.0 KiB |
| beyond-1.5m-ingest | 1 500 000 | 1 500 000 | 2.728 ms | 100% | 0 | pass | 278% | 182% | 18.4 MiB | 156.1 KiB |
| beyond-1.5m-mesh | 1 500 000 | 1 500 000 | 5.200 ms | 100% | 0 | pass | 210% | 131% | 39.6 MiB | 148.9 KiB |
| beyond-2m-fanout | 2 000 000 | 2 000 000 | 10.912 ms | 100% | 0 | pass | 598% | 350% | 144.0 MiB | 144.1 KiB |
| beyond-2m-mesh | 2 000 000 | 2 000 000 | 4.240 ms | 100% | 0 | pass | 284% | 195% | 32.7 MiB | 151.3 KiB |
| beyond-3m-explore | 3 000 000 | 2 994 931 | 5.648 ms | 99.831% | 500 | cliff | 373% | 281% | 47.0 MiB | 149.7 KiB |

The 1M goal is hit in 3 of 3 goal scenarios and 5 of 6 stretch scenarios pass.
These are one run each. A pass at 2M on this machine says the process held that
load for the measured window, not that 2M is a ceiling or a guarantee, and
`beyond-3m-explore` shows a run that already drops frames.

## Measured baselines

Record results here when `perf-check` produces them, with the hardware, the
build profile, and the exact `loadgen` invocation — a number without its
conditions is not a baseline. `scripts/bench.sh` prints the table in this shape
and refuses to run if `scripts/calibrate.sh` fails first. Goal and stretch
numbers come from `scripts/bench.sh --goal`.

**Conditions:**
- Apple M4 Pro, 14 cores, `--release`, gateway and load generator on the same
  machine.
- Measured at commit `0bc858b`, driven by
  `bash scripts/bench.sh --all --protocol legacy`, after `scripts/calibrate.sh`
  passed all 15 cases.
- The machine was not idle. The 1-minute load average was between 3.4 and 11
  during the run, so latencies carry that noise.

This is the global-bus "before" of topic routing.

| Scenario | Invocation | Throughput | service p50 | service p99 | Delivery | Warnings | Verdict | Gateway CPU | Loadgen CPU | Peak RSS | RSS per connection |
|----------|------------|------------|-------------|-------------|----------|----------|---------|-------------|-------------|----------|--------------------|
| fanout-200 | `--connections 200 --senders 5 --rate 50 --seconds 15` | 49 750 | 2.584 ms | 6.384 ms | 100% | 0 | pass | 25% | 40% | 31.7 MiB | 146.9 KiB |
| fanout-500 | `--connections 500 --senders 5 --rate 50 --seconds 15` | 124 750 | 3.592 ms | 12.384 ms | 100% | 0 | pass | 76% | 69% | 73.2 MiB | 143.6 KiB |
| fanout-1000 | `--connections 1000 --senders 5 --rate 50 --seconds 15` | 249 750 | 4.880 ms | 10.976 ms | 100% | 0 | pass | 192% | 146% | 141.7 MiB | 142.0 KiB |
| ingest-50 | `--connections 50 --senders 50 --rate 200 --seconds 15` | 490 000 | 1.796 ms | 5.488 ms | 100% | 0 | pass | 98% | 112% | 11.2 MiB | 167.9 KiB |
| payload-4k | `--connections 200 --senders 5 --rate 50 --payload-bytes 4096 --seconds 15` | 49 750 | 3.848 ms | 9.504 ms | 100% | 0 | pass | 36% | 52% | 37.8 MiB | 177.7 KiB |
| binary-200 | `--connections 200 --senders 5 --rate 50 --seconds 15 --binary` | 49 750 | 2.520 ms | 11.104 ms | 100% | 0 | pass | 24% | 34% | 31.2 MiB | 143.9 KiB |
| cliff-300 | `--connections 300 --senders 30 --rate 400 --seconds 15` | 3 588 000 | 1.948 ms | 3.688 ms | 100% | 0 | pass | 547% | 386% | 46.8 MiB | 149.2 KiB |

Every row is one run.

### How to read these

**The clean rows measure latency and cost, not capacity.** Throughput equals
`senders x rate x (connections - 1)` to the frame in every row, because the
gateway delivered everything that was offered.

**Fanout latency grows slowly with connection count.** Going from 200 to 1000
connections, five times the deliveries per published frame, moved service p50
from 2.584 ms to 4.880 ms and p99 from 6.384 ms to 10.976 ms.

**Padding 4 KiB onto every payload cost about 1.3 ms at p50** (2.584 ms to
3.848 ms). The payload is cloned per subscriber as a refcount, not as bytes.

**Per-connection cost is roughly 145 KiB of RSS.** Across the unpadded rows
with at least 200 connections in `bench-results/20261006-142007-all.csv`,
`rss_per_conn_kib` is 142.0-151.3 KiB, and 142.0-149.7 KiB for the baseline
rows (`beyond-3m-explore` at 149.7 is the top of that range). It does not trend
with connection count from 200 to 1000 (146.9, 143.6, 142.0 KiB). Rows with
fewer connections cost more per connection (167.9 KiB at 50, 156-158 KiB at
101) because the fixed process cost is spread over fewer sockets, and
`payload-4k` costs 177.7 KiB.

**`cliff-300` was gateway-bound in the bench run, not harness-bound.** The
gateway used 547% CPU against 386% for `loadgen`. A harness-bound row shows the
opposite. The row was clean, so it says the gateway held that load on this
machine at that moment, and the CPU columns show how much of the machine it took.

**`cliff-300` is load-sensitive.** With the previous `loadgen` it lost about 28%
of the frames in every run (70.5–72.8% delivered, 206, 481 and 868 warnings,
p99 575–686 ms). With the current one it was clean in the
bench run (3 588 000 deliveries/s, 100%, zero warnings, p99 3.688 ms). Three
repeat runs at a load average of 11–20 gave 0, 80 and 5081 warnings, delivery
99.51–100% and p99 3.85–21.2 ms. The raw runs are in
[the loadgen comparison](measurements/2026-10-06-loadgen-comparison.md). Quote
it only with the machine load next to it.

**Single runs, and the machine was not idle.** The load average is recorded
next to each run instead of requiring an idle machine. Treat differences of a few
milliseconds between rows as noise until they repeat. The p99 values here vary
by 2x between neighbouring rows of the same shape.


### Superseded: measured with the previous harness

The rows below were the published baselines until 2026-10-06. They were taken
with the `loadgen` that predates `prepara-harness-para-topicos`, on gateway
commit `334cf1d`. No gateway code has changed since. They are kept as a record
and must not be quoted as the gateway's numbers.

**Why they are unreliable under load.** The old `loadgen` created a new timer
(`sleep_until(read_end)`) inside its read loop's `select!` for every frame it
received, and the rebuilt one creates it once. Only the difference between the
two `loadgen` builds was measured. Pinning that one timer alone was not run, so
it is not established that the timer is what caused the old results. Measured
against the same gateway binary, old (`2612f71`) against new (`0bc858b`), three
runs each, at `--connections 201 --senders 50 --rate 100` (`goal-1m-mesh`), with
CPU per frame as loadgen user plus sys time over frames received. The raw
output and exact commands are in
[the loadgen comparison](measurements/2026-10-06-loadgen-comparison.md):

- **CPU per frame:** the old `loadgen` spent 68.8–69.8 s of CPU, mostly in the
  kernel, 5.30–5.37 µs per frame. The current one spent 22.0–22.5 s,
  1.69–1.73 µs per frame, about 3.1x less.
- **Latency did not change:** service p99 was 6.26–6.67 ms with the old
  `loadgen` and 6.16–7.02 ms with the new one.
- **The cliff differs between the two builds:** on `cliff-300` (300 connections, 30
  senders, 400 msg/s) the old `loadgen` delivered 70.5–72.8% in every run, with
  206, 481 and 868 warnings and a p99 of 575–686 ms. The current one delivered
  99.51–100%, but still showed 0, 80 and 5081 warnings when the machine was at a
  load average of 11–20 (see *How to read these*).

The old `cliff-300` rows are consistent with the generator falling behind on
its own reads, but the cause was never isolated: only the old-versus-new
difference is measured. Clean rows were mostly unaffected.


All runs below (superseded): Apple M4 Pro, 14 cores, `--release`, gateway and load generator
on the same machine, commit `334cf1d` plus the harness rewrite.

| Date       | Scenario      | Invocation                                                                 | Throughput   | service p50 | service p99 | response p99 | Delivery | Warnings | Gateway CPU | Loadgen CPU | Peak RSS |
|------------|---------------|----------------------------------------------------------------------------|--------------|-------------|-------------|--------------|----------|----------|-------------|-------------|----------|
| 2026-08-03 | fanout-200    | `--connections 200 --senders 5 --rate 50 --seconds 15`                     | 49,750 msg/s | 1.55 ms     | 4.94 ms     | 7.06 ms      | 100.00%  | 0        | 37%         | 39%         | 31.7 MiB |
| 2026-08-03 | fanout-500    | `--connections 500 --senders 5 --rate 50 --seconds 15`                     | 124,750 msg/s| 2.22 ms     | 7.57 ms     | 9.70 ms      | 100.00%  | 0        | 84%         | 90%         | 73.0 MiB |
| 2026-08-03 | fanout-1000   | `--connections 1000 --senders 5 --rate 50 --seconds 15`                    | 249,750 msg/s| 3.48 ms     | 8.18 ms     | 10.21 ms     | 100.00%  | 0        | 136%        | 144%        | 141.5 MiB|
| 2026-08-03 | ingest-50     | `--connections 50 --senders 50 --rate 200 --seconds 15`                    | 490,000 msg/s| 1.20 ms     | 2.58 ms     | 4.02 ms      | 100.00%  | 0        | 142%        | 292%        | 11.2 MiB |
| 2026-08-03 | payload-4k    | `--connections 200 --senders 5 --rate 50 --payload-bytes 4096 --seconds 15`| 49,750 msg/s | 1.84 ms     | 5.94 ms     | 8.14 ms      | 100.00%  | 0        | 52%         | 48%         | 37.6 MiB |
| 2026-08-03 | cliff-300     | `--connections 300 --senders 30 --rate 400 --seconds 15`                   | 2.69M msg/s  | 131.58 ms   | 579.58 ms   | 5423.10 ms   | 75.10%   | 498      | 332%        | 1068%       | 46.4 MiB |

#### How they were read at the time (superseded)

Kept verbatim. The cliff paragraphs below describe the old harness, not the gateway (see above).

**The clean rows measure latency and cost, not capacity.** In every row above
the cliff, throughput equals `senders x rate x (connections - 1)` to the frame —
49,750, 124,750, 249,750, 490,000 — because the gateway delivered everything
that was offered. They are a statement that the gateway sustained that load at
that latency with complete delivery, not that it could not do more.

**Per-connection cost is roughly 145 KiB of RSS**, stable from 200 to 1000
connections (146.4, 143.2, 141.7 KiB/conn), and each of the three tasks per
connection is cheap enough that 1000 idle-ish sockets cost 141 MiB total.

**Fanout latency grows slowly with connection count.** Going from 200 to 1000
connections — five times the deliveries per published frame — moved service p50
from 1.55 ms to 3.48 ms and p99 from 4.94 ms to 8.18 ms.

**Padding 4 KiB onto every payload cost 0.3 ms at p50** and 41 MiB more RSS at
the same message rate, which is the refcounted-clone invariant doing its job:
the payload is cloned per subscriber as a refcount, not as bytes.

**The cliff row is not a throughput figure.** Only 75.1% of the expected
deliveries arrived; the rest were dropped for consumers that fell more than
`BROADCAST_CAPACITY` behind. It is recorded because the shape of the failure is
the useful part: throughput and latency both still look like numbers, and only
delivery and the warning count reveal that a quarter of the traffic never made
it.

**The cliff row is also harness-bound.** The load generator burned 1068% CPU
against the gateway's 332% on a 14-core machine — the harness was consuming
three times the CPU of the thing it was measuring, so part of that backpressure
is self-inflicted. This is exactly why `bench.sh` records both CPU columns. Do
not quote the cliff as the gateway's ceiling; it is the ceiling of this machine
running both sides at once.

### Two latencies, and why both are reported

`loadgen` publishes on an absolute schedule: frame `n` is due at
`start + n / rate`. Each frame carries both the instant it was **due** and the
instant it **actually left**.

- **service latency** = arrival − actual send. What the gateway did, with the
  generator's own lateness removed.
- **response latency** = arrival − scheduled send. Includes any delay in
  getting the frame out, so a publisher stalled by backpressure shows up here
  instead of vanishing.

Reporting only service latency hides coordinated omission — a harness that
stalls simply sends less and reports the same good number. Reporting only
response latency charges the gateway for the harness's timer, which on this
machine wakes about 1.5-2.5 ms late under load. The gap between the two columns
is the harness's own contribution, and `slip_mean_ms` in the CSV names it
directly.

### Harness calibration

None of the numbers above were recorded until `scripts/calibrate.sh` passed.
It measures `loadgen` against `examples/refserver.rs`, a reference server whose
behaviour is fixed on purpose so the correct answer is known by arithmetic
rather than by expectation. Cases run under both protocols where they apply.
Measured on 2026-10-06, same machine and conditions as *Measured baselines*
(legacy / topics):

| Case | Predicted | Measured |
|---|---|---|
| 1 | 2,500 frames published (5 x 50 x 10 s) | 2,500 / 2,500 |
| 1 | 497,500 deliveries (2,500 x 199), 100% | 497,500 / 497,500, 100% |
| 1 | Server's own delivery counter | 597,000 / 597,000 (topics: plus 200 acknowledgements) |
| 2 | 50 ms injected delay → service p50 minus (floor + held delay) is 0 ± 1 ms, p99 within ± 5 ms, using the `hold_p50_ms` and `hold_p99_ms` the `refserver` reports | service p50 51.58 / 51.58 ms, p99 53.38 / 54.14 ms, floor 0.49-0.50 ms, server held 51.2 ms, difference -0.13 ms |
| 3 | 1-in-10 injected loss → delivery 90% | 90.0000% / 90.0000% |
| 4 | 500 ms freeze → response max ≥ 497 ms | 518.97 / 507.39 ms |
| 5 | 10 topics of 20: 5,000 published, 95,000 delivered | topics: 5,000, 95,000, misrouted 0 |
| 6 | case 5 against a server ignoring topics → misrouted 900,000 | topics: 900,000, delivered still 95,000 |
| 7 | silent 1-in-10 loss → unaccounted 10% of expected | 10.0000% / 10.0000% (legacy scope 95 sockets, topics 100) |
| 8 | the same loss with warnings → dropped 10% (legacy: 10 x 99/95 = 10.42%), unaccounted 0 | 10.4211% / 10.0000%, 0 / 0 |
| 9 | binary: same counts as case 1 | 497,500 / 497,500 |
| 10 | case 5 with 20 churning sockets → churn_failed 0, unaccounted 0 | topics: 0, 0 |
| 11 | 2 s delay, 500 ms drain limit → not drained, unaccounted null | null / null |
| 12 | 2 s delay, default limit → drained, ≥ 2 s, unaccounted 0 | 2.50 / 2.49 s, 0 / 0 |
| 13 | extra topic at 10/s → extra_expected 20,000 at 100% | topics: 20,000, 100% |
| 14 | case 5 against `--drop-subscribe-acks 10` → subscribe_failed 20 (every 10th of 200 setup acknowledgements), misrouted > 0 | topics: 20, misrouted > 0 |
| 15 | case 10 against `--drop-unsubscribe-acks 10` → churn_failed 50 (500 churn operations, half of them unsubscribes, every 10th withheld) | topics: 50 |

Case 8 is the run that set the `legacy` prediction to exactly 10 x 99/95: the
`dropped` total counts every receiver while the scope excludes the 5
publishers. The assertion was tightened to that value after this run.

The 1.58 ms above the injected 50 ms is mostly the server's own timer overshoot:
the `refserver` held each frame 51.2 ms against the 50 ms requested, about 1.2
ms. The measurement floor of the setup, one loopback hop without delay, is
0.49-0.50 ms. Case 2 therefore checks `loadgen` against the delay the server
actually held, not against 50 ms. Treat any service-latency figure below about
0.5 ms as at the noise floor.

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

**Not yet met.** Best clean baseline on the machine below is about 490 000 msg/s
(`ingest-50`) at service p99 ≈ 2.6 ms and 100% delivery. Fanout at 1000
connections sustains ~250 000 msg/s inside the latency band. Closing the gap to
1 000 000 msg/s without leaving the SLO is the work ahead.

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

The 1M goal is **hit** when at least one `goal-1m-*` row sustains its offered
load with delivery ≥ 99.9%, service p99 ≤ 20 ms, zero warnings, and the
generator holding the requested publish rate. Stretch rows use the same SLO at
higher offered load; they measure how far past 1M the process still holds, they
are not required to pass for the goal to count.

On a same-machine run the load generator can saturate first (see the cliff row
below). A `harness-bound` verdict means the measurement described the harness,
not the gateway ceiling — do not quote it as either a hit or a gateway failure.

Goal/stretch result rows belong in a separate table here once
`scripts/bench.sh --goal` has been run on a calibrated harness. Do not invent
them.

## Measured baselines

Record results here when `perf-check` produces them, with the hardware, the
build profile, and the exact `loadgen` invocation — a number without its
conditions is not a baseline. `scripts/bench.sh` prints the table in this shape
and refuses to run if `scripts/calibrate.sh` fails first. Goal and stretch
numbers come from `scripts/bench.sh --goal`.

All runs below: Apple M4 Pro, 14 cores, `--release`, gateway and load generator
on the same machine, commit `334cf1d` plus the harness rewrite.

| Date       | Scenario      | Invocation                                                                 | Throughput   | service p50 | service p99 | response p99 | Delivery | Warnings | Gateway CPU | Loadgen CPU | Peak RSS |
|------------|---------------|----------------------------------------------------------------------------|--------------|-------------|-------------|--------------|----------|----------|-------------|-------------|----------|
| 2026-08-03 | fanout-200    | `--connections 200 --senders 5 --rate 50 --seconds 15`                     | 49,750 msg/s | 1.55 ms     | 4.94 ms     | 7.06 ms      | 100.00%  | 0        | 37%         | 39%         | 31.7 MiB |
| 2026-08-03 | fanout-500    | `--connections 500 --senders 5 --rate 50 --seconds 15`                     | 124,750 msg/s| 2.22 ms     | 7.57 ms     | 9.70 ms      | 100.00%  | 0        | 84%         | 90%         | 73.0 MiB |
| 2026-08-03 | fanout-1000   | `--connections 1000 --senders 5 --rate 50 --seconds 15`                    | 249,750 msg/s| 3.48 ms     | 8.18 ms     | 10.21 ms     | 100.00%  | 0        | 136%        | 144%        | 141.5 MiB|
| 2026-08-03 | ingest-50     | `--connections 50 --senders 50 --rate 200 --seconds 15`                    | 490,000 msg/s| 1.20 ms     | 2.58 ms     | 4.02 ms      | 100.00%  | 0        | 142%        | 292%        | 11.2 MiB |
| 2026-08-03 | payload-4k    | `--connections 200 --senders 5 --rate 50 --payload-bytes 4096 --seconds 15`| 49,750 msg/s | 1.84 ms     | 5.94 ms     | 8.14 ms      | 100.00%  | 0        | 52%         | 48%         | 37.6 MiB |
| 2026-08-03 | cliff-300     | `--connections 300 --senders 30 --rate 400 --seconds 15`                   | 2.69M msg/s  | 131.58 ms   | 579.58 ms   | 5423.10 ms   | 75.10%   | 498      | 332%        | 1068%       | 46.4 MiB |

### How to read these

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
rather than by expectation:

| Predicted | Measured |
|---|---|
| 2,500 frames published (5 x 50 x 10 s) | 2,500 |
| 497,500 deliveries (2,500 x 199) | 497,500 |
| 49,750 msg/s throughput | 49,750.0 |
| 100% delivery | 100.0000% |
| Server's own delivery counter, 597,000 | 597,000 |
| 50 ms injected delay → service p50 50 ms | 51.58 ms |
| 1-in-10 injected loss → delivery 90% | 90.0000% |
| 500 ms freeze → response max ≥ 497 ms | 499.09 ms |

The 1.58 ms above the injected 50 ms is the measurement floor of this setup:
one loopback hop plus the millisecond granularity of the timer on each side.
Treat any service-latency figure below about 1.5 ms as at the noise floor.

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

**Met on the current gateway, with margin.** Measured on 2026-10-06 with the
rebuilt, calibrated harness (`--protocol legacy`, machine and conditions under
*Measured baselines*):

- **All three goal scenarios pass:** `goal-1m-fanout`, `goal-1m-ingest` and
  `goal-1m-mesh` sustain 1 000 000 deliveries/s at 100% delivery, zero
  warnings, and a service p99 of 6.4–8.9 ms.
- **All six stretch scenarios pass too,** up to `beyond-3m-explore` at
  3 000 000 deliveries/s (service p99 4.2 ms).
- **`cliff-300` holds** 3 587 927 deliveries/s at 99.998% with 7 warnings.

This sweep did not find the gateway's ceiling on this machine.

The earlier "not yet met" came from two things:

- **Never measured.** The goal scenarios had never been run on a calibrated
  harness.
- **A harness artifact.** The previous `loadgen` was the bottleneck under heavy
  load (see *Superseded: measured with the previous harness*).

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

Results from `scripts/bench.sh --all --protocol legacy` on 2026-10-06, same
machine and conditions as *Measured baselines*. `goal-1m-topics` needs
`--protocol topics` and was skipped:

| Scenario | Offered | Throughput | service p99 | Delivery | Warnings | Rate held | Verdict |
|----------|---------|------------|-------------|----------|----------|-----------|---------|
| goal-1m-fanout | 1 000 000 | 1 000 000 | 8.864 ms | 100.0000% | 0 | yes | pass |
| goal-1m-ingest | 1 000 000 | 1 000 000 | 6.384 ms | 100.0000% | 0 | yes | pass |
| goal-1m-mesh | 1 000 000 | 1 000 000 | 7.408 ms | 100.0000% | 0 | yes | pass |
| beyond-1.5m-fanout | 1 500 000 | 1 500 000 | 5.808 ms | 100.0000% | 0 | yes | pass |
| beyond-1.5m-ingest | 1 500 000 | 1 500 000 | 4.752 ms | 100.0000% | 0 | yes | pass |
| beyond-1.5m-mesh | 1 500 000 | 1 500 000 | 8.176 ms | 100.0000% | 0 | yes | pass |
| beyond-2m-fanout | 2 000 000 | 2 000 000 | 13.984 ms | 100.0000% | 0 | yes | pass |
| beyond-2m-mesh | 2 000 000 | 2 000 000 | 4.208 ms | 100.0000% | 0 | yes | pass |
| beyond-3m-explore | 3 000 000 | 3 000 000 | 4.240 ms | 100.0000% | 0 | yes | pass |

These are one run each. A pass at 3M on this machine says the process held that
load for 13 measured seconds, not that 3M is a ceiling or a guarantee.

## Measured baselines

Record results here when `perf-check` produces them, with the hardware, the
build profile, and the exact `loadgen` invocation — a number without its
conditions is not a baseline. `scripts/bench.sh` prints the table in this shape
and refuses to run if `scripts/calibrate.sh` fails first. Goal and stretch
numbers come from `scripts/bench.sh --goal`.

**Conditions:**
- Apple M4 Pro, 14 cores, `--release`, gateway and load generator on the same
  machine.
- Gateway code identical to `main` at `dbe04ae`, measured from commit `26404a6`
  (the rebuilt harness).
- Driven by `scripts/bench.sh --all --protocol legacy`, after
  `scripts/calibrate.sh` passed every case.
- The machine was not idle. macOS's `BTLEServer` held one core at about 100%
  throughout, and the 1-minute load average was 5.5 at the start.

This is the global-bus "before" of topic routing.

| Date | Scenario | Invocation | Throughput | service p50 | service p99 | response p99 | Delivery | Warnings | Gateway CPU | Loadgen CPU | Peak RSS | RSS/conn |
|------|----------|------------|------------|-------------|-------------|--------------|----------|----------|-------------|-------------|----------|----------|
| 2026-10-06 | fanout-200 | `--connections 200 --senders 5 --rate 50 --seconds 15` | 49 750 | 2.39 ms | 9.06 ms | 11.74 ms | 100.00% | 0 | 18% | 26% | 31.5 MiB | 145.4 KiB |
| 2026-10-06 | fanout-500 | `--connections 500 --senders 5 --rate 50 --seconds 15` | 124 750 | 4.21 ms | 13.54 ms | 15.78 ms | 100.00% | 0 | 81% | 83% | 73.1 MiB | 143.4 KiB |
| 2026-10-06 | fanout-1000 | `--connections 1000 --senders 5 --rate 50 --seconds 15` | 249 750 | 4.72 ms | 11.87 ms | 14.30 ms | 100.00% | 0 | 186% | 153% | 141.8 MiB | 142.1 KiB |
| 2026-10-06 | ingest-50 | `--connections 50 --senders 50 --rate 200 --seconds 15` | 490 000 | 1.68 ms | 6.83 ms | 10.66 ms | 100.00% | 0 | 138% | 120% | 10.9 MiB | 161.8 KiB |
| 2026-10-06 | payload-4k | `--connections 200 --senders 5 --rate 50 --payload-bytes 4096 --seconds 15` | 49 750 | 4.06 ms | 10.72 ms | 13.28 ms | 100.00% | 0 | 31% | 37% | 38.5 MiB | 181.8 KiB |
| 2026-10-06 | binary-200 | `--connections 200 --senders 5 --rate 50 --seconds 15 --binary` | 49 750 | 2.52 ms | 10.40 ms | 14.05 ms | 100.00% | 0 | 18% | 29% | 31.7 MiB | 146.9 KiB |
| 2026-10-06 | cliff-300 | `--connections 300 --senders 30 --rate 400 --seconds 15` | 3 587 927 | 2.23 ms | 5.68 ms | 8.11 ms | 99.998% | 7 | 496% | 557% | 46.6 MiB | 148.8 KiB |

Every row drained in about 0.5 s and reports `unaccounted` 0. `ingest-50` has
no non-publishing socket, so its `unaccounted` is `null` under `legacy`.

### How to read these

**The clean rows measure latency and cost, not capacity.** Throughput equals
`senders x rate x (connections - 1)` to the frame in every row but the cliff,
because the gateway delivered everything that was offered.

**Per-connection cost is about 145 KiB of RSS**, stable from 200 to 1000
connections (145.4, 143.4, 142.1 KiB/conn).

**Fanout latency grows slowly with connection count.** Going from 200 to 1000
connections — five times the deliveries per published frame — moved service p50
from 2.39 ms to 4.72 ms and p99 from 9.06 ms to 11.87 ms.

**Padding 4 KiB onto every payload cost about 1.7 ms at p50** and 36 KiB more
RSS per connection at the same message rate. The payload is cloned per
subscriber as a refcount, not as bytes.

**`cliff-300` is barely a cliff.** At 3.59 million deliveries/s:
- 7 warnings reported 962 dropped frames, out of 46.6 million expected in the
  measured window;
- `unaccounted` is 0, so every lost frame was reported;
- the generator used 557% CPU against the gateway's 496%, so this run measured
  the gateway, not the harness.

**Single runs, and the machine was not idle.** Treat differences of a few
milliseconds between rows as noise until they repeat.

**Clean-row latencies are about 1 ms higher than the superseded rows**
(fanout-200 service p50 2.39 ms against 1.55 ms). The instrument does not
explain it: on the same day, at `goal-1m-mesh`, the rebuilt `loadgen` measured a
*lower* service p99 than the old one (4.3–4.4 ms against 5.4–5.5 ms). Read it as
the machine's state, a core held by `BTLEServer`, until a run on an idle machine
says otherwise.

### Superseded: measured with the previous harness

The rows below were the published baselines until 2026-10-06. They were taken
with the `loadgen` that predates `prepara-harness-para-topicos`, on gateway
commit `334cf1d`. No gateway code has changed since. They are kept as a record
and must not be quoted as the gateway's numbers.

**Why they are wrong under load.** The old `loadgen` created a new timer
(`sleep_until(read_end)`) inside its read loop's `select!` for every frame it
received. Measured on 2026-10-06 against the same gateway, at
`--connections 201 --senders 50 --rate 100` (15 million frames received):

- **CPU per frame:** the old `loadgen` spent 62 s of CPU, 51 s of it in the
  kernel, about 4.1 µs per frame. The rebuilt one spent 17 s, about 1.1 µs per
  frame.
- **Cause isolated:** changing only that loop in the old `loadgen` to create the
  timer once brought it down to 13–15 s.
- **The cliff disappears:** with that one change, the old `loadgen` measured
  `cliff-300` at 100% delivery, zero warnings and a 3.5 ms service p99. Unchanged
  it measured 76.5% delivery, 750 warnings and a 5.2 s response p99, matching the
  row below.

The cliff and the "harness-bound" reading below were the generator falling
behind on its own reads, which made the gateway drop frames for it. Clean rows
were mostly unaffected.

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
| 2 | 50 ms injected delay → service p50 50 ms (±2), p99 ±5 | 51.58 / 51.58 ms, p99 53.38 / 54.14 ms |
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

Case 8 is the run that set the `legacy` prediction to exactly 10 x 99/95: the
`dropped` total counts every receiver while the scope excludes the 5
publishers. The assertion was tightened to that value after this run.

The 1.58 ms above the injected 50 ms is the measurement floor of this setup:
one loopback hop plus the millisecond granularity of the timer on each side.
Treat any service-latency figure below about 1.5 ms as at the noise floor.

# Architecture

## Connection lifecycle

`handle_socket` in `src/ws.rs` owns one connection from upgrade to close:

1. A `Uuid` is generated for the connection.
2. It sends `{"type":"welcome","id":"<uuid>"}`.
3. The connection counter is incremented.
4. The connection gets its own inbox (`INBOX_CAPACITY` frames) and a control
   channel (`CONTROL_QUEUE_CAPACITY` frames), and its `Subscriptions` value,
   which starts empty.
5. Two tasks are spawned: reader and writer.

There is no handshake. A connection receives nothing until it subscribes to a
topic. When the connection ends, dropping `Subscriptions` removes it from every
topic it held.

## The two tasks

The socket is split, and each direction gets its own task:

| Task       | Job                                                                                                                                                                                                                           |
|------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **reader** | Reads frames from the socket. Text is parsed as a control frame (`subscribe`, `unsubscribe`, `publish`) and `data` is validated as JSON and embedded verbatim. Binary has its topic header parsed. A publish fans out into the inbox of every other subscriber of the topic. |
| **writer** | Serves the control channel first, then the connection's inbox. It awaits one frame, drains up to `WRITE_BATCH_SIZE` more without waiting, and issues one `flush` per batch. Turns `Lagged(n)` into a `warning` frame in position. |

Only the writer touches the sink, so no locking is needed around it.

`tokio::select!` waits for the first of the two to finish, then aborts the
other. Any termination reason, such as client close or socket error, tears the
whole connection down through one path.

## Message flow

```
client A ──► reader(A) ──► registry[topic] ──► inbox (B) ──► writer (B) ──► client B
                                          └──► inbox (C) ──► writer (C) ──► client C
```

The envelope is built **once**, by A's reader. What goes into each inbox is the
finished `Message`, so B and C do no serialization work at all. The publisher
skips its own inbox, and the fanout runs inside the publisher's task, with no
task or wakeup between the registry and the inbox. Replies to `subscribe` and
`unsubscribe`, and `error` frames, go through the connection's own control
channel and are never dropped.

## Topic lifecycle

A topic has no existence of its own. It is the set of connections subscribed to a
key, and the registry entry exists exactly while that set is not empty. There is
no create, delete, declare or configure operation.

| Stage | What happens |
|---|---|
| **Does not exist** | No registry entry. A `publish` to the key is discarded, nothing is created or retained, and the publisher is not told. |
| **Created** | The first `subscribe` to a valid key (1-255 bytes of UTF-8) inserts the entry with one subscriber. The connection receives `subscribed` only after the insert. A `publish` never creates a topic. |
| **Active** | Further `subscribe`s join it and each `unsubscribe` leaves it, both by copy-on-write. Every publish fans out to the members present when it reads the entry. |
| **Emptied** | The `unsubscribe` or disconnect that removes the last member also removes the entry, in the same atomic compute. A concurrent `subscribe` either lands before, so the entry stays, or after, so it creates a new one. It is never lost. |
| **Re-created** | A later `subscribe` to the same key starts a new topic. Nothing from before survives: no frames, no member list, no settings. A frame published while the topic was empty is never delivered. |

A topic has no owner, no namespace, no access control, no retention and no
publish feedback. The reasoning, and how other systems differ, is in the
`roteia-por-topico` change design.

## Hot-path invariants

Each of these is a property of the current code. Breaking one is a regression
even if tests still pass.

- **The payload is never deserialized.** The gateway parses its own control frame
  (`type`, `topic`) and reads `data` as `&RawValue`, which validates that the
  bytes are JSON without building a `Value` tree, then splices them into the
  envelope verbatim. No `serde_json::Value`, no typed struct for `data`, no
  field access inside it.
- **One serialization per message, not per receiver.** The envelope is built in
  the publishing connection's reader task, and a binary header is built once.
- **Cloning a message is a refcount bump, not a copy.** An inbox holds an `axum`
  `Message`, whose `Text` and `Binary` variants are backed by
  `Utf8Bytes`/`Bytes`. Fanout clones once per subscriber; that clone must stay
  O(1).
- **Gateway code uses no locks.** Short internal locks inside tokio channels are
  allowed, and are never held across `.await`. The shared state is an
  `AtomicUsize` connection counter and the lock-free topic registry, which
  publishers read without locking.
- **Never `.await` while holding a lock.** Fanout is synchronous, and the locks
  inside tokio's `broadcast::send` are released inside the call.
- **Bounded everywhere.** Each connection's inbox holds `INBOX_CAPACITY` frames
  and its control channel `CONTROL_QUEUE_CAPACITY`, a connection holds at most 64
  subscriptions, topics are at most 255 bytes, and frames are capped at
  `MAX_MESSAGE_SIZE`. A slow client degrades into `warning` frames; it never
  grows memory without bound and never blocks a publisher.
- **No per-message logging in the fanout path.** Publishing and forwarding a
  message never formats a `Uuid` or any other per-frame data into a log line —
  that cost scales with fanout. Connect, disconnect, and the accumulated lag
  count are logged twice per connection lifetime, not per message.

## Backpressure

Each connection has its own inbox, a `tokio::sync::broadcast` channel of
`INBOX_CAPACITY` frames with the writer as its only receiver. A publisher never
waits: when an inbox is full, the new frame overwrites the oldest one. A slow
consumer that is more than `INBOX_CAPACITY` frames behind gets `Lagged(n)` from
`recv`, the gateway logs it and sends `{"type":"warning","dropped":n}`, and
delivery continues from the oldest frame still held.

This is a deliberate trade: the gateway drops messages for the slow client
rather than slowing down every other client or buffering without limit. Clients
that cannot tolerate loss must detect `warning` and recover at their own layer.

The inbox is shared by all of a connection's topics, so a busy topic can push out
the frames of a quiet one, and `warning` reports only a total for the connection.
A topic that must be protected goes on its own connection.

## Scalability limits

| Limit                | Value                                              | Consequence                                                                                                                                                |
|----------------------|----------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Routing              | Exact topic key, up to 64 topics per connection    | A publish reaches only the subscribers of its topic. There are no wildcards.                                                                               |
| Loss isolation       | One inbox per connection, shared by its topics     | Loss is not isolated per topic. A busy topic can overwrite a quiet topic's pending frames on the same connection.                                          |
| Fanout cost          | O(topic size) per publish                          | A publish does one `inbox.send` per subscriber, in the publisher's task. One task fans out one publish, and a topic of 10 000 subscribers is not measured. |
| Membership churn     | O(topic size) per subscribe or unsubscribe         | Each change copies the topic's subscriber list. The churn rows measure the cost.                                                                           |
| Inbox depth          | `INBOX_CAPACITY = 256`                             | A consumer more than 256 frames behind starts losing frames.                                                                                               |
| Inbox memory         | About 20 KB per connection, allocated up front     | The estimate comes from the change design. The measured RSS per connection is under *Measured baselines*.                                                  |
| Control queue        | `CONTROL_QUEUE_CAPACITY = 16`                      | Replies are never dropped. A connection that floods control frames without reading stalls only its own reader.                                             |
| Message size         | `MAX_MESSAGE_SIZE = 64 KiB`                        | Larger frames are rejected at the WebSocket layer and the connection is closed. A delivered binary frame is at most 16 bytes larger.                       |
| Per-connection cost  | 2 tasks, 1 control channel, 1 inbox                | Task overhead dominates at high connection counts.                                                                                                         |
| Process model        | Single process, no backplane                       | The ceiling is one machine. Two instances share nothing; clients on different instances cannot reach each other.                                           |

### What would have to change

- **Subscription authorization** is deliberately absent (`docs/decisions.md`,
  entry 18). Any connection that is admitted can read any topic it can guess.
  It would need a credential issued per connection, and `subscribe` is the
  single place where a check would go.
- **Horizontal scale** needs a backplane (Redis pub/sub, NATS, a gossip mesh) so
  instances relay to each other. That decision is open and is built on the
  registry: the first local subscriber of a topic would subscribe the instance
  to it.
- **A very large topic** would justify splitting one publish's fanout across
  tasks. Do not do this without a measurement showing the single-task fanout
  matters.
- **Per-topic loss isolation** would need one inbox per connection-topic pair, at
  a memory cost of up to 64 inboxes per connection. Do not do this without the
  overlap rows showing the loss in practice.

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
`senders × rate × (connections − 1)` when every connection shares one topic
(`topics = 1`).
Frames on the extra quiet topic (`--extra-topic-rate`) are reported apart and
are not deliveries in this sense.

The goal is **sustained offered load within SLO**, not the peak number printed
while consumers are lagging. A cliff row that shows multi-million msg/s with
delivery well below 99.9% does not count. The latency bar is **service p99**
(arrival minus the instant the frame left the publisher), not response p99.

### Status

**The goal is hit, with one scenario that misses the latency bar.** Measured
with topic routing at commit `24f4ca3` plus uncommitted changes, with the
calibrated harness from `ca04d29` (conditions under *Measured baselines*):

- **1M goal:** `goal-1m-fanout`, `goal-1m-ingest` and `goal-1m-mesh` sustain
  1 000 000 deliveries/s at 100% delivery, zero warnings and a service p99 of
  4.5-6.0 ms.
- **`goal-1m-topics` misses the latency bar.** It delivers 1 000 000/s at 100%,
  but service p99 is 27.8 ms (p50 18.6 ms) against the 20 ms bar. Verdict
  `latency`.
- **Stretch: 6 of 6 pass,** up to `beyond-3m-explore` at 3 000 000 deliveries/s
  and 100% delivery (service p99 4.2 ms).
- **`topics-5k` has the same shape:** p50 46 ms and p99 71 ms at 5 000
  connections and 500 topics, verdict `latency`.
- **`churn-500` p99 was 50 ms in the sweep,** and 16-17 ms in two later runs.
  Gateway CPU was about 8-10 times the reference server's under the same churn.
- **RSS per connection rose** from about 142-151 KiB to about 197-208 KiB on
  the rows with 200 or more connections that share the same shape. Each
  connection now owns an inbox.

Every row is one run, and the machine's 1-minute load average went from 4.2 to
13.7 during the sweep. The global-bus numbers this replaces are kept under
*Global-bus architecture*.

**Known open items, both unconfirmed:**

- `goal-1m-topics` may be bound by the harness: `loadgen` used 311% CPU against
  192% for the gateway. The comparison against the reference server that would
  confirm it was not obtained.
- The churn CPU may come from copy-on-write of `Arc<[Subscriber]>` on every
  subscribe and unsubscribe. No measurement isolates it yet.

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
| `goal-1m-topics` | 100 topics of 21, publishers in the topic they publish to | `--connections 2100 --topics 100 --senders 100 --rate 500` | 1 000 000 msg/s |

The 1M goal is **hit** when at least one `goal-1m-*` row sustains its offered
load with delivery ≥ 99.9%, service p99 ≤ 20 ms, zero warnings, and the
generator holding the requested publish rate. Stretch rows use the same SLO at
higher offered load; they measure how far past 1M the process still holds, they
are not required to pass for the goal to count.

On a same-machine run the load generator can saturate first (see the cliff row
below). A `harness-bound` verdict means the measurement described the harness,
not the gateway ceiling — do not quote it as either a hit or a gateway failure.

Results from `scripts/bench.sh --all`, same machine and conditions as
*Measured baselines*:

| Scenario | Offered | Throughput | service p99 | Delivery | Warnings | Verdict | Gateway CPU | Loadgen CPU | Peak RSS | RSS per connection |
|----------|---------|------------|-------------|----------|----------|---------|-------------|-------------|----------|--------------------|
| goal-1m-fanout | 1 000 000 | 1 000 000 | 4.720 ms | 100% | 0 | pass | 307% | 272% | 101.5 MiB | 200.9 KiB |
| goal-1m-ingest | 1 000 000 | 1 000 000 | 4.528 ms | 100% | 0 | pass | 255% | 174% | 22.6 MiB | 196.7 KiB |
| goal-1m-mesh | 1 000 000 | 1 000 000 | 5.968 ms | 100% | 0 | pass | 244% | 157% | 41.7 MiB | 196.6 KiB |
| goal-1m-topics | 1 000 000 | 1 000 000 | 27.840 ms | 100% | 0 | latency | 192% | 311% | 365.7 MiB | 176.8 KiB |
| beyond-1.5m-fanout | 1 500 000 | 1 500 000 | 4.912 ms | 100% | 0 | pass | 380% | 300% | 99.7 MiB | 197.2 KiB |
| beyond-1.5m-ingest | 1 500 000 | 1 500 000 | 3.064 ms | 100% | 0 | pass | 274% | 194% | 23.0 MiB | 200.7 KiB |
| beyond-1.5m-mesh | 1 500 000 | 1 500 000 | 4.944 ms | 100% | 0 | pass | 237% | 134% | 52.2 MiB | 200.3 KiB |
| beyond-2m-fanout | 2 000 000 | 2 000 000 | 13.984 ms | 100% | 0 | pass | 727% | 352% | 204.1 MiB | 205.6 KiB |
| beyond-2m-mesh | 2 000 000 | 2 000 000 | 4.088 ms | 100% | 0 | pass | 308% | 197% | 42.3 MiB | 199.2 KiB |
| beyond-3m-explore | 3 000 000 | 3 000 000 | 4.176 ms | 100% | 0 | pass | 454% | 292% | 61.9 MiB | 199.7 KiB |

The 1M goal is hit in 3 of 4 goal scenarios (`goal-1m-topics` misses the latency
bar) and 6 of 6 stretch scenarios pass. These are one run each. A pass at 3M on
this machine says the process held that load for the measured window, not that 3M
is a ceiling or a guarantee. No row dropped a frame, so this sweep did not find
the cliff that `beyond-3m-explore` showed on the global bus.

## Measured baselines

Record results here when `perf-check` produces them, with the hardware, the
build profile, and the exact `loadgen` invocation — a number without its
conditions is not a baseline. `scripts/bench.sh` prints the table in this shape
and refuses to run if `scripts/calibrate.sh` fails first. Goal and stretch
numbers come from `scripts/bench.sh --goal`.

**Conditions:**
- Apple M4 Pro, 14 cores, `--release`, gateway and load generator on the same
  machine.
- Gateway at commit `24f4ca3` plus uncommitted changes, harness from the
  `ca04d29` tree, driven by `bash scripts/bench.sh --all` after
  `scripts/calibrate.sh` passed. The raw rows are in
  `bench-results/20261006-150929-all.csv`.
- The machine was not idle. The 1-minute load average rose from 4.2 to 13.7
  during the run, so latencies carry that noise.

| Scenario | Invocation | Throughput | service p50 | service p99 | Delivery | Warnings | Verdict | Gateway CPU | Loadgen CPU | Peak RSS | RSS per connection |
|----------|------------|------------|-------------|-------------|----------|----------|---------|-------------|-------------|----------|--------------------|
| fanout-200 | `--connections 200 --senders 5 --rate 50 --seconds 15` | 49 750 | 2.408 ms | 6.544 ms | 100% | 0 | pass | 21% | 31% | 39.1 MiB | 183.8 KiB |
| fanout-500 | `--connections 500 --senders 5 --rate 50 --seconds 15` | 124 750 | 4.656 ms | 11.552 ms | 100% | 0 | pass | 85% | 109% | 95.6 MiB | 189.4 KiB |
| fanout-1000 | `--connections 1000 --senders 5 --rate 50 --seconds 15` | 249 750 | 4.752 ms | 12.192 ms | 100% | 0 | pass | 154% | 154% | 206.2 MiB | 207.9 KiB |
| ingest-50 | `--connections 50 --senders 50 --rate 200 --seconds 15` | 490 000 | 1.980 ms | 4.816 ms | 100% | 0 | pass | 102% | 113% | 13.2 MiB | 204.8 KiB |
| payload-4k | `--connections 200 --senders 5 --rate 50 --payload-bytes 4096 --seconds 15` | 49 750 | 4.400 ms | 9.952 ms | 100% | 0 | pass | 41% | 62% | 47.4 MiB | 226.3 KiB |
| cliff-300 | `--connections 300 --senders 30 --rate 400 --seconds 15` | 3 588 000 | 2.488 ms | 4.400 ms | 100% | 0 | pass | 637% | 366% | 61.6 MiB | 199.3 KiB |
| topics-1k | `--connections 1000 --topics 100 --senders 100 --rate 100 --seconds 15` | 90 000 | 2.744 ms | 5.104 ms | 100% | 0 | pass | 266% | 232% | 173.0 MiB | 174.0 KiB |
| topics-5k | `--connections 5000 --topics 500 --senders 500 --rate 100 --seconds 15` | 450 000 | 46.208 ms | 71.424 ms | 100% | 0 | latency | 185% | 254% | 834.5 MiB | 170.3 KiB |
| binary-200 | `--connections 200 --senders 5 --rate 50 --seconds 15 --binary` | 49 750 | 2.392 ms | 7.248 ms | 100% | 0 | pass | 24% | 34% | 39.1 MiB | 183.8 KiB |
| overlap-200 | `--connections 200 --senders 5 --rate 50 --seconds 15 --extra-topic-rate 5` | 49 750 | 2.680 ms | 7.792 ms | 100% | 0 | pass | 29% | 43% | 40.2 MiB | 188.5 KiB |
| overlap-cliff | `--connections 300 --senders 30 --rate 400 --seconds 15 --extra-topic-rate 5` | 3 588 000 | 2.568 ms | 4.720 ms | 100% | 0 | pass | 645% | 356% | 62.4 MiB | 201.4 KiB |
| churn-500 | `--connections 500 --senders 5 --rate 50 --seconds 15 --churn 200 --churn-rate 50` | 124 750 | 5.296 ms | 50.304 ms | 100% | 0 | latency | 1193% | 78% | 178.1 MiB | 255.9 KiB |

Every row is one run.

### How to read these

**The clean rows measure latency and cost, not capacity.** Throughput equals
`senders x rate x (connections / topics - 1)` to the frame in every row, because
the gateway delivered everything that was offered.

**Fanout latency grows with connection count.** Going from 200 to 1000
connections, five times the deliveries per published frame, moved service p50
from 2.408 ms to 4.752 ms and p99 from 6.544 ms to 12.192 ms.

**Padding 4 KiB onto every payload cost about 2.0 ms at p50** (2.408 ms in
`fanout-200` to 4.400 ms in `payload-4k`). The payload is cloned per subscriber as
a refcount, not as bytes.

**Per-connection cost is roughly 200 KiB of RSS.** Across the rows with at least
200 connections and no padding, `rss_per_conn_kib` is 183.8-207.9 KiB, and
196.6-205.6 KiB for the goal and stretch rows other than `goal-1m-topics`
(176.8 KiB). The global-bus rows were 142-151 KiB.
The `topics-1k` and `topics-5k` rows cost 173.0 and 170.3 KiB, and `churn-500`
255.9 KiB. The increase is larger than the 20 KB per-connection inbox estimated
in the change design, and no measurement here splits it by cause.

**No row shows a cliff.** `cliff-300` held 3 588 000 deliveries/s at 100% with
zero warnings, and every stretch row up to `beyond-3m-explore` held at 100%. In
`cliff-300` the gateway used 637% CPU against 366% for `loadgen`, so it was
gateway-bound and not harness-bound. The global-bus run lost frames at 3M. This
sweep did not, but it is one run at a load average that climbed to 13.7, and
`cliff-300` was load-sensitive on the global bus (see *Global-bus architecture*).

**Topic scenarios show where topics cost more.**
- `topics-1k` (100 topics of 10) is clean at p99 5.1 ms.
- `topics-5k` (500 topics of 10, 500 publishers) has p50 46.2 ms and p99 71.4 ms
  at 5 000 connections, verdict `latency`. `loadgen` used 254% CPU against 185%
  for the gateway.
- `churn-500` has p99 50.3 ms, with 200 churning sockets, and the gateway used
  1193% CPU against 78% for `loadgen`. Two later runs gave p99 of 16-17 ms.
- `overlap-200` and `overlap-cliff` delivered 100% of the extra quiet topic's
  13 000 and 19 500 frames, with zero warnings, so no loss to report from one
  topic pushing out another in those runs.

**Single runs, and the machine was not idle.** The load average is recorded
next to each run instead of requiring an idle machine. Treat differences of a few
milliseconds between rows as noise until they repeat.

## Global-bus architecture

The numbers below were measured before topic routing, when every frame went to
every other connection over one global bus. They are the "before" of
`roteia-por-topico` and are not the current gateway. They were recorded at commit
`0bc858b` by `prepara-harness-para-topicos`.

### Status at `0bc858b`

**Met on the global bus.** Measured at `0bc858b` with the calibrated
harness (machine and conditions below):

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

Goal and stretch results at `0bc858b`, same machine and conditions as below.
`goal-1m-topics` did not exist on the global bus and was skipped:

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

### Measured baselines at `0bc858b`

**Conditions:**
- Apple M4 Pro, 14 cores, `--release`, gateway and load generator on the same
  machine.
- Measured at commit `0bc858b`, driven by
  `bash scripts/bench.sh --all`, after `scripts/calibrate.sh`
  passed all 15 cases.
- The machine was not idle. The 1-minute load average was between 3.4 and 11
  during the run, so latencies carry that noise.

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

### How to read the `0bc858b` rows

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

## Two latencies, and why both are reported

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

## Harness calibration

None of the numbers above were recorded until `scripts/calibrate.sh` passed.
It measures `loadgen` against `examples/refserver.rs`, a reference server whose
behaviour is fixed on purpose so the correct answer is known by arithmetic
rather than by expectation. Measured on 2026-10-06 on the same machine as
*Measured baselines*:

| Case | Predicted | Measured |
|---|---|---|
| 1 | 2,500 frames published (5 x 50 x 10 s) | 2,500 / 2,500 |
| 1 | 497,500 deliveries (2,500 x 199), 100% | 497,500 / 497,500, 100% |
| 1 | Server's own delivery counter | 597,000 / 597,000 (plus 200 acknowledgements) |
| 2 | 50 ms injected delay → service p50 minus (floor + held delay) is 0 ± 1 ms, p99 within ± 5 ms, using the `hold_p50_ms` and `hold_p99_ms` the `refserver` reports | service p50 51.58 ms, p99 54.14 ms, floor 0.49-0.50 ms, server held 51.2 ms, difference -0.13 ms |
| 3 | 1-in-10 injected loss → delivery 90% | 90.0000% |
| 4 | 500 ms freeze → response max ≥ 497 ms | 507.39 ms |
| 5 | 10 topics of 20: 5,000 published, 95,000 delivered | 5,000, 95,000, misrouted 0 |
| 6 | case 5 against a server ignoring topics → misrouted 900,000 | 900,000, delivered still 95,000 |
| 7 | silent 1-in-10 loss → unaccounted 10% of expected | 10.0000% (scope 100 sockets) |
| 8 | the same loss with warnings → dropped 10%, unaccounted 0 | 10.0000%, 0 |
| 9 | binary: same counts as case 1 | 497,500 / 497,500 |
| 10 | case 5 with 20 churning sockets → churn_failed 0, unaccounted 0 | 0, 0 |
| 11 | 2 s delay, 500 ms drain limit → not drained, unaccounted null | null |
| 12 | 2 s delay, default limit → drained, ≥ 2 s, unaccounted 0 | 2.49 s, 0 |
| 13 | extra topic at 10/s → extra_expected 20,000 at 100% | 20,000, 100% |
| 14 | case 5 against `--drop-subscribe-acks 10` → subscribe_failed 20 (every 10th of 200 setup acknowledgements), misrouted > 0 | 20, misrouted > 0 |
| 15 | case 10 against `--drop-unsubscribe-acks 10` → churn_failed 50 (500 churn operations, half of them unsubscribes, every 10th withheld) | 50 |

The 1.58 ms above the injected 50 ms is mostly the server's own timer overshoot:
the `refserver` held each frame 51.2 ms against the 50 ms requested, about 1.2
ms. The measurement floor of the setup, one loopback hop without delay, is
0.49-0.50 ms. Case 2 therefore checks `loadgen` against the delay the server
actually held, not against 50 ms. Treat any service-latency figure below about
0.5 ms as at the noise floor.

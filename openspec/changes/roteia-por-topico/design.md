# Design

## Context

Today a frame takes this path:

```
reader(A) ──► broadcast bus (256) ──► bridge(B) ──► mpsc(B, 32) ──► writer(B) ──► B
                                 └──► bridge(C) ──► mpsc(C, 32) ──► writer(C) ──► C
```

- **Reader** (`src/ws.rs:95`): builds the envelope once and sends it to the bus.
- **Bridge** (`src/ws.rs:77`): filters out the connection's own frames and turns `Lagged(n)` into a `warning`.
- **Writer** (`src/ws.rs:57`): drains in batches of up to 32 and flushes once per batch.
- **Shared state** (`src/state.rs:15`): one `broadcast::Sender` and one `AtomicUsize`.

Topics have to answer four questions together:

1. How does a client say what it wants to receive?
2. How does a publish find who is listening?
3. How does the frame reach each listener without the publisher waiting?
4. How does all of that get cleaned up when a connection ends?

Answering one in isolation constrains the others, so the design below follows a frame end to end.

## Goals / Non-Goals

**Goals:**

- Fanout bounded by topic size instead of connection count.
- Keep every hot-path invariant in `docs/architecture.md`: payload never deserialized, one serialization per publish, refcounted clones, no locks on the publish path, bounded memory, no per-message logging.
- A registry shape that presence, direct delivery by id and a backplane can extend later without being redesigned.

**Non-Goals:**

- Presence, direct delivery by connection id, wildcard or predicate subscriptions, a backplane, authorization of subscriptions. Each is a later change. The section *Extension points* says where each would plug in.
- Topic-level backpressure attribution. A `warning` stays per connection.

## Decisions

### 1. The registry stores where to deliver, not just who is subscribed

The registry maps topic → list of subscriber handles. A handle is everything needed to deliver to one connection:

```
Subscriber { id: Uuid, queue: mpsc::Sender<Outbound>, dropped: Arc<AtomicU64> }
```

There is one `queue` and one `dropped` counter per connection, shared by every topic that connection subscribes to.

The publisher looks up the topic and delivers straight into each subscriber's queue:

```
reader(A) ── publish k ──► registry[k] ──► [B, C] ── try_send ──► writer(B) ──► B
                                                                  writer(C) ──► C
```

Why this shape:

- The same handle is what direct delivery by id needs (`id → Subscriber`).
- The topic's list is what presence needs.
- A backplane can be modelled as one more subscriber per topic.

**Alternatives rejected:**

- **One `broadcast` channel per topic.** It keeps the bus model, but each connection would need one receiver per topic: either a task per subscription or a `StreamMap` in the bridge. Presence and direct delivery would each need a separate structure next to it. Every topic would also allocate a 256-slot ring, even for a single subscriber.
- **Subscriptions held in Redis.** A subscription lives exactly as long as a socket on this process, so it has nothing to persist. Each instance would still need the in-memory topic → local queue map to deliver, and Redis cannot push into a Tokio queue. Redis would add a network round trip per publish or subscribe, an external failure mode, and orphaned entries when an instance dies (TTL, heartbeats). It also contradicts "the gateway holds no durable state" in `docs/scope.md`. Redis or NATS belongs to the backplane change, where it carries only cross-instance traffic (see *Extension points*).

### 2. Lock-free reads, copy-on-write membership

- The map is a lock-free concurrent hash map: `papaya`. The value for a topic is an immutable `Arc<[Subscriber]>`.
- **Publish:** pins the map, reads the entry and iterates the slice. It takes no lock, holds nothing across `.await`, and calls only synchronous `try_send`.
- **Subscribe / unsubscribe:** an atomic compute on the entry builds a new slice with the handle added or removed. Unsubscribing the last handle removes the entry in the same compute, so an empty topic never stays in memory and a concurrent subscribe cannot be lost.
- Publishing to a missing topic reads nothing and creates nothing. Publishes cannot grow the registry.

Membership changes cost O(subscribers of that topic), because the slice is copied. Publishes vastly outnumber membership changes, so the trade favours the publish path. A topic with tens of thousands of subscribers joining at once would make the joins quadratic. That is recorded as a limit in `docs/architecture.md`, not solved here.

**Alternatives rejected:**

- `std::sync::RwLock<HashMap>` and `DashMap` take a lock, or a shard lock, on every publish. That breaks "no locks on the hot path" in `CLAUDE.md`.
- An `arc-swap` of the whole map copies every topic on every membership change.

The exact `papaya` API used (`compute` with remove-on-empty) is confirmed during implementation. If it cannot express remove-on-empty atomically, implementation stops and the fallback comes back here as a revision, not a silent substitution.

### 3. The publisher fans out and never waits

- The reader builds the outbound frame once:
  - text: the envelope serialized to a `Utf8Bytes`;
  - binary: header and payload copied once into a `Bytes`.
- For each subscriber other than itself, it reserves a slot with `try_reserve` and sends a refcount clone.
- The cost of a publish is O(subscribers of the topic) inside the publisher's task. Today that cost is spread over N bridge tasks. Removing the bridge also removes one task and one wakeup per delivery.

Back-of-envelope cost: a topic with 10 000 subscribers is about 10 000 `try_reserve` + `send` in one task. At roughly 50–100 ns each, the last subscriber's frame leaves 0.5–1 ms after the first. These are expected costs, not measurements. The bench run in the tasks measures them. If one hot topic makes this matter, a later change can split fanout across tasks.

### 4. Backpressure: drop for the slow receiver, warn exactly at the gap

- Each connection's outgoing queue holds `Outbound { dropped_before: u64, frame: Message }`, with capacity `SUBSCRIBER_QUEUE_CAPACITY = 256`. That matches today's bus depth, so slow-consumer tolerance stays about the same.
- If `try_reserve` fails because the queue is full, the publisher increments that subscriber's `dropped` counter and moves on. The publisher is never held back.
- If `try_reserve` succeeds, the publisher does `n = dropped.swap(0)` and sends `Outbound { dropped_before: n, frame }`.
- The writer emits `{"type":"warning","dropped":n}` immediately before the frame whenever `n > 0`.

A warning therefore lands exactly where the gap is, and the exact-count property in `delivery-backpressure` still holds. The swap happens only after a slot is reserved, so a counted drop is never lost to a failed send.

The connection's own control replies (`subscribed`, `unsubscribed`, `error`) go through the same queue with `send().await` from its reader. A connection that does not read its socket can only stall its own reader. The `warning` frame shape does not change.

### 5. Inbound protocol: the gateway parses its own frame, never `data`

Text frames are parsed as:

```
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientFrame<'a> {
    Subscribe   { topic: Cow<'a, str> },
    Unsubscribe { topic: Cow<'a, str> },
    Publish     { topic: Cow<'a, str>, #[serde(borrow)] data: &'a RawValue },
}
```

- This is the gateway's own control vocabulary, the same way `ServerMessage` already is. `data` is validated as JSON and embedded verbatim, exactly as today.
- The envelope is `{"type":"message","topic":…,"from":…,"data":…}`. `topic` is re-serialized with standard JSON escaping. `data` is spliced.
- Unknown extra fields are ignored, for forward compatibility. Missing fields, a wrong `type`, or a non-JSON frame produce `INVALID_FRAME`.

Topic rules, identical for text and binary:

- 1 to 255 bytes of UTF-8 after JSON unescaping, compared byte for byte.
- 255 is the most a one-byte length can express in the binary header.

Subscription rules:

- `subscribe` and `unsubscribe` are idempotent and always answered.
- Subscribing beyond `MAX_SUBSCRIPTIONS_PER_CONNECTION = 64` distinct topics returns `error` and changes nothing.

### 6. Binary header

- **Inbound:** `[len: u8][topic: len bytes, UTF-8][payload: rest]`.
- **Delivered:** `[len: u8][topic][sender uuid: 16 bytes, RFC 4122 byte order][payload]`.

A `len` of 0, a frame shorter than `1 + len`, or a topic that is not UTF-8 produces `INVALID_BINARY` as a text `error` frame, and the connection stays open. The payload may be empty. The 64 KiB frame limit applies to the inbound frame including its header. A delivered frame is at most 16 bytes larger.

### 7. Ordering and visibility guarantees

These are the guarantees a client can rely on, and each is a spec scenario.

- **Subscription visibility.** The registry insert happens before `subscribed` is queued. Any publish a client makes after another connection has received `subscribed` for that topic reaches that connection. A frame for the topic may arrive *before* `subscribed` if a publish raced the subscription. It carries `topic`, so the client can tell.
- **Unsubscribe in flight.** After `unsubscribed`, frames for that topic that were already being fanned out may still arrive. Clients that care filter on `topic`. Closing this window would need per-frame subscription checks in the writer, which costs more than it buys.
- **Per-sender order.** A sender's frames reach a given receiver in send order, across topics, while the receiver is not dropping. One reader task publishes them in order into one FIFO queue.
- **No duplicates.** A publish targets exactly one topic, so a receiver gets at most one copy of each publish.

### 8. Lifecycle and cleanup

- Each connection keeps its subscribed topics in a `Subscriptions` value owned by the reader task, with `Drop` removing the connection from every one of them.
- When the connection ends, the `select!` aborts the reader, and dropping it unsubscribes everything. No path leaves a dead handle in the registry.
- A handle whose queue is closed (connection mid-teardown) makes `try_reserve` fail with `Closed`, which is ignored.

### 9. Hot-path invariants checked

| Invariant (`docs/architecture.md`) | Effect of this change |
|---|---|
| Payload never deserialized | Kept. The control frame is parsed, and `data` stays `&RawValue`. The invariant text is reworded to say so. |
| One serialization per message | Kept. Text envelope and binary header are each built once per publish. |
| Clone is a refcount bump | Kept. `Outbound` holds a `Message` backed by `Utf8Bytes`/`Bytes`. |
| No locks on the hot path | Kept, but no longer trivial. Publish reads a lock-free map. The text changes from "one `AtomicUsize`" to "the `AtomicUsize` and the lock-free topic registry". |
| Never `.await` holding a lock | Kept. There is no lock, and fanout is synchronous. |
| Bounded everywhere | Kept. Queue of 256 per connection, at most 64 subscriptions per connection, topics of 255 bytes at most. The registry is bounded by `connections × 64` entries. |
| No per-message logging | Kept. Drops are counted, not logged. Connect and disconnect log lines still report the lag total. |

### 10. Harness: rebuilt for topics, not patched

The harness was designed around the global bus, and that assumption runs through every layer of it, not just one formula:

| Where | Global-bus assumption |
|---|---|
| `examples/loadgen.rs` — `expected` | Every published frame reaches every other connection: `sent × (established − 1)`. |
| `examples/loadgen.rs` — `connect_one` | A connection is ready once it receives `welcome`. There is no subscribe step to wait for. |
| `examples/loadgen.rs` — `write_loop` | It publishes the bare stamp `{"t":…,"s":…}` as the whole frame. |
| `examples/loadgen.rs` — `read_loop` | It only knows `message`, `warning` and `error`, ignores binary frames, counts `warning` frames without adding up `dropped`, and has no notion of a frame arriving on the wrong topic. |
| `examples/refserver.rs` | The reference routes through one broadcast bus with a source filter. Its injected loss is silent, with no `warning`. |
| `scripts/calibrate.sh` | Every predicted value uses `CONNS − 1`. |
| `scripts/bench.sh` | Scenarios are `connections senders rate payload seconds`. The verdict's `offered()` is `senders × rate × (conns − 1)`. Nothing in the CSV can show a routing error. |

After this change a run can be wrong in ways the old meters cannot see: a frame delivered to a non-subscriber, a frame lost without a `warning`, a subscriber missing from a topic. So the harness gains meters for those, and each meter gets a calibration case with a known answer.

**`loadgen`**

- **Topology.**
  - `--topics T` (default 1) puts connection `i` on topic `t-(i mod T)`. `connections` must be divisible by `T`.
  - Sender `j` publishes to the topic of the connection it is, so it is a member of the topic it publishes to.
  - Before the clock starts, every connection sends `subscribe` and waits for `subscribed`. A missing acknowledgement counts as `subscribe_failed`, and the connection is left out of the expected arithmetic.
- **Expected deliveries.** For each topic, the frames sent to it × (its acknowledged members − 1). With every connection acknowledged, this reduces to `senders × rate × measured_seconds × (connections / T − 1)`, which is today's formula at `T = 1`.
- **Publish.** `write_loop` sends `{"type":"publish","topic":…,"data":{stamp}}`. With `--binary`, it sends `[len][topic][stamp: two u64 little-endian][padding]` instead.
- **Read.**
  - `read_loop` decodes the envelope's `topic` and `data`, or the binary header, and drops neither path.
  - Text is parsed with the same borrowed decoding as today, so the timing path is unchanged.
- **New meters, all in `--json`:**
  - `misrouted`: frames whose `topic` is not the receiver's. It must be 0.
  - `dropped`: the sum of `dropped` over every `warning`.
  - `unaccounted`: over the whole run (warmup included, since a `warning` carries no timestamp), expected − received − dropped. Thanks to the exact-count guarantee in `delivery-backpressure`, a non-zero value means frames vanished without a warning. It must be 0.
  - `subscribe_ack_p50_ms` and `subscribe_ack_p99_ms`: the setup cost of the registry's copy-on-write joins, measured outside the clock.
- **Churn.** `--churn N` opens `N` extra connections that, during the measured window, alternately subscribe to and unsubscribe from topic `t-0` at one operation per second each. Their receptions are excluded from expected deliveries. The steady members' delivery and latency show what membership churn costs a publish (`design.md` decision 2, *Risks*).
- **Unit tests.** `cargo test --examples` covers the expected-delivery arithmetic for uneven acknowledgements, alongside the existing histogram tests.

**`refserver`**

- **Routing.** It speaks the new protocol, including acknowledgements and the binary header. Internally it keeps its single broadcast bus, and each connection filters by its own topic set.
  - This deliberately stays different from the gateway's registry. A reference that copies the code under test cannot catch that code's bugs.
  - It is slow at large `T`, which does not matter because it only runs calibration loads.
- **New injected faults, each with a known answer:**
  - `--warn-drops`: the existing 1-in-K loss is reported with `warning` frames.
  - `--ignore-topics`: delivers every publish to every connection, as the old bus did.
- **Existing faults.** Delay, silent loss and freeze keep their current behaviour.

**`calibrate.sh`**

The existing cases 1 to 4 run unchanged at `T = 1`, so their predicted answers (2,500 published, 497,500 delivered, 90% under loss, ≥ 497 ms freeze) must still hold. That proves the rebuild did not move the instrument. New cases:

| Case | Setup | Predicted |
|---|---|---|
| 5 topics | 200 connections, `--topics 10`, 10 senders at 50/s, 10 s measured | 5,000 published, 95,000 delivered (5,000 × 19), 100%, `misrouted` 0, `unaccounted` 0 |
| 6 misrouting meter | case 5 against `--ignore-topics` | every publish reaches all 199 other connections: 95,000 correctly routed and `misrouted` = 900,000 (5,000 × 180), which shows the meter detects leaks |
| 7 silent loss | case 3 (1-in-10 loss, no warnings) | delivery 90%, `dropped` 0, `unaccounted` ≈ 10% of expected |
| 8 reported loss | case 3 with `--warn-drops` | delivery 90%, `dropped` ≈ 10% of expected, `unaccounted` 0 |
| 9 binary | case 1 with `--binary` | same counts as case 1 |
| 10 churn exclusion | case 5 with `--churn 20` | steady members' delivery 100% and `unaccounted` 0, so churners are excluded correctly |

**`bench.sh`**

- **Scenario tuple:** `name connections topics senders rate payload seconds [flags]`. The existing scenarios keep their names and offered load at `topics = 1`.
- **New scenarios:**

  | Scenario | Invocation | Offered | What it shows |
  |---|---|---|---|
  | `topics-1k` | `--connections 1000 --topics 100 --senders 100 --rate 100` | 90 000 deliveries/s | Topic routing at moderate size. |
  | `topics-5k` | `--connections 5000 --topics 500 --senders 500 --rate 100` | 450 000 deliveries/s | Same topic size, five times the connections: latency should stay flat if fanout is bounded by topic size. |
  | `binary-200` | `fanout-200` with `--binary` | same as `fanout-200` | Cost of building the binary header once per publish. |
  | `churn-500` | `fanout-500` with `--churn 100` | same as `fanout-500` | Membership churn under load. |
  | `goal-1m-topics` (goal set) | `--connections 2100 --topics 100 --senders 100 --rate 500` | exactly 1 000 000 deliveries/s across 100 topics of 21 | The point of the change: the goal delivery rate with fanout bounded by topic size. |

- **CSV:** gains `topics`, `binary`, `churn`, `subscribe_failed`, `subscribe_ack_p99_ms`, `misrouted`, `dropped` and `unaccounted`. The markdown table prints the full invocation.
- **Verdict.** `offered()` becomes `senders × rate × (conns / topics − 1)`. A new verdict, `incorrect`, applies when `misrouted > 0`, `unaccounted > 0` or `subscribe_failed > 0`, and it overrides every other verdict: a fast run that routes wrongly is not a performance result.
- **RSS per connection** divides by every open connection, churners included.

**Sequencing.** The harness rebuild and its calibration only need `refserver`, so they land and pass `calibrate.sh` before the gateway code is measured. The new harness cannot drive the old gateway, because the protocols differ. So there is no same-harness before/after comparison of the gateway, which is one more reason the old rows stay historical only (decision 11).

### 11. Existing benchmarks become invalid

Every number in the *Measured baselines* table and the *Status* paragraph of `docs/architecture.md` was measured on the global-bus architecture, at commit `334cf1d` plus the harness rewrite. This change replaces the part that dominates those numbers:

- Fanout moves from one bridge task per receiver, fed by a 256-slot broadcast ring, to a single publisher task that does `try_reserve` into each subscriber's queue.
- The task count per connection drops from three to two, which changes the per-connection RSS figure.
- Lag is triggered by a full per-connection queue instead of the bus position, so the cliff behaves differently.
- Every frame now carries a control wrapper and a `topic`, so the bytes per delivery change too.

Same invocation does not mean same system. After the implementation:

- None of the existing rows is a baseline for the gateway, and none may be quoted as its current throughput, latency, RSS per connection or cliff. That includes the "~490 000 msg/s best clean baseline" and the 145 KiB/connection figure.
- They are not deleted. They move to a subsection marked as the global-bus architecture, with their commit, as a record of where the project started.
- New baselines come from a full `scripts/bench.sh --all` run on the new code, after `scripts/calibrate.sh` passes, recorded with hardware, profile, commit and invocation as the table already requires.
- An old row next to a new one is a before/after illustration of this change. It is not evidence that a later change regressed or improved anything; later comparisons use only post-change baselines.

### Extension points (not implemented)

- **Presence:** the topic's `Arc<[Subscriber]>` is the member list. Join and leave events hook the same compute that changes membership.
- **Direct delivery by id:** a second lock-free map `Uuid → Subscriber`, filled at connect and emptied by the same `Drop`, delivering through the same `try_reserve` path.
- **Backplane:** the first local subscriber of a topic makes the instance subscribe to it on the backplane, and removing the empty entry unsubscribes it. A local publish fans out locally and is sent once to the backplane. Remote frames enter through the same fanout function.
- **Authorization:** `subscribe` is the single choke point where a connect-time credential can be checked.

## Decision entries for `docs/decisions.md`

Appended by the implementation PR. Entries 3, 4 and 8 are not edited. Each new entry names the one it supersedes.

---

## 11. Delivery is routed by topic; the global bus is gone

**Context.** One global bus made fanout O(N²) and made targeted delivery impossible (entry 8). Topics need the client to say what it wants, which entry 4 had ruled out by having no client-to-server protocol.

**Decision.** Clients send `subscribe`, `unsubscribe` and `publish` control frames. A publish reaches only the other subscribers of its topic, as `{"type":"message","topic":…,"from":…,"data":…}`. The gateway parses its own control frame and still never deserializes `data`. A topic is an opaque key of 1–255 bytes of UTF-8, compared byte for byte. The global bus is removed: "everyone" is a topic everyone subscribes to. Supersedes entries 4 and 8.

**Consequence.** Breaking change for every client. Fanout is bounded by topic size. The gateway now has a client protocol to version and test. Anything a client wants beyond routing still goes inside `data`.

---

## 12. The registry holds subscriber queues, read without locks

**Context.** Routing needs topic → subscribers, which is the first shared state beyond the connection counter (entry 6 anticipated a registry). A lock on the publish path would serialize every publisher.

**Decision.** A lock-free concurrent map (`papaya`) from topic to an immutable `Arc<[Subscriber]>`, where a subscriber is the connection's id, outgoing queue and drop counter. Membership changes are copy-on-write and remove the entry when it empties. The publisher fans out with `try_reserve` into each queue, and the bridge task is removed. Rejected alternatives: a broadcast channel per topic, `RwLock`/`DashMap`, and subscriptions held in Redis (see the change's design).

**Consequence.** Publish takes no lock and cannot grow memory. Membership changes cost O(topic size). Fanout work moves into the publisher's task. The same handle serves later presence, direct delivery and backplane work.

---

## 13. Backpressure is per receiver queue

**Context.** Without the bus there is no `Lagged(n)`. The receiver's queue becomes the only buffer.

**Decision.** Each connection's queue holds 256 frames. A full queue drops the frame for that receiver and counts it. The next delivered frame carries the count, and the writer emits `{"type":"warning","dropped":n}` right before it. Amends the mechanism of entry 7. The policy stays the same.

**Consequence.** The `warning` frame and the exact-count guarantee are unchanged for clients. A slow receiver still never slows a publisher.

---

## 14. Binary frames carry a topic header

**Context.** Binary frames had no envelope (entry 3), so they could neither name a topic nor tell a receiver who sent them.

**Decision.** Inbound binary is `[len: u8][topic][payload]`. Delivered binary is `[len: u8][topic][sender uuid: 16 bytes][payload]`, built once per publish. The payload stays opaque. Supersedes entry 3.

**Consequence.** Binary clients must frame the header. In exchange they route like text clients and learn the sender. A topic is limited to 255 bytes for both frame types.

---

## Risks / Trade-offs

- **Breaking every client.** This is accepted and recorded in entry 11. The README protocol section is rewritten as the reference.
- **New dependency on the hot path.** `papaya` correctness under concurrent compute and remove is load-bearing. Mitigation: integration tests that subscribe and unsubscribe concurrently with publishing, and a bench run.
- **Fanout concentrated in one task.** A single very large topic adds latency proportional to its size for its last subscriber (decision 3). The bench row `goal-1m-fanout` at `T = 1` measures the worst case directly.
- **Unsubscribe window.** Frames in flight can arrive after `unsubscribed`. This is documented, and the spec scenario allows it.
- **Membership churn on huge topics** is quadratic. It is recorded as a scalability limit and not addressed.
- **Stale performance claims.** Until the new baselines are recorded, `docs/architecture.md` has no valid gateway numbers. Any claim made from the old rows in that window is wrong. Mitigation: the documentation and bench tasks land in the same PR as the code (decision 11).
- **Harness drift.** If `loadgen` and `refserver` disagree on the protocol, calibration fails. `scripts/calibrate.sh` must pass before any number is published.
- **A harness that measures the wrong thing.** A rebuilt harness could report clean numbers while routing is broken. Mitigation: the `misrouted`, `unaccounted` and `subscribe_failed` meters, each proven by a calibration case with a known answer, and the `incorrect` verdict that overrides every other verdict (decision 10).

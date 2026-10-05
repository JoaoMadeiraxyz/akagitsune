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

### 10. Harness: provided by `prepara-harness-para-topicos`

The harness rebuild is its own change, `prepara-harness-para-topicos`, and it lands first. It brings:
- expected deliveries per topic;
- the `misrouted`, `dropped`, `unaccounted` and subscribe-acknowledgement meters, each with a calibration case;
- the `--binary` and `--churn` dimensions;
- the topic scenarios, including `goal-1m-topics`;
- the `incorrect` verdict.

During the transition it keeps `--protocol legacy|topics`, with `legacy` as the default, so `main` stays measurable.

This change only switches the harness over:

- `loadgen`, `refserver` and `bench.sh` default to `topics`.
- The `legacy` protocol and its `null` rules are deleted, together with the `legacy` runs in `calibrate.sh`.
- `scripts/calibrate.sh` must pass before any gateway number is recorded.

The first run of the real gateway under `topics` is the first time the harness's `topics` side meets something other than `refserver`. A disagreement there is investigated, not tolerated.

### 11. Existing benchmarks become invalid

When this change starts, *Measured baselines* and *Status* in `docs/architecture.md` hold the global-bus numbers that `prepara-harness-para-topicos` re-measured with the new harness under `--protocol legacy`. This change replaces the part that dominates those numbers:

- Fanout moves from one bridge task per receiver, fed by a 256-slot broadcast ring, to a single publisher task that does `try_reserve` into each subscriber's queue.
- The task count per connection drops from three to two, which changes the per-connection RSS figure.
- Lag is triggered by a full per-connection queue instead of the bus position, so the cliff behaves differently.
- Every frame now carries a control wrapper and a `topic`, so the bytes per delivery change too.

Same invocation does not mean same system. After the implementation:

- None of the global-bus rows is a baseline for the gateway. None may be quoted as its current throughput, latency, RSS per connection or cliff.
- They are not deleted. They move to a subsection marked as the global-bus architecture, with their commit, as a record of where the project started.
- New baselines come from a full `scripts/bench.sh --all` run on the new code, after `scripts/calibrate.sh` passes, recorded with hardware, profile, commit and invocation as the table already requires.
- Both sets come from the same calibrated instrument, so for the `topics = 1` scenarios this change's PR can show a real before/after comparison. That comparison is the evidence for this change only. Later changes compare against post-change baselines.

### Extension points (not implemented)

- **Presence:** the topic's `Arc<[Subscriber]>` is the member list. Join and leave events hook the same compute that changes membership.
- **Direct delivery by id:** a second lock-free map `Uuid → Subscriber`, filled at connect and emptied by the same `Drop`, delivering through the same `try_reserve` path.
- **Backplane:** the first local subscriber of a topic makes the instance subscribe to it on the backplane, and removing the empty entry unsubscribes it. A local publish fans out locally and is sent once to the backplane. Remote frames enter through the same fanout function.
- **Authorization:** `subscribe` is the single choke point where a connect-time credential can be checked.

## Decision entries for `docs/decisions.md`

Appended by the implementation PR. Entries 3, 4 and 8 are not edited. Each new entry names the one it supersedes.

---

## 12. Delivery is routed by topic; the global bus is gone

**Context.** One global bus made fanout O(N²) and made targeted delivery impossible (entry 8). Topics need the client to say what it wants, which entry 4 had ruled out by having no client-to-server protocol.

**Decision.** Clients send `subscribe`, `unsubscribe` and `publish` control frames. A publish reaches only the other subscribers of its topic, as `{"type":"message","topic":…,"from":…,"data":…}`. The gateway parses its own control frame and still never deserializes `data`. A topic is an opaque key of 1–255 bytes of UTF-8, compared byte for byte. The global bus is removed: "everyone" is a topic everyone subscribes to. Supersedes entries 4 and 8.

**Consequence.** Breaking change for every client. Fanout is bounded by topic size. The gateway now has a client protocol to version and test. Anything a client wants beyond routing still goes inside `data`.

---

## 13. The registry holds subscriber queues, read without locks

**Context.** Routing needs topic → subscribers, which is the first shared state beyond the connection counter (entry 6 anticipated a registry). A lock on the publish path would serialize every publisher.

**Decision.** A lock-free concurrent map (`papaya`) from topic to an immutable `Arc<[Subscriber]>`, where a subscriber is the connection's id, outgoing queue and drop counter. Membership changes are copy-on-write and remove the entry when it empties. The publisher fans out with `try_reserve` into each queue, and the bridge task is removed. Rejected alternatives: a broadcast channel per topic, `RwLock`/`DashMap`, and subscriptions held in Redis (see the change's design).

**Consequence.** Publish takes no lock and cannot grow memory. Membership changes cost O(topic size). Fanout work moves into the publisher's task. The same handle serves later presence, direct delivery and backplane work.

---

## 14. Backpressure is per receiver queue

**Context.** Without the bus there is no `Lagged(n)`. The receiver's queue becomes the only buffer.

**Decision.** Each connection's queue holds 256 frames. A full queue drops the frame for that receiver and counts it. The next delivered frame carries the count, and the writer emits `{"type":"warning","dropped":n}` right before it. Amends the mechanism of entry 7. The policy stays the same.

**Consequence.** The `warning` frame and the exact-count guarantee are unchanged for clients. A slow receiver still never slows a publisher.

---

## 15. Binary frames carry a topic header

**Context.** Binary frames had no envelope (entry 3), so they could neither name a topic nor tell a receiver who sent them.

**Decision.** Inbound binary is `[len: u8][topic][payload]`. Delivered binary is `[len: u8][topic][sender uuid: 16 bytes][payload]`, built once per publish. The payload stays opaque. Supersedes entry 3.

**Consequence.** Binary clients must frame the header. In exchange they route like text clients and learn the sender. A topic is limited to 255 bytes for both frame types.

---

## Risks / Trade-offs

- **Breaking every client.** This is accepted and recorded in entry 12. The README protocol section is rewritten as the reference.
- **New dependency on the hot path.** `papaya` correctness under concurrent compute and remove is load-bearing. Mitigation: integration tests that subscribe and unsubscribe concurrently with publishing, and a bench run.
- **Fanout concentrated in one task.** A single very large topic adds latency proportional to its size for its last subscriber (decision 3). The bench row `goal-1m-fanout` at `T = 1` measures the worst case directly.
- **Unsubscribe window.** Frames in flight can arrive after `unsubscribed`. This is documented, and the spec scenario allows it.
- **Membership churn on huge topics** is quadratic. It is recorded as a scalability limit and not addressed.
- **Stale performance claims.** Until the new baselines are recorded, `docs/architecture.md` has no valid gateway numbers. Any claim made from the old rows in that window is wrong. Mitigation: the documentation and bench tasks land in the same PR as the code (decision 11).
- **Harness drift.** The `topics` side of the harness was calibrated only against `refserver`. If it and the gateway disagree on the protocol, calibration still passes but the gateway run fails or misroutes. Mitigation: the `incorrect` verdict, and investigating the first gateway run instead of tolerating it (decision 10).

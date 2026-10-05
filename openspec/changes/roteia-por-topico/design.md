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
Subscriber { id: Uuid, inbox: broadcast::Sender<Message> }
```

There is one inbox per connection: a 256-slot `broadcast` channel whose only receiver is that connection's writer. It is shared by every topic the connection subscribes to (decision 4 explains why this is a `broadcast` and not an `mpsc`).

The publisher looks up the topic and delivers straight into each subscriber's inbox:

```
reader(A) ── publish k ──► registry[k] ──► [B, C] ── inbox.send ──► writer(B) ──► B
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
- **Publish:** pins the map, reads the entry and iterates the slice. It takes no lock in the gateway's own code, holds nothing across `.await`, and calls only the synchronous `broadcast::Sender::send` on each inbox. That call takes tokio's internal locks (decision 4).
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
- For each subscriber other than itself, it sends a refcount clone with `inbox.send(frame)`. The call never waits: if the inbox is full, it overwrites the oldest pending frame.
- The cost of a publish is O(subscribers of the topic) inside the publisher's task. Today that cost is spread over N bridge tasks. Removing the bridge also removes one task and one wakeup per delivery.

Back-of-envelope cost: a topic with 10 000 subscribers is about 10 000 `inbox.send` calls in one task. Each one takes that inbox's tail mutex and slot lock, which are uncontended unless several publishers hit the same receiver at once. At roughly 100–200 ns each, the last subscriber's frame leaves 1–2 ms after the first. These are expected costs, not measurements. The bench run in the tasks measures them. If one hot topic makes this matter, a later change can split fanout across tasks.

### 4. Backpressure: drop the oldest for the slow receiver, warn exactly at the gap

**Policy: drop the oldest, exactly as today.** A receiver that falls behind loses the frames it had not read yet, and then gets the most recent ones. Today's global bus does this: a `Lagged(n)` receiver skips to the newest 256 frames. Keeping it matters for two reasons:

- **Realtime semantics.** For game state, telemetry or dashboards, the newest frame is the valuable one, and drop-oldest is the norm for realtime fanout.
- **Existing guarantees.** `slow_consumer_receives_a_warning_frame`, `delivery_resumes_after_a_warning`, `dropped_count_matches_the_frames_skipped` and `slow_receiver_does_not_hold_back_others` (`tests/gateway.rs:174-320`) depend on it. In each of them, B reads nothing while A sends 128 000 frames, then expects a `warning` and, in two of them, the final `{"marker":"end"}`.

**Mechanism: a per-connection `broadcast` inbox with a single receiver.**

- `INBOX_CAPACITY = 256`, the same depth as today's bus.
- Publishers call `inbox.send(frame)`. It never waits, and when the ring is full it overwrites the oldest slot.
- The writer is the only receiver. When it reads after falling behind, `recv` returns `Lagged(n)` with `n` exactly equal to the number of frames overwritten for this connection. The writer emits `{"type":"warning","dropped":n}` and continues with the oldest frame still in the ring, as the bridge does today.
- The publisher never sends to its own inbox, so `n` never counts a connection's own frames. Today's global bus does count them for a lagging publisher.
- **Batching:** the writer awaits one `recv`, then drains up to a batch with `try_recv`, which also reports `Lagged` in order, and flushes once per batch (`docs/decisions.md` entry 9).

**Why not the `mpsc` with `try_reserve` from the first draft.** An `mpsc` can only refuse new frames, so it drops the newest. The warning then has nothing to ride on once the stream stops:

- When B stops reading, its queue fills with the first 256 frames, and everything after is discarded, `{"marker":"end"}` included.
- The warning was to be emitted before the next delivered frame, but none comes, so B never learns it lost anything. All four tests above fail.
- The benchmark harness's `unaccounted` meter would also be non-zero on every overloaded run, wrongly marking it `incorrect` (`prepara-harness-para-topicos`).

**Locks, stated plainly.** `broadcast::Sender::send` takes a mutex on the tail and a lock on the slot (tokio 1.53.1, `sync/broadcast.rs:662` and `:677`). Today's global bus already takes the same locks, with every publisher contending for one tail. With one inbox per connection, contention is limited to publishers writing to the same receiver at the same instant. The locks are held for a few instructions and never across `.await`. The gateway's own code adds no lock, and the hot-path invariant text says so explicitly instead of claiming the path is lock-free (decision 9).

**Alternative kept for later:** a lock-free drop-oldest ring, for example crossbeam's `ArrayQueue::force_push`, with a notify. It removes tokio's locks, but placing the warning exactly at the gap under concurrent pushes and pops is the hard part that `broadcast` already solves. It is worth doing only if a measurement shows inbox contention mattering.

**Control replies are never dropped.** `subscribed`, `unsubscribed` and `error` frames go through a separate small `mpsc` from the connection's own reader, with `send().await`. If they shared the inbox, a lagging connection could lose its own acknowledgement. The writer serves the control channel first (`biased` select). The visibility guarantee only needs the registry insert to happen before `subscribed` is sent (decision 7), so the two channels need no ordering between them.

**Memory.** A 256-slot ring is allocated per connection up front, at an estimated ~20 KB. That replaces today's bus receiver and 32-slot `mpsc`. The figure is an estimate. The per-connection RSS from the benchmark run is the measurement.

### 5. Inbound protocol: the gateway parses its own frame, never `data`

Text frames are parsed into a plain struct, and the dispatch on `type` happens after parsing:

```
struct ClientFrame<'a> {
    #[serde(rename = "type")] kind: &'a str,
    #[serde(borrow)] topic: Cow<'a, str>,
    #[serde(borrow, default, deserialize_with = "present")] data: Option<&'a RawValue>,
}
```

Each attribute is there for a measured reason. Each was checked against `serde_json` 1.0.151, the version in `Cargo.toml`:

- **Not an internally tagged enum.** `#[serde(tag = "type")]` with a `&RawValue` field fails with `invalid type: newtype struct, expected any valid JSON value`. Worse, that enum shape first buffers the whole frame into serde's intermediate content tree, which would deserialize `data`. That violates "Never deserialize the payload". A plain struct reads `data` straight from the input as a raw slice.
- **`#[serde(borrow)]` on `topic`.** Without it the `Cow` is always owned, so every publish would allocate the topic. With it, an unescaped topic borrows from the frame. `Option<Cow<str>>` does not borrow even with the attribute, so `topic` is not optional: a frame without it fails to parse and gets `INVALID_FRAME`.
- **`deserialize_with = "present"` on `data`.** A plain `Option<&RawValue>` turns `"data":null` into `None`, so a `publish` of `null` would be rejected as missing `data`. The `message-relay` spec requires `null` to be accepted. `present` deserializes a `&RawValue` and wraps it in `Some`, so `null` becomes `Some("null")` and only an absent field is `None`. Checked results:
  - `"data":null` → `Some("null")`;
  - no `data` → `None`;
  - `"data":  42  ` → `Some("42")`;
  - object bytes kept verbatim.
- **Duplicate fields** (`"topic":"a","topic":"b"`) fail with `duplicate field`, and get `INVALID_FRAME`.

Dispatch after parsing:
- `kind` `subscribe` or `unsubscribe` uses `topic` and ignores `data`;
- `publish` requires `data` to be `Some`;
- any other `kind` is `INVALID_FRAME`.

Other rules:
- This is the gateway's own control vocabulary, the same way `ServerMessage` already is. `data` is validated as JSON and embedded verbatim, exactly as today.
- The envelope is `{"type":"message","topic":…,"from":…,"data":…}`. `topic` is re-serialized with standard JSON escaping. `data` is spliced.
- Unknown extra fields are ignored, for forward compatibility.

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

- **Subscription visibility.** The registry insert happens before `subscribed` is sent to the connection's control channel. Any publish a client makes after another connection has received `subscribed` for that topic reaches that connection. A frame for the topic may arrive *before* `subscribed` if a publish raced the subscription. It carries `topic`, so the client can tell.
- **Unsubscribe in flight.** After `unsubscribed`, frames for that topic that were already being fanned out may still arrive. Clients that care filter on `topic`. Closing this window would need per-frame subscription checks in the writer, which costs more than it buys.
- **Per-sender order.** A sender's frames reach a given receiver in send order, across topics, while the receiver is not dropping. One reader task publishes them in order, and each receiver has one FIFO inbox.
- **No duplicates.** A publish targets exactly one topic, so a receiver gets at most one copy of each publish.

### 8. Lifecycle and cleanup

- Each connection keeps its subscribed topics in a `Subscriptions` value owned by the reader task, with `Drop` removing the connection from every one of them.
- When the connection ends, the `select!` aborts the reader, and dropping it unsubscribes everything. No path leaves a dead handle in the registry.
- A handle whose inbox has no receiver left (connection mid-teardown) makes `send` return an error, which is ignored.

### 9. Hot-path invariants checked

| Invariant (`docs/architecture.md`) | Effect of this change |
|---|---|
| Payload never deserialized | Kept. The control frame is parsed, and `data` stays `&RawValue`. The invariant text is reworded to say so. |
| One serialization per message | Kept. Text envelope and binary header are each built once per publish. |
| Clone is a refcount bump | Kept. The inbox holds a `Message` backed by `Utf8Bytes`/`Bytes`. |
| No locks on the hot path | Kept for the gateway's own code. Publish reads a lock-free map. Each `inbox.send` takes tokio's internal tail and slot locks (decision 4), as the global bus does today, but contended only per receiver instead of globally. The invariant text changes to: "no lock in gateway code; shared state is the `AtomicUsize` and the lock-free topic registry; channel-internal locks are short and never held across `.await`". |
| Never `.await` holding a lock | Kept. Fanout is synchronous, and tokio's internal locks are released inside `send`. |
| Bounded everywhere | Kept. Inbox of 256 per connection, a small control channel per connection, at most 64 subscriptions per connection, topics of 255 bytes at most. The registry is bounded by `connections × 64` entries. |
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

- Fanout moves from one bridge task per receiver, fed by one shared 256-slot broadcast ring, to a single publisher task that sends into each subscriber's own 256-slot inbox.
- The task count per connection drops from three to two, which changes the per-connection RSS figure.
- Lag is measured per inbox instead of against one shared ring, so a busy topic no longer pushes receivers of other topics into lag, and the cliff behaves differently.
- Every frame now carries a control wrapper and a `topic`, so the bytes per delivery change too.

Same invocation does not mean same system. After the implementation:

- None of the global-bus rows is a baseline for the gateway. None may be quoted as its current throughput, latency, RSS per connection or cliff.
- They are not deleted. They move to a subsection marked as the global-bus architecture, with their commit, as a record of where the project started.
- New baselines come from a full `scripts/bench.sh --all` run on the new code, after `scripts/calibrate.sh` passes, recorded with hardware, profile, commit and invocation as the table already requires.
- Both sets come from the same calibrated instrument, so for the `topics = 1` scenarios this change's PR can show a real before/after comparison. That comparison is the evidence for this change only. Later changes compare against post-change baselines.

### 12. Topic lifecycle

A topic has no existence of its own. It is the set of connections subscribed to a key, and the registry entry exists exactly while that set is non-empty. There is no create, delete, declare or configure operation, and none is planned.

| Stage | What happens | Where |
|---|---|---|
| **Does not exist** | No registry entry. A `publish` to the key is discarded, and nothing is created or retained. The publisher is not told. | decision 2, `topic-routing` spec |
| **Created** | The first `subscribe` to a valid key (1–255 bytes of UTF-8) inserts the entry with one subscriber. The connection receives `subscribed` only after the insert. A `publish` never creates a topic. | decisions 2 and 7 |
| **Active** | Further `subscribe`s join it and each `unsubscribe` leaves it, both by copy-on-write. Every publish fans out to the members present when it reads the entry. | decisions 2 and 3 |
| **Emptied** | The `unsubscribe` or disconnect (`Drop` of `Subscriptions`) that removes the last member also removes the entry, in the same atomic compute. A concurrent `subscribe` either lands before, so the entry stays, or after, so it creates a new one. It is never lost. | decisions 2 and 8 |
| **Re-created** | A later `subscribe` to the same key starts a new topic. Nothing from before survives: no frames, no member list, no settings. A frame published while the topic was empty is never delivered to anyone. | `topic-routing` spec |

**What a topic does not have:**

- **An owner.** Any connection may subscribe and publish to any key, and nobody can close a topic for others.
- **Namespacing.** Keys are compared byte for byte, and the gateway gives no meaning to `/`, `.`, `:` or case. Two unrelated applications that choose the same key share one topic. Separating them, for example with a prefix such as `app-a/…`, is the consuming application's job. The README recommends it, and the gateway does not enforce it.
- **Access control.** Nothing restricts who subscribes to what, so a connection can read every topic whose key it can guess. This matches the gateway as it is today, which has no authentication, but it is the main gap compared with the market (below).
- **Retention.** Nothing is kept for late subscribers. That would be the replay buffer, a borderline case in `docs/scope.md`.
- **Publish feedback.** A publisher does not learn how many connections received a frame, or whether the topic existed.

**Market comparison.** This lifecycle is the norm for ephemeral real-time pub/sub. In Redis Pub/Sub, NATS core subjects, MQTT brokers, Socket.IO rooms, Phoenix channels, Pusher, Ably and Centrifugo, a topic exists through use and disappears when unused. Explicit creation belongs to brokers whose topics hold durable state (Kafka, Google Pub/Sub, SNS, RabbitMQ exchanges and queues), and this gateway holds none.

This comparison is from the author's knowledge of those systems and was not re-verified against their current documentation for this proposal.

Where this design differs, and why:

- **Access control.** Almost every real-time product gates subscriptions: MQTT ACLs, NATS subject permissions, Phoenix `join/3`, Pusher `private-` channels. In Socket.IO only the server can place a connection in a room. This change leaves it out because the gateway has no authentication to build on. It is the expected next step after topics, and `subscribe` is the single choke point where it goes (*Extension points*).
- **Publish feedback.** Redis `PUBLISH` returns the receiver count; NATS and MQTT return nothing. This design follows NATS and MQTT, because a count would need a reply frame per publish, which doubles a heavy publisher's control traffic.
- **Wildcards.** NATS, MQTT and Redis `PSUBSCRIBE` offer them. They are left out because they turn the exact-key lookup on every publish into pattern matching, and `docs/scope.md` lists predicate subscriptions as borderline.
- **Key length.** MQTT allows up to 65,535 bytes and Pusher 164 characters for a channel name. The 255-byte limit here comes from the one-byte length in the binary header (decision 6).

### Extension points (not implemented)

- **Presence:** the topic's `Arc<[Subscriber]>` is the member list. Join and leave events hook the same compute that changes membership.
- **Direct delivery by id:** a second lock-free map `Uuid → Subscriber`, filled at connect and emptied by the same `Drop`, delivering through the same `inbox.send` path.
- **Backplane:** the first local subscriber of a topic makes the instance subscribe to it on the backplane, and removing the empty entry unsubscribes it. A local publish fans out locally and is sent once to the backplane. Remote frames enter through the same fanout function.
- **Authorization (expected next step):** `subscribe`, and `publish` if publishing is ever restricted, are the single choke points where a connect-time credential can be checked against a key. A prefix convention, such as Pusher's `private-` or Centrifugo's namespaces, would let the rule stay payload-agnostic. Authentication at connect time has to come first (decision 4 in `docs/decisions.md`).

## Decision entries for `docs/decisions.md`

Appended by the implementation PR. Entries 3, 4 and 8 are not edited. Each new entry names the one it supersedes.

---

## 12. Delivery is routed by topic; the global bus is gone

**Context.** One global bus made fanout O(N²) and made targeted delivery impossible (entry 8). Topics need the client to say what it wants, which entry 4 had ruled out by having no client-to-server protocol.

**Decision.** Clients send `subscribe`, `unsubscribe` and `publish` control frames. A publish reaches only the other subscribers of its topic, as `{"type":"message","topic":…,"from":…,"data":…}`. The gateway parses its own control frame and still never deserializes `data`. A topic is an opaque key of 1–255 bytes of UTF-8, compared byte for byte. It exists only while it has subscribers: the first `subscribe` creates it, removing the last subscriber deletes it, a `publish` never creates it, and nothing is retained across an empty period. Topics have no owner, no namespace and no access control. The global bus is removed: "everyone" is a topic everyone subscribes to. Supersedes entries 4 and 8.

**Consequence.** Breaking change for every client. Fanout is bounded by topic size. The gateway now has a client protocol to version and test. Anything a client wants beyond routing still goes inside `data`. Applications sharing a gateway must avoid key collisions themselves, and any connection can read any topic until subscription authorization is added, which is the expected next step.

---

## 13. The registry holds subscriber inboxes, read without locks

**Context.** Routing needs topic → subscribers, which is the first shared state beyond the connection counter (entry 6 anticipated a registry). A lock on the publish path would serialize every publisher.

**Decision.** A lock-free concurrent map (`papaya`) from topic to an immutable `Arc<[Subscriber]>`, where a subscriber is the connection's id and its inbox. Membership changes are copy-on-write and remove the entry when it empties. The publisher fans out by sending into each inbox, and the bridge task is removed. Rejected alternatives: a broadcast channel per topic, `RwLock`/`DashMap`, and subscriptions held in Redis (see the change's design).

**Consequence.** Publish takes no lock in gateway code and cannot grow memory. Membership changes cost O(topic size). Fanout work moves into the publisher's task. The same handle serves later presence, direct delivery and backplane work.

---

## 14. Backpressure is a per-connection inbox that drops the oldest

**Context.** Without the global bus there is no shared ring to lag behind. A first draft used an `mpsc` per connection, but an `mpsc` can only refuse new frames. That drops the newest, and a warning has nothing to ride on once the stream stops, so a receiver that stopped reading never learned what it lost.

**Decision.** Each connection's inbox is a 256-slot `tokio::sync::broadcast` channel with a single receiver, its writer. Publishers never wait, a full inbox overwrites its oldest frame, and the writer turns `Lagged(n)` into `{"type":"warning","dropped":n}` before continuing with the oldest frame still held. Control replies (`subscribed`, `unsubscribed`, `error`) use a separate channel and are never dropped. This amends the mechanism of entry 7, and the policy and the wire frame are unchanged.

**Consequence.** A slow receiver loses its oldest frames, is told exactly how many, and still gets the newest, as today. The count no longer includes a connection's own frames. `broadcast::send` takes tokio's internal locks, now contended per receiver instead of globally. A lock-free ring is a later option if a measurement shows that contention.

---

## 15. Binary frames carry a topic header

**Context.** Binary frames had no envelope (entry 3), so they could neither name a topic nor tell a receiver who sent them.

**Decision.** Inbound binary is `[len: u8][topic][payload]`. Delivered binary is `[len: u8][topic][sender uuid: 16 bytes][payload]`, built once per publish. The payload stays opaque. Supersedes entry 3.

**Consequence.** Binary clients must frame the header. In exchange they route like text clients and learn the sender. A topic is limited to 255 bytes for both frame types.

---

## Risks / Trade-offs

- **Open subscriptions.** Any connection can subscribe to any key, so topic keys are not a secret and must not be treated as one. This is documented in the README and entry 12, and authorization is named as the next step (decision 12).
- **Channel-internal locks remain.** `broadcast::send` locks the inbox's tail and slot. That is better than today's single global tail, but it is not lock-free. Mitigation: measured by the `churn-500` and fanout rows. The lock-free ring in decision 4 is the fallback if inbox contention shows up.
- **Breaking every client.** This is accepted and recorded in entry 12. The README protocol section is rewritten as the reference.
- **New dependency on the hot path.** `papaya` correctness under concurrent compute and remove is load-bearing. Mitigation: integration tests that subscribe and unsubscribe concurrently with publishing, and a bench run.
- **Fanout concentrated in one task.** A single very large topic adds latency proportional to its size for its last subscriber (decision 3). The bench row `goal-1m-fanout` at `T = 1` measures the worst case directly.
- **Unsubscribe window.** Frames in flight can arrive after `unsubscribed`. This is documented, and the spec scenario allows it.
- **Membership churn on huge topics** is quadratic. It is recorded as a scalability limit and not addressed.
- **Stale performance claims.** Until the new baselines are recorded, `docs/architecture.md` has no valid gateway numbers. Any claim made from the old rows in that window is wrong. Mitigation: the documentation and bench tasks land in the same PR as the code (decision 11).
- **Harness drift.** The `topics` side of the harness was calibrated only against `refserver`. If it and the gateway disagree on the protocol, calibration still passes but the gateway run fails or misroutes. Mitigation: the `incorrect` verdict, and investigating the first gateway run instead of tolerating it (decision 10).

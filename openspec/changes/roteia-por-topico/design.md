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

- `std::sync::RwLock<HashMap>` and `DashMap`. The reason is not the lock count per publish: the inbox already takes one tail lock per subscriber (decision 4). It is what the lock couples:
  - A map lock is shared across topics and taken for the whole lookup. `DashMap` shards it, but a membership change write-locks the shard while every publish to any topic in that shard read-locks it, so a burst of subscribes stalls unrelated publishers.
  - With `papaya`, a publish never waits for a membership change, because a reader sees the old or the new slice and never a lock.
  - The inbox locks only couple publishers aimed at the same receiver, and only for one slot write.
  - Under the reworded rule (entry 16), gateway code takes no lock at all. A map lock would be exactly such a lock, written in gateway code and held against other topics' publishers.
- An `arc-swap` of the whole map copies every topic on every membership change.

The `papaya` API was checked by compiling against 0.2.5:
- `HashMap::pin().compute(key, f)`, with `f` returning `Operation::Insert(new_slice)`, `Operation::Remove` or `Operation::Abort`, updates or removes the entry atomically;
- removing the last member returns `Compute::Removed`, and the entry is gone.

`papaya` may call `f` more than once under contention, so `f` must be pure. It only builds the new slice and decides insert or remove. Counting the connection's subscriptions, enforcing the 64 limit and sending `subscribed`/`unsubscribed` all happen outside `f`, after `compute` returns.

### 3. The publisher fans out and never waits

- The reader builds the outbound frame once:
  - text: the envelope serialized to a `Utf8Bytes`;
  - binary: header and payload copied once into a `Bytes`.
- For each subscriber other than itself, it sends a refcount clone with `inbox.send(frame)`. The call never waits: if the inbox is full, it overwrites the oldest pending frame.
- The cost of a publish is O(subscribers of the topic) inside the publisher's task. Today that cost is spread over N bridge tasks. Removing the bridge also removes one task and one wakeup per delivery.

Measured cost of the call itself: a topic with 10 000 subscribers is 10 000 `inbox.send` calls in one task. Each call takes that inbox's tail mutex and slot lock, which are uncontended unless several publishers hit the same receiver at once.

- **Conditions:** Apple M4 Pro, `--release`, tokio 1.53.1 and axum 0.8.9 as in `Cargo.lock`, one thread, 10 000 `broadcast(256)` inboxes, 600 rounds.
- **Result:** one `send` of a refcounted `Message` costs about 60 ns at p50 and 170–220 ns at p99, both while the ring fills and once it overwrites. One publish to 10 000 subscribers takes about 0.6 ms at p50.
- **What this does not include:** waking a writer that is parked in `recv`, cross-core cache traffic, and contention. It is a lower bound, not the end-to-end fanout latency.

The benchmark rows measure the real figure. If one hot topic makes it matter, a later change can split fanout across tasks.

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

**Locks, stated plainly.** `broadcast::Sender::send` takes a mutex on the tail and a lock on the slot (tokio 1.53.1, `sync/broadcast.rs:662` and `:677`). Today's global bus already takes the same locks, with every publisher contending for one tail. With one inbox per connection, contention is limited to publishers writing to the same receiver at the same instant. The locks are held for a few instructions and never across `.await`. The gateway's own code adds no lock. The project owner decided to reword the hard rule accordingly (entry 16) instead of claiming the path is lock-free (decision 9).

**Alternative kept for later:** a lock-free drop-oldest ring, for example crossbeam's `ArrayQueue::force_push`, with a notify. It removes tokio's locks, but placing the warning exactly at the gap under concurrent pushes and pops is the hard part that `broadcast` already solves. It is worth doing only if a measurement shows inbox contention mattering.

**Control replies are never dropped.** `subscribed`, `unsubscribed` and `error` frames go through a separate `mpsc` of `CONTROL_QUEUE_CAPACITY = 16` from the connection's own reader, with `send().await`. The reader produces at most one reply per inbound frame. A connection that floods control frames without reading its socket fills those 16 slots and then stalls only its own reader. If they shared the inbox, a lagging connection could lose its own acknowledgement. The writer serves the control channel first (`biased` select). The visibility guarantee only needs the registry insert to happen before `subscribed` is sent (decision 7), so the two channels need no ordering between them.

**Memory**, measured with a counting allocator under the same versions:

- an empty `broadcast::<Message>(256)` allocates **20 632 bytes**, all up front;
- the 16-slot control `mpsc` allocates **2 208 bytes**, the same as today's 32-slot `mpsc`, because tokio allocates `mpsc` blocks of 32 slots;
- a `Subscriber` handle is **24 bytes** and an axum `Message` is 48 bytes.

Net effect: about **+20 KB of fixed memory per connection** compared with today. That is roughly 14% on top of the 145 KiB per connection in `docs/architecture.md`, a figure measured with the old harness. The per-connection RSS from the new benchmark run is the number to record.

### 5. Inbound protocol: the gateway parses its own frame, never `data`

Text frames are parsed into a plain struct, and the dispatch on `type` happens after parsing:

```
struct ClientFrame<'a> {
    #[serde(rename = "type", borrow)] kind: Cow<'a, str>,
    #[serde(borrow)] topic: Cow<'a, str>,
    #[serde(borrow, default, deserialize_with = "present")] data: Option<&'a RawValue>,
}
```

Each attribute is there for a measured reason. Each was checked against `serde_json` 1.0.151, the version in `Cargo.toml`:

- **Not an internally tagged enum.** `#[serde(tag = "type")]` with a `&RawValue` field fails with `invalid type: newtype struct, expected any valid JSON value`. Worse, that enum shape first buffers the whole frame into serde's intermediate content tree, which would deserialize `data`. That violates "Never deserialize the payload". A plain struct reads `data` straight from the input as a raw slice.
- **`Cow` with `#[serde(borrow)]` on `kind` and `topic`.** A `&str` field rejects any value written with a JSON escape: `"type":"\u0070ublish"` fails with `invalid type: string "publish", expected a borrowed string`, although it is valid JSON for `publish`. A `Cow` accepts it and allocates only when the value contains an escape. Without `#[serde(borrow)]` the `Cow` is always owned, so every publish would allocate the topic. With it, an unescaped topic borrows from the frame, and an escaped one such as `"\u00e9"` is unescaped into an owned string. `Option<Cow<str>>` does not borrow even with the attribute, so `topic` is not optional: a frame without it fails to parse and gets `INVALID_FRAME`.
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
- **Unsubscribe ordering.** The registry removal, the `compute` that drops the handle, happens before `unsubscribed` is sent on the control channel. Any publish that reads the topic's entry after the removal no longer sees the connection, so no publish made after the client received `unsubscribed` can reach it. This mirrors subscription visibility, and the `topic-routing` requirement "Unsubscribing stops delivery" depends on it.
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
| No locks on the hot path | Kept, under the reworded rule decided for this change (entry 16): "Gateway code uses no locks. Short internal locks inside tokio channels are allowed, and are never held across `.await`." Publish reads a lock-free map, and gateway code takes no lock. Each `inbox.send` takes tokio's short internal tail and slot locks (decision 4), as the global bus already does today, but contended only per receiver instead of globally. Shared state is the `AtomicUsize` and the lock-free topic registry. |
| Never `.await` holding a lock | Kept. Fanout is synchronous, and tokio's internal locks are released inside `send`. |
| Bounded everywhere | Kept. Inbox of `INBOX_CAPACITY = 256` and control channel of `CONTROL_QUEUE_CAPACITY = 16` per connection, at most 64 subscriptions per connection, topics of 255 bytes at most. The registry is bounded by `connections × 64` entries. |
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
- **Access control.** Nothing restricts who subscribes to what or who publishes where, so a connection can read every topic whose key it can guess and inject frames into it, always under its own `from` id. This matches the gateway as it is today, which has no authentication, but it is the main gap compared with the market (below).
- **Retention.** Nothing is kept for late subscribers. That would be the replay buffer, a borderline case in `docs/scope.md`.
- **Publish feedback.** A publisher does not learn how many connections received a frame, or whether the topic existed.

**Market comparison.** Implicit lifecycles like this are the norm for ephemeral real-time pub/sub:

- **Redis Pub/Sub:** a channel is described only by its subscribers ("an active channel is a Pub/Sub channel with one or more subscribers"), and there is no create or delete command.
- **NATS core:** subjects need no declaration ("creating new subjects has virtually no overhead"), and a message nobody is subscribed to is not stored.
- **MQTT:** the spec allows either: a topic "MAY be either predefined in the Server by an administrator or it MAY be dynamically created by the Server when it receives the first subscription or an Application Message". Mosquitto creates topics on use.
- **Socket.IO:** the in-memory adapter creates a room on the first join and deletes it when the last socket leaves, emitting `create-room` and `delete-room`. This is in the adapter source; the rooms page lists the events but not when they fire.
- **Phoenix:** a topic is just an identifier, and joining is authorized by `join/3`.
- **Pusher:** channels are "instantiated on client demand", and become occupied and vacated as subscribers come and go.
- **Ably:** channels "are created on demand when clients attach", and close some time after the last client detaches.
- **Centrifugo:** "Channels are automatically created by Centrifugo as soon as the first client subscribes. Similarly, when the last subscriber leaves, the channel is automatically cleaned up."

Brokers whose topics hold durable state treat a topic as a resource with its own lifetime instead:

- **Google Pub/Sub and SNS** create topics explicitly (`CreateTopic` in SNS is idempotent).
- **RabbitMQ** has exchanges and queues declared by clients. Declaring an existing queue with the same attributes has no effect. An `auto-delete` queue is removed when its last consumer goes, but only if it ever had one.
- **Kafka** auto-creates a topic on first use by default (`auto.create.topics.enable`, default `true`), but keeps it and its log until it is explicitly deleted.

None of these brokers removes a topic just because nobody is listening, and this gateway, which holds no durable state, has nothing that would need that.

Every statement above was checked against each project's official documentation or source on 2026-10-05:
- [redis.io pubsub-channels](https://redis.io/docs/latest/commands/pubsub-channels/)
- [docs.nats.io subjects](https://docs.nats.io/nats-concepts/subjects)
- [MQTT 5.0 spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html)
- [socket.io-adapter in-memory-adapter.ts](https://github.com/socketio/socket.io/blob/main/packages/socket.io-adapter/lib/in-memory-adapter.ts)
- [Phoenix.Channel](https://phoenix.hexdocs.pm/Phoenix.Channel.html)
- [Pusher channels](https://pusher.com/docs/channels/using_channels/channels/)
- [Ably channel states](https://ably.com/docs/channels/states)
- [Centrifugo channels](https://centrifugal.dev/docs/server/channels)
- [SNS CreateTopic](https://docs.aws.amazon.com/sns/latest/api/API_CreateTopic.html)
- [RabbitMQ queues](https://www.rabbitmq.com/docs/queues)
- [Kafka broker configs](https://kafka.apache.org/41/configuration/broker-configs/)

Where this design differs, and why:

- **Access control.** Almost every real-time product gates subscriptions:
  - Mosquitto ACLs (`topic [read|write|readwrite|deny]`);
  - NATS permissions ("a grant to publish to, or subscribe to, a set of subjects");
  - Phoenix `join/3`;
  - Pusher `private-` and `presence-` channels, which need a server-signed authorization token;
  - Ably capabilities;
  - Centrifugo namespace options.

  In Socket.IO, `join` exists only on the server-side socket, so the client cannot place itself in a room. This change leaves it out because the gateway has no authentication to build on. It is the expected next step after topics, and `subscribe` is the single choke point where it goes (*Extension points*).
- **Publish feedback.** Redis `PUBLISH` returns "the number of clients that the message was sent to". In a Redis Cluster it counts only clients on the publisher's node. No other system here returns a count:
  - **NATS core:** a plain publish has "no acknowledgment". Only request/reply gets a "no responders" 503 when a subject has zero subscribers.
  - **MQTT 5.0:** QoS 0 gets no response at all. At QoS 1/2 the server MAY answer reason code 0x10, "No matching subscribers", instead of success. That is optional and a yes/no signal, not a count.

  This design returns nothing, like a NATS core publish or MQTT QoS 0, because any answer means a reply frame per publish, which doubles a heavy publisher's control traffic. A yes/no "no subscribers" signal like MQTT's 0x10 would be the cheapest addition if a consumer ever needs one. It is not part of this change. Sources: [redis.io PUBLISH](https://redis.io/docs/latest/commands/publish/), [NATS core](https://docs.nats.io/nats-concepts/core-nats), [NATS request-reply](https://docs.nats.io/learn/core-nats/request-reply), [MQTT 5.0 §3.4.2.1](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html).
- **Wildcards.** NATS, MQTT and Redis `PSUBSCRIBE` offer them. They are left out because they turn the exact-key lookup on every publish into pattern matching, and `docs/scope.md` lists predicate subscriptions as borderline.
- **Key length.** MQTT topic names "MUST NOT encode to more than 65,535 bytes", and Pusher channel names are limited to 164 characters including the `private-`/`presence-` prefix. The 255-byte limit here comes from the one-byte length in the binary header (decision 6).
- **Publishing without a subscription.** This design follows the broker model, where publishing and subscribing are independent:
  - Redis Pub/Sub categorizes messages "without knowledge of what (if any) subscribers there may be";
  - NATS and MQTT treat publish and subscribe as separate operations with separate permissions;
  - Ably can publish over REST "outside the context of any specific connection";
  - in Kafka, "producers and consumers are fully decoupled and agnostic of each other".

  Room-style products tie publishing to membership instead:
  - Phoenix: "Clients must join a channel to send and receive PubSub events on that channel".
  - Pusher: client events "can only be triggered on private and presence channels", "the user must be subscribed to the channel", and they must be enabled in the app settings.
  - Socket.IO: rooms are server-side (`join` exists only on the server socket), so server code decides which rooms an event reaches.
  - Centrifugo offers both: `allow_publish_for_subscriber` requires the subscription, and `allow_publish_for_client` explicitly does not.

  Those products have an application server in the middle that does the real publishing, so room membership doubles as a permission. This gateway has no application server inside it: a backend that publishes is just another connection. Requiring a subscription would force it to receive a topic's whole traffic just to write to it, and would break fan-in uses (telemetry, a backend publishing notices it never reads). It would not protect anything either, since any connection may subscribe. The `from` field is set by the gateway, so a publisher cannot impersonate another connection.

### Extension points (not implemented)

- **Presence:** the topic's `Arc<[Subscriber]>` is the member list. Join and leave events hook the same compute that changes membership.
- **Direct delivery by id:** a second lock-free map `Uuid → Subscriber`, filled at connect and emptied by the same `Drop`, delivering through the same `inbox.send` path.
- **Backplane:** the first local subscriber of a topic makes the instance subscribe to it on the backplane, and removing the empty entry unsubscribes it. A local publish fans out locally and is sent once to the backplane. Remote frames enter through the same fanout function.
- **Authorization (expected next step):** `subscribe` and `publish` are the single choke points where a connect-time credential can be checked against a key. They need separate read and write permissions, as in NATS (subject publish/subscribe permissions), MQTT ACLs (read/write) and Ably capabilities. A connection may be allowed to publish to a topic without being allowed to read it (fan-in), or the reverse (a read-only consumer). Gating writes by requiring a subscription is not an option, for the reasons under *Market comparison*. A prefix convention, such as Pusher's `private-` or Centrifugo's namespaces, would let the rule stay payload-agnostic. Authentication at connect time has to come first (decision 4 in `docs/decisions.md`).

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

## 16. The no-locks rule covers gateway code, not tokio channel internals

**Context.** `CLAUDE.md` said "No locks on the hot path", and `docs/architecture.md` said the only shared mutable state was one `AtomicUsize`. Neither was literally true: today's global bus is a `tokio::sync::broadcast`, whose `send` locks the channel tail and a slot (tokio 1.53.1, `sync/broadcast.rs:662` and `:677`), with every publisher contending on that one tail. Topic routing keeps `broadcast`, now as one inbox per connection, so the question had to be decided explicitly.

**Decision.** The rule is reworded: "Gateway code uses no locks. Short internal locks inside tokio channels are allowed, and are never held across `.await`." Gateway code takes no lock and holds nothing across `.await`. Channel-internal locks are accepted because they are held for a few instructions, are released inside the call, and in this design are contended only by publishers writing to the same receiver at the same instant, instead of by every publisher as today. Replacing the inbox with a lock-free ring is not done preemptively. It is reopened only if a benchmark shows inbox contention, which the `ingest-50`, `goal-1m-ingest` and `goal-1m-mesh` rows measure.

**Consequence.** The rule now describes what the code does. Reviews check gateway code for locks and for `.await` while holding one, not tokio's internals. A future dependency that takes locks inside its own calls falls under the same allowance only if those locks are short and never held across `.await`; anything else needs a new entry.

---

## Risks / Trade-offs

- **Open subscriptions.** Any connection can subscribe to any key, so topic keys are not a secret and must not be treated as one. This is documented in the README and entry 12, and authorization is named as the next step (decision 12).
- **Channel-internal locks remain, by decision.** `broadcast::send` locks the inbox's tail and slot. That is better than today's single global tail, and it is allowed under the reworded rule (entry 16). Inbox contention needs many publishers sending to the same receiver. That is the shape of `ingest-50`, `goal-1m-ingest` and `goal-1m-mesh`, so those rows measure it; the fanout and churn rows do not. The lock-free ring in decision 4 is the fallback if those rows show it.
- **Breaking every client.** This is accepted and recorded in entry 12. The README protocol section is rewritten as the reference.
- **New dependency on the hot path.** `papaya` correctness under concurrent compute and remove is load-bearing. Mitigation: integration tests that subscribe and unsubscribe concurrently with publishing, and a bench run.
- **Fanout concentrated in one task.** A single very large topic adds latency proportional to its size for its last subscriber (decision 3). The largest topic any scenario measures has 1 001 members (`beyond-2m-fanout`). The 10 000-subscriber estimate in decision 3 stays unmeasured and is recorded as a scalability limit in `docs/architecture.md`, not as a measured result.
- **Unsubscribe window.** Frames in flight can arrive after `unsubscribed`. This is documented, and the spec scenario allows it.
- **Membership churn on huge topics** is quadratic. It is recorded as a scalability limit and not addressed.
- **Stale performance claims.** Until the new baselines are recorded, `docs/architecture.md` has no valid gateway numbers. Any claim made from the old rows in that window is wrong. Mitigation: the documentation and bench tasks land in the same PR as the code (decision 11).
- **Harness drift.** The `topics` side of the harness was calibrated only against `refserver`. If it and the gateway disagree on the protocol, calibration still passes but the gateway run fails or misroutes. Mitigation: the `incorrect` verdict, and investigating the first gateway run instead of tolerating it (decision 10).

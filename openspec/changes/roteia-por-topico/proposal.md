# Proposal

## Why

Every frame goes to every connection. There is one global `broadcast` bus (`src/state.rs:15` — `AppState`), so fanout is O(N²): with `N` connections publishing at rate `R`, the gateway performs `N × R × (N−1)` deliveries per second. A client cannot address a subset of connections, so targeted delivery and isolated groups are impossible. `docs/architecture.md` names topics as the highest-value next step and says they must come before any backplane decision. `docs/decisions.md` entry 8 says routing should be revisited before optimizing anything else about fanout.

## Scope verdict

**In scope, with a decision entry.** `docs/scope.md` lists routing by an opaque key (topics, rooms, channels) under *In scope*. The topic is a key the client chooses. The gateway compares it byte for byte and never interprets it. `data` stays a `&RawValue` and is never deserialized.

Two things make it more than a drive-by change, so the decision entries in `design.md` go into `docs/decisions.md` with the implementation:

- The gateway gains a small client-to-server protocol (`subscribe`, `unsubscribe`, `publish`). This replaces decision 4, "there is no client-to-server protocol beyond send a frame".
- The gateway gains shared state beyond the `AtomicUsize`: a registry from topic to subscribers. It must stay bounded and lock-free on the publish path.
- The hard rule "No locks on the hot path" is reworded, by the project owner's decision, to what the code actually does: "Gateway code uses no locks. Short internal locks inside tokio channels are allowed, and are never held across `.await`." Today's global bus already relies on tokio's internal `broadcast` locks. The per-connection inbox keeps them and contends only per receiver (`design.md` decision entry 16).

Vocabulary: the only new noun is `topic`. No `room`, `channel name` or other domain term appears in frames, types, constants or error messages.

## What Changes

- **BREAKING** The global bus is removed. A frame is delivered only to the connections subscribed to the topic it was published to. "Everyone" is a topic that every client subscribes to.
- **BREAKING** A text frame is no longer the payload itself. Clients send one of three control frames:
  - `{"type":"subscribe","topic":"<key>"}`, answered with `{"type":"subscribed","topic":"<key>"}`
  - `{"type":"unsubscribe","topic":"<key>"}`, answered with `{"type":"unsubscribed","topic":"<key>"}`
  - `{"type":"publish","topic":"<key>","data":<any JSON>}`
- **BREAKING** Text deliveries become `{"type":"message","topic":"<key>","from":"<uuid>","data":<payload>}`. `data` is still embedded byte for byte.
- **BREAKING** Binary frames carry a minimal header:
  - Inbound: `[topic length: u8][topic: UTF-8][payload]`.
  - Delivered: `[topic length: u8][topic][sender uuid: 16 bytes][payload]`.
  - The header is built once per publish. Binary receivers now learn the sender, which removes the trade-off recorded in decision 3.
- **Topic lifecycle, stated explicitly** (`design.md` decision 12):
  - A topic exists only while at least one connection is subscribed to it. The first `subscribe` creates it, and removing the last subscriber (by `unsubscribe` or disconnect) deletes it.
  - A `publish` never creates a topic. Publishing to a topic with no subscribers is a silent no-op.
  - Nothing is retained across an empty period: a topic re-created later starts from nothing.
  - Topics have no owner, no namespace and no access control. Two applications that choose the same key share it.
  - This is how ephemeral real-time pub/sub works in Redis, NATS, MQTT, Socket.IO and similar systems. The gap compared with them is subscription authorization.
- Publishing does not require a subscription. Senders never receive their own frames, even when subscribed.
- New limits, each answered with an `error` frame while the connection stays open:
  - topics of 1 to 255 bytes of UTF-8;
  - at most 64 subscriptions per connection.
- Backpressure keeps today's policy, dropping the oldest frames, with a per-connection mechanism instead of the global bus. Each connection gets a 256-slot `broadcast` inbox with one receiver. A slow receiver loses its oldest frames, gets the same `{"type":"warning","dropped":n}` frame exactly at the gap, and still receives the newest ones. Control replies use a separate channel and are never dropped.
- Each connection runs two tasks (reader, writer) instead of three. The bridge task disappears.
- The benchmark harness switches to the topic protocol. The rebuild itself (per-topic expectations, correctness meters, topic scenarios, `goal-1m-topics`) is the separate change `prepara-harness-para-topicos`, which lands first and keeps a temporary `legacy` protocol. This change makes `topics` the default and deletes `legacy`.
- Documentation is updated to match: `README.md`, `docs/architecture.md`, `docs/decisions.md`, `CLAUDE.md`, `openspec/config.yaml` and the `gateway-review` and `perf-check` skills.

## Capabilities

### New Capabilities

- `topic-routing`: subscribing, unsubscribing, publishing by topic, topic limits, and who receives a publish.

### Modified Capabilities

- `message-relay`: text and binary relay now go to subscribers of a topic, text deliveries carry `topic`, binary frames carry a topic header, and unrecognized text frames are rejected.
- `connection-lifecycle`: scenarios that relied on the global bus now subscribe and publish. The frame size limit applies to the whole control frame.
- `delivery-backpressure`: lag is measured against each connection's own inbox instead of the shared bus. The drop-oldest policy and the `warning` frame are unchanged.

## Impact

- **Code:**
  - `src/state.rs` loses `BroadcastMessage` and the bus.
  - A new `src/registry.rs` holds the topic registry and the subscriber handle.
  - `src/protocol.rs` gains the inbound control frames, the `subscribed`/`unsubscribed` frames, `topic` on `message`, and the binary header.
  - `src/ws.rs` replaces the bridge task with direct fanout from the reader.
- **Dependencies:** one new crate for a lock-free concurrent map (`papaya`, see `design.md`).
- **Tests:** every test in `tests/gateway.rs` that relies on the global bus is rewritten to subscribe first, and new tests cover each `topic-routing` scenario.
- **Clients:** all existing clients break. There are no known external consumers. The README protocol section is the migration guide.
- **Performance:** the hot path changes, so the implementation PR carries a manual benchmark run.
- **Existing benchmarks become invalid.** By the time this change starts, the global-bus numbers in `docs/architecture.md` will have been re-measured by `prepara-harness-para-topicos` with the new harness. This change replaces exactly the part that dominates fanout: the bus, the bridge task per receiver and the lag mechanism. After implementation those rows describe a system that no longer exists. They move to a section marked as the old architecture, and new baselines are measured from scratch with `scripts/bench.sh --all`. Both sets come from the same instrument, so the PR can show a real before/after comparison (`design.md` decision 11).
- **Depends on** `prepara-harness-para-topicos` being merged first.
- **Out of scope:** presence, direct delivery by connection id, wildcard or predicate subscriptions, a backplane, and authorization of who may subscribe. The design keeps each one as an extension of the registry (see `design.md`). None is implemented here.
- **Expected next step:** subscription authorization, which needs connect-time authentication first. Until it exists, any connection can subscribe to any topic, so a topic key must not be treated as a secret.

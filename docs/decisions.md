# Decisions

Why the design is the way it is. Read this before changing any of it — these are
settled questions, and reopening one should be a deliberate act with a new entry
appended, not a silent edit.

Format: context, decision, consequence.

---

## 1. The gateway is generic; the payload is opaque

**Context.** The first implementation was a chat server: a `join` handshake with
a `username`, messages shaped as `{"type":"message","content":"..."}`, and
rejection of anything else. That made the gateway unusable for notifications,
game state, or any other purpose.

**Decision.** The gateway relays messages without interpreting them. Text frames
are validated as JSON via `&RawValue` — enough to embed them safely in an
envelope — and never deserialized further.

**Consequence.** Every domain concept is now the consuming application's
problem, which is the intent. It also means the gateway cannot offer any feature
that depends on message content; see `docs/scope.md`.

---

## 2. Messages are wrapped in an envelope, not passed through raw

**Context.** A pure byte-for-byte relay is maximally neutral, but the receiver
then has no idea who sent a message, and the gateway has no way to signal
anything — an error or a lag warning injected into the stream would be
indistinguishable from a client payload.

**Decision.** Text frames are delivered as
`{"type":"message","from":"<uuid>","data":<payload>}`. Gateway-generated frames
(`welcome`, `warning`, `error`) share the same `type` discriminator.

**Consequence.** Text payloads must be valid JSON, which is a real constraint on
clients. Clients that need arbitrary bytes use binary frames instead.

---

## 3. Binary frames pass through without an envelope

**Context.** JSON cannot embed arbitrary bytes without base64, which costs CPU
and inflates payloads by a third.

**Decision.** Binary frames are relayed verbatim.

**Consequence.** There is no ambiguity with the JSON envelope, because text and
binary are distinct WebSocket frame types. The cost is that binary receivers do
not learn the sender's id — they must carry it in their own framing if they need
it. This is the right trade for clients that already have their own binary
protocol, which is the usual reason to send binary at all.

---

## 4. No handshake

**Context.** The chat implementation required a `join` frame before doing
anything, with a 10-second timeout enforcing it. Once `username` was removed,
the only thing left for the handshake to carry was nothing.

**Decision.** Connecting is joining. The server immediately sends
`{"type":"welcome","id":"<uuid>"}` with the assigned identity.

**Consequence.** `wait_for_join`, `JOIN_TIMEOUT`, `MAX_USERNAME_LEN` and the
entire `ClientMessage` type disappeared; there is no client-to-server protocol
beyond "send a frame". If authentication is added later it goes at connect time
— headers, query string, or a subprotocol — not as an in-band frame.

---

## 5. The envelope is serialized once per message, not once per receiver

**Context.** The original code broadcast a struct and let every subscriber's
task serialize it independently, so a message reaching `N` clients was
serialized `N` times.

**Decision.** The publishing connection builds the final `Message` and puts that
on the bus. `axum`'s `Message::Text`/`Binary` are backed by `Utf8Bytes`/`Bytes`,
so each subscriber's clone is a refcount increment.

**Consequence.** Fanout cost per receiver drops to a refcount bump and a queue
push. This is why the envelope cannot contain per-receiver information — it is
built before the gateway knows who will receive it.

---

## 6. Connection count is an atomic, not a registry

**Context.** An earlier version kept `RwLock<HashMap<Uuid, UserInfo>>` of every
connection. Nothing read it except a `len()` call in a log line.

**Decision.** Replaced with an `AtomicUsize`.

**Consequence.** No lock exists on the hot path. A registry will be needed again
for presence or direct addressing; when that happens it should be added with a
consumer already in mind, and the "no `.await` while holding a lock" invariant
in `docs/architecture.md` becomes live rather than trivial.

---

## 7. Backpressure drops messages for the slow client

**Context.** A client that stops reading must not be able to stall the gateway
or grow its memory without bound.

**Decision.** Bounded bus (`BROADCAST_CAPACITY`) plus bounded per-connection
queue (`LOCAL_QUEUE_SIZE`). A consumer that falls too far behind gets
`Lagged(n)`, which is surfaced to it as `{"type":"warning","dropped":n}`.

**Consequence.** Delivery is best-effort, and the client is told when it was not.
Clients needing guaranteed delivery must build acknowledgement and recovery on
top — which is listed as a borderline feature in `docs/scope.md`, not a given.

---

## 8. One global bus, for now

**Context.** Topics or rooms are the obvious next primitive, and without them
every connection receives every message.

**Decision.** Ship the single global bus and defer routing.

**Consequence.** Fanout is O(N²) and the gateway is a broadcaster rather than a
router — it cannot do targeted notification or isolated game sessions. This is
the largest known limitation and is documented in `docs/architecture.md`.
Revisit before optimizing anything else about fanout, because topics change the
shape of that cost entirely.

---

## 9. The writer flushes in batches, not once per message

**Context.** The writer task called `sender.send(msg)` per queued message,
which is `feed` plus `flush` — one syscall per frame. Under load, `top`
showed the gateway spending the large majority of its CPU in `sys`, not
`user`; an A/B run at `--connections 1000 --senders 100 --rate 50 --seconds 8`
measured `sys` time dropping from roughly 92% of process CPU to roughly 65%
after batching, with throughput up and the client-visible backpressure
warnings down by two orders of magnitude.

**Decision.** The writer drains its local queue with
`rx_local.recv_many(&mut batch, LOCAL_QUEUE_SIZE)` — which waits for at least
one message, then takes whatever else is already queued without waiting
further — calls `sender.feed(msg)` for each item in order, and flushes once
per batch.

**Consequence.** Per-socket delivery order is unchanged: nothing is dropped,
reordered, or coalesced beyond what `Lagged` already drops upstream at the
bus. Idle sockets still flush a batch of one, so latency for a lone message is
unaffected. The same batching also fixed the hot-path logging cost: the
per-message `info!`/`warn!` calls that formatted a `Uuid` for every fanned-out
frame were removed, with the lag count folded into the existing
connect/disconnect log lines instead.

## 10. The load generator is calibrated against a known answer before it is believed

**Problem.** The first two baselines in `docs/architecture.md` were produced by
a harness nobody had checked. It counted frames over the whole run, warmup
included, and divided by the measured window alone, so every throughput figure
it ever printed was inflated by `seconds / (seconds - warmup)` — 25% at the
`--seconds 10` used for those runs. It timestamped latency samples *after*
building a full `serde_json::Value` from each received frame, started its clock
before the first socket connected, capped samples per connection so the tail of
a long run went unrecorded, silently fell back to defaults on an unparseable
flag, and — worst — drove its senders from a `tokio::time::interval` inside the
same `select!` as the reader. When a send blocked, the generator simply
published less and said nothing: a run asking for 150,000 frames delivered
73,126 of them and reported the result as if the requested load had been
applied.

**Decision.** Publishing follows an absolute schedule (`frame n` is due at
`start + n / rate`), and each frame carries both its due instant and the instant
it actually left, so *service* and *response* latency are reported side by side
and neither coordinated omission nor harness lateness can hide in a single
number. A frame belongs to the measured window by the timestamp its publisher
stamped on it rather than by when it arrived, which makes the throughput
denominator match its numerator exactly and lets in-flight frames drain after
the window instead of counting as loss. Samples go into a log-linear histogram
with a bounded relative error under 1% instead of a truncating vector, all
sockets are opened and acknowledged before the clock starts, and an unknown or
unparseable argument aborts.

None of that is self-evidently correct either, so `scripts/calibrate.sh`
measures the harness against `examples/refserver.rs` — a reference server that
speaks the same wire protocol with a delay, a loss rate and a freeze chosen on
purpose. Every metric there has an answer known by arithmetic: 2,500 frames
published, 497,500 delivered, 49,750 msg/s, 100% delivery, 90% under 1-in-10
loss, service p50 of 50 ms under an injected 50 ms. `scripts/bench.sh` runs the
calibration first and refuses to produce baselines if it fails.

**Consequence.** The gateway's numbers moved, and the old ones were wrong: the
`--connections 200 --senders 5 --rate 50` baseline was published as 62k msg/s
and is actually 49,750 msg/s, which is also exactly
`senders x rate x (connections - 1)` — the clean rows measure latency and cost
at a known offered load, not capacity. The harness also reports its own CPU
next to the gateway's, which is how the cliff row is now visibly harness-bound
rather than quietly presented as the gateway's ceiling.

## 11. The harness measures routing correctness, not just speed

**Context.** The harness from entry 10 assumed one global bus: every frame reaches every other connection, a connection is ready on `welcome`, and nothing can be misrouted. Topic routing breaks each of those assumptions. It also makes new failures possible, such as a frame delivered to a non-subscriber or lost without a `warning`, which a speed-only harness reports as a clean pass.

**Decision.** `loadgen` computes expected deliveries per topic from acknowledged members. It reports `misrouted`, `dropped`, `unaccounted` and subscribe-acknowledgement latency, and it can publish binary frames and churn memberships. It drains until delivery settles instead of for a fixed second, and it reports `unaccounted` as `null` when the drain did not complete, so an overloaded but correct run is never called `incorrect`. `refserver` gains `--warn-drops`, `--ignore-topics`, `--drop-subscribe-acks` and `--drop-unsubscribe-acks`, so each new meter has a calibration case with a known answer. `bench.sh` gains an `incorrect` verdict that overrides any other. Both the old and the topic protocol are supported until topic routing lands, so `main` stays measurable and the current gateway gets a same-instrument baseline.

**Consequence.** The 2026-08-03 baselines are superseded by a re-measurement with the new harness. A fast run that routes wrongly can no longer pass. The `legacy` protocol is temporary and is removed by the topic-routing change.

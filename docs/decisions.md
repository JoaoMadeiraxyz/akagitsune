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

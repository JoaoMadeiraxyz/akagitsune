# Design

## Context

`read_loop` in `src/ws.rs:164` reads one frame at a time and handles it before reading the next. A publish fans out synchronously inside it (`src/ws.rs:210`), so fanout happens in the order frames were read. A connection that cannot be served already stalls only its own reader (`CONTROL_QUEUE_CAPACITY`, `docs/architecture.md` scalability table). The change builds on exactly that.

Example the project owner gave, which the design must reproduce:

```
msg1 -> delay 5 s
msg2 -> no delay
msg3 -> delay 6 s
```

msg1 leaves 5 s after it became the next frame to process. msg2 leaves immediately after msg1. msg3 leaves 6 s after msg2.

## Decisions

### 1. The delay is served by the reader, not by a queue

**Considered.** A queue per connection ordered by due time, drained by a timer task, with a byte budget per connection and a global one.

**Chosen.** The reader waits before fanning out. FIFO comes from the reader being sequential, the delay semantics from where the wait sits, and backpressure from the socket not being read during the wait.

**Why.** Memory per connection stays at one in-flight frame (at most `MAX_MESSAGE_SIZE`, 64 KiB), instead of a backlog that grows with the burst. A byte cap per connection did not help: 100 connections at a 10 MB cap is still 1 000 MB, and the clients this feature is for are exactly the ones that send large bursts. The backlog stays where it was produced, in the client and in the TCP buffers, which the kernel already bounds per socket. A queue, a budget and a timer task all disappear.

**Cost.** The wait blocks the connection's other inbound frames (decision 3).

### 2. The delay counts from the moment the frame becomes the next one to process

Not from the previous frame's send. A frame that arrives after a long idle period with `delay_ms: 6000` still waits 6 s. It is predictable, needs no stored timestamp, and reads as "wait this long before sending this".

### 3. One frame of lookahead, so a disconnect is noticed during the wait

**Problem.** While the reader sleeps nobody reads the socket and the writer is usually idle, so a client that closes during the wait would not be noticed until the wait ends, and its frame would still be delivered after it left. That contradicts decision 5.

**Chosen.** `wait_out` selects between the timer and `stream.next()`. A `Close`, end of stream or error returns `Disconnected` and the publish is dropped. `Ping` and `Pong` are skipped. The first `Text` or `Binary` frame is held, and the stream is not polled again until the timer fires. The held frame is processed right after the delayed publish.

**Evidence from the spike** (`tests/gateway.rs`, spike worktree): a `Close` frame and an abrupt TCP drop during the wait both leave the subscriber with nothing; a text frame sent during the wait is processed after the delayed publish; a `Ping` during a 600 ms wait is answered in 0.37 ms because tungstenite answers it inside the read poll. `SplitStream::next` is cancel-safe: `futures-util-0.3.33/src/stream/stream/next.rs:32` is only `self.stream.poll_next_unpin(cx)`, and `split.rs:41` drops the lock guard inside the poll.

**Limits, kept and documented.**
- Only one frame is read ahead. A `Close` that arrives after a held frame is not seen until the wait ends, and the delayed publish is then delivered.
- A client cannot cancel a waiting publish. Only disconnecting does.
- A held frame is discarded if the connection ends before it is processed.
- A peer that disappears without a FIN or RST is not noticed before TCP notices it. This is unchanged from today.

**Cost of the lookahead.** About 18 lines (`wait_out`), one `Option<Message>`, one more frame (up to 64 KiB) per waiting connection. Performance is not yet established (`tasks.md` 4.2). If repeated measurements show the no-delay path regressed outside the run-to-run spread, the lookahead is reopened and this entry is amended with the numbers.

### 4. Text only in this version

The binary header is `[len][topic][payload]` with no room for a delay (`src/protocol.rs:60`). Adding one changes the wire format and is a second-wire-format decision in `docs/scope.md`. Binary publishes are never delayed, and a client that wants paced binary frames waits on its own side.

### 5. Everything a connection owns dies with it

A waiting publish and a held frame belong to the connection. When the connection ends they are dropped. A new connection has a new UUID and starts with nothing. The gateway keeps no durable state, which is why storing waiting publishes in a database was rejected: it would need an identity that survives reconnection, a recovery path that republishes to whoever is subscribed at that moment, and a write on the hot path. An application that needs durable scheduling runs a service beside the gateway that stores the publishes and sends them at the due time through the normal protocol.

Shutdown needs no rule of its own. It closes connections, and what they hold is dropped.

### 6. The lock rule covers tokio's timer

`tokio::time::sleep` registers with the runtime's timer wheel, which is protected by a mutex (`tokio-1.53.1/src/runtime/time/mod.rs:98`, `state: Mutex<InnerState>`). Gateway code takes no lock and holds nothing across `.await`, but the rule as worded (`CLAUDE.md`, `docs/decisions.md` entry 16) names tokio channels only. The wording becomes "Short internal locks inside tokio's channels and timers are allowed, and are never held across `.await`", by the project owner's decision. The same allowance conditions as entry 16 apply.

### 7. Limits

- `MAX_DELAY_MS = 60 000`. The longest a single publish can hold the reader. It bounds how long a connection can be stalled by one frame and how long a delivery can lag after its send.
- No cap on the number of delayed publishes in a row. They are never queued, so each one holds the reader for its own delay only, and the cost of a long burst falls on the client's own connection.
- No per-connection or global byte budget. Memory is bounded by the two-frame hold and by `MAX_MESSAGE_SIZE`.

## Hot-path invariants

| Invariant | Effect |
|---|---|
| Payload never deserialized | Untouched. `data` stays `&RawValue`. `delay_ms` is read from the control frame. |
| One serialization per message | Untouched. The envelope is built once, after the wait, inside the same fanout closure. |
| Cloning a message is a refcount bump | Untouched. |
| No locks in gateway code | Gateway code adds none. The timer's internal mutex is covered by decision 6. |
| Never `.await` while holding a lock | Untouched. `wait_out` holds none. |
| Bounded everywhere | Preserved and strengthened: a waiting connection holds at most two frames of at most 64 KiB, with no queue. |
| No per-message logging in the fanout path | Untouched. Nothing is logged for a delayed publish. |

## Risks

- **Heartbeat-driven clients.** While a publish waits, subscribe, unsubscribe and application-level pings the client sends as text frames are not processed. Protocol-level `Ping` is answered. The gateway has no heartbeat of its own, so it drops nobody for this.
- **Silent downgrade.** A gateway without this feature ignores `delay_ms`. The client cannot detect it from frames alone.
- **Disconnect noticed late** in the single case described in decision 3.
- **Performance of the no-delay path is unproven** beyond one benchmark sample per side.

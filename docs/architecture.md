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

| Task       | Job                                                                                                                                            |
|------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| **reader** | Reads frames from the socket. Text is validated as JSON and wrapped in an envelope; binary is passed through untouched. Publishes to the bus.  |
| **bridge** | Receives from the bus, skips messages the connection itself published, and forwards to the local queue. Turns `Lagged` into a `warning` frame. |
| **writer** | Drains the local queue into the socket.                                                                                                        |

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

## Measured baselines

Record results here when `perf-check` produces them, with the hardware, the
build profile, and the exact `loadgen` invocation — a number without its
conditions is not a baseline.

All runs below: Apple M4 Pro, 14 cores, `--release`, gateway and load generator
on the same machine.

| Date       | Commit           | Invocation                                               | Throughput  | p50      | p99      | Warnings   |
|------------|------------------|----------------------------------------------------------|-------------|----------|----------|------------|
| 2026-08-01 | `166f68b` + docs | `--connections 200 --senders 5 --rate 50 --seconds 10`   | 62k msg/s   | 3.27 ms  | 5.40 ms  | 0          |
| 2026-08-01 | `166f68b` + docs | `--connections 300 --senders 30 --rate 400 --seconds 10` | 1.40M msg/s | 36.85 ms | 87.37 ms | 391,873    |

The first run is the clean baseline: 495,510 deliveries received against 495,510
expected — complete fanout, nothing dropped. Idle server RSS was 31.5 MiB with
200 connections open.

The second run is past the cliff and is **not** a valid throughput figure. Only
11.2M of 35.7M expected deliveries arrived; the rest were dropped as consumers
fell behind. It is recorded because the shape of the failure is the useful part:
throughput and latency both look like numbers, and only the warning count
reveals that a third of the traffic never made it. Note also that the load
generator shares the machine, so some of that backpressure is self-inflicted —
see `perf-check` on isolating the harness before drawing conclusions.

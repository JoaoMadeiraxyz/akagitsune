# Akagitsune

A generic realtime WebSocket gateway in Rust, built on axum and tokio. It
manages connections and relays messages between sockets — nothing else. The
gateway does not interpret message content: what a payload means is decided by
whoever uses the gateway, whether that is chat, notifications, game state, or
anything else.

**Performance goal:** sustain **1 000 000 deliveries/s** at **service p99
10–20 ms** with **≥ 99.9% delivery**. See
[`docs/architecture.md`](docs/architecture.md#performance-goal) for the bar,
current gap, and goal/stretch scenarios.

## Scope

> A feature belongs in this gateway only if it can be implemented **without
> knowing what the payload means**.

Routing, identity, presence, admission control and delivery semantics belong
here. Usernames, content validation, history and business logic belong in the
application consuming the gateway. See [`docs/scope.md`](docs/scope.md) for the
full boundary, including the borderline cases.

## Running

```bash
cargo run
# gateway listening on ws://127.0.0.1:3000/ws
```

| Variable       | Default                 | Description   |
| -------------- | ----------------------- | ------------- |
| `GATEWAY_ADDR` | `127.0.0.1:3000`        | Bind address  |
| `RUST_LOG`     | `realtime_gateway=info` | Log filter    |

`Ctrl-C` shuts down gracefully.

## Protocol

There is no handshake. On connect, the client receives the identity the gateway
assigned to it:

```json
{"type": "welcome", "id": "9f3c1a2e-..."}
```

Nothing is delivered until the client subscribes. A message published to a
topic reaches every other connection subscribed to it, and senders never receive
an echo of their own messages.

A topic is an opaque key of 1 to 255 bytes of UTF-8, compared byte for byte.
The gateway gives no meaning to its characters.

### Control frames from the client

```json
{"type": "subscribe", "topic": "t"}
{"type": "unsubscribe", "topic": "t"}
{"type": "publish", "topic": "t", "data": {"hp": 42, "pos": [1, 2]}}
```

`subscribe` and `unsubscribe` are idempotent and always answered. A connection
holds at most 64 topics. `data` may be any JSON value, including `null`. It is
validated and forwarded untouched, without being deserialized. The gateway parses
its own control frame, never `data`. Unknown extra fields are ignored.

The gateway answers:

```json
{"type": "subscribed", "topic": "t"}
{"type": "unsubscribed", "topic": "t"}
```

`subscribed` is sent after the connection is registered, so any publish made
after another connection received it reaches that connection. A frame for the
topic can arrive before `subscribed` if a publish raced the subscription.
`unsubscribed` is sent after the connection is removed, but frames already being
delivered when it happened can still arrive, so filter on `topic` if that matters.

Subscribers receive:

```json
{"type": "message", "topic": "t", "from": "9f3c1a2e-...", "data": {"hp": 42, "pos": [1, 2]}}
```

The envelope exists so receivers know the topic and the origin, and so the
gateway can signal errors and message loss on the same stream without colliding
with the payload. A sender's frames reach a receiver in send order.

### Binary frames

The payload is opaque bytes. A client sends `[len: u8][topic: len bytes][payload]`
and subscribers receive `[len: u8][topic][sender uuid: 16 bytes][payload]`, where
the uuid is in RFC 4122 byte order. The payload may be empty.

### Other frames

```json
{"type": "warning", "dropped": 12}
{"type": "error", "topic": "t", "message": "text frames must be subscribe, unsubscribe or publish control frames"}
```

`warning` means the client did not drain its socket fast enough and the gateway
discarded `dropped` messages destined for it. Delivery is best-effort by design;
see [`docs/decisions.md`](docs/decisions.md).

All of a connection's topics share one inbox. Under lag a busy topic can push out
the frames of a quiet one, and `warning` reports only a total for the connection,
not which topics lost frames. A topic that must be isolated goes on its own
connection.

`error` always carries `topic`: the topic of the frame that failed, after JSON
unescaping, or `null` when no topic could be read (the frame is not JSON, has no
string `topic`, or a binary header is truncated or not UTF-8). The connection
stays open after an error.

### Topic lifecycle

A topic has no existence of its own. It is the set of connections subscribed to a
key, and it exists exactly while that set is not empty.

- The first `subscribe` creates it. Removing the last subscriber, by
  `unsubscribe` or disconnect, deletes it.
- A `publish` never creates a topic. Publishing to a key nobody is subscribed to
  is discarded, and the publisher is not told.
- Nothing is retained: a subscriber receives only what is published after it
  subscribed, and nothing survives an empty period.
- Topics have no owner and no namespace. Two applications that pick the same key
  share one topic, so prefix keys per application, for example `app-a/...`. The
  gateway does not enforce it.
- There is no access control. Any connection can subscribe to or publish on any
  key it can guess, so keys are not secrets.

### Limits

Frames larger than 64 KiB, header included, are rejected at the WebSocket layer:
the sender's connection is dropped without a close frame, and nothing is relayed.
A connection holds at most 64 subscriptions.

## Layout

| Path                  | Responsibility                      |
| --------------------- | ----------------------------------- |
| `src/protocol.rs`     | Frames the gateway emits            |
| `src/registry.rs`     | Topic registry and subscriptions    |
| `src/state.rs`        | Shared state                        |
| `src/ws.rs`           | Connection lifecycle and tasks      |
| `src/lib.rs`          | Router and `run`                    |
| `src/main.rs`         | Config, logging and shutdown        |
| `tests/gateway.rs`    | End-to-end integration tests        |
| `examples/loadgen.rs` | Load generator                      |
| `examples/refserver.rs`| Reference server for calibration   |
| `scripts/bench.sh`    | Benchmark sweep                     |
| `scripts/calibrate.sh`| Harness self-check                  |

Each connection runs two tasks: one reading from the socket and one writing to
it. The first to finish tears down the other. The sender serializes the envelope
once and each subscriber only clones a refcounted buffer.

## Documentation

- [`docs/scope.md`](docs/scope.md) — what belongs here and what does not
- [`docs/architecture.md`](docs/architecture.md) — lifecycle, hot-path
  invariants, scalability limits
- [`docs/decisions.md`](docs/decisions.md) — why the design is the way it is

## Development

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

Measuring performance:

```bash
scripts/bench.sh --quick     # calibrate the harness, then one fanout run
scripts/bench.sh             # baseline sweep, writes bench-results/<timestamp>.csv
scripts/bench.sh --goal      # 1M goal + stretch scenarios and a pass/miss verdict
scripts/bench.sh --all       # baseline + goal + stretch, with verdict
scripts/calibrate.sh         # harness self-check against a known answer
```

`bench.sh` refuses to produce numbers if the calibration fails.
See `docs/architecture.md` for the performance goal, current
baselines, and how to read them.

## Not implemented yet

There is no authentication, no per-connection rate limiting, no access control
on topics, and no backplane for running more than one instance.

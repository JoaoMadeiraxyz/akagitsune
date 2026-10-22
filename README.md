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

| Variable              | Default                    | Description                     |
| --------------------- | -------------------------- | ------------------------------- |
| `GATEWAY_ADDR`        | `127.0.0.1:3000`           | Bind address                    |
| `GATEWAY_AUTH_TOKENS` | unset (authentication off) | Comma-separated accepted tokens |
| `RUST_LOG`            | `realtime_gateway=info`    | Log filter                      |

`Ctrl-C` shuts down gracefully.

### Authentication

With `GATEWAY_AUTH_TOKENS` unset or empty, any client can connect, and the
gateway logs `authentication is off: any client can connect` at startup.
Exposing it beyond a trusted network in that state is the operator's decision.

```bash
GATEWAY_AUTH_TOKENS=token-a,token-b cargo run
```

Each token is 1 to 128 bytes of letters, digits and ``!#$%&'*+-.^_`|~``. An
invalid entry stops startup with an error naming its position. A client
presents a token in one of two places, and the header wins if both are sent:

- `Authorization: Bearer <token>`
- `Sec-WebSocket-Protocol: bearer, <token>`, for browsers, which cannot set
  headers on a WebSocket. The gateway answers with `Sec-WebSocket-Protocol:
  bearer` and never echoes the token.

```js
const socket = new WebSocket("ws://127.0.0.1:3000/ws", ["bearer", "token-a"]);
```

A missing, malformed or wrong token gets `401` with `WWW-Authenticate: Bearer`
and an empty body, before any WebSocket exists. Each refused request logs one
`warn` line with the peer address and no credential. An operator who does not
want those lines can raise the log level, for example
`RUST_LOG=realtime_gateway=error`. A token in the query string is not accepted,
so a client that can set neither form cannot authenticate. The token is checked
once, at connect: removing it from the configuration does not close connections
already open. A token identifies a deployment, not a client, and does not limit
which topics a connection can use.

The gateway does not terminate TLS. A bearer token over plain `ws://` is
readable on the wire, so put the gateway behind a proxy that serves `wss://`.

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

### Delaying a publish

A text `publish` can carry `delay_ms`, a whole number of milliseconds from 0 to
120 000 (120 s):

```json
{"type": "publish", "topic": "t", "data": {"hp": 42}, "delay_ms": 5000}
```

The gateway waits that long before delivering the publish, counting from the
moment it becomes the next frame the gateway processes for the connection. A
publish without `delay_ms`, or with `0`, is delivered immediately. A connection's
frames are processed in order, so for three frames sent back to back:

```
msg1 -> delay 5 s
msg2 -> no delay
msg3 -> delay 6 s
```

msg1 is delivered 5 s after it was sent, msg2 right after msg1, and msg3 6 s after
msg2.

A `delay_ms` from 120 001 up to 18 446 744 073 709 551 615 is answered with an
`error` frame naming the topic, the publish is not delivered, and the connection
stays open. Only a JSON integer literal is accepted. A `delay_ms` that is
negative, fractional, written as `1e3`, `1.0` or `-0`, `null`, not a number, or an
integer too large for 64 bits makes the whole frame an invalid frame, on any frame
type. On `subscribe` and `unsubscribe`, a well-formed `delay_ms` is ignored, whatever
its value, like any other unknown field.

The delay is served by the connection's own reader, not by a queue, and that has
consequences a client has to plan for:

- **Text publishes only.** The binary header has no field for a delay, and
  extending it is a second wire format. A client that wants paced binary frames
  waits on its own side.
- **The connection's other text frames wait.** While a publish waits, the
  reader does not process a later subscribe, unsubscribe or publish, nor
  application-level pings sent as text. They are processed, in order, after the
  wait. The reader holds one text or binary frame and stops reading the socket, so
  TCP backpressure slows a client that keeps writing, and the gateway never
  holds more than two frames for the connection.
- **Protocol pings are answered.** A `Ping` frame during the wait gets its `Pong`
  at once.
- **A waiting publish cannot be cancelled.** Only disconnecting drops it. There
  is no cancel frame, because the reader is not processing frames while it waits.
- **A waiting publish is lost if the connection drops before another frame is
  held.** A `Close` frame, the end of the stream or a socket error during the
  wait drops the publish. There is no persistence and no replay, and a new
  connection starts with nothing. A peer that vanishes without a FIN or RST is
  noticed only when TCP notices it, as without delays. An application that needs
  durable scheduling runs a service beside the gateway that stores the publishes
  and sends them at the due time.
- **A disconnect behind a held frame is seen late.** Only one frame is read
  ahead, and once it is held the socket is not read until the wait ends. A
  `Close`, end of stream or socket error that arrives after a held frame is
  therefore seen only when the wait ends: the connection keeps its slot and
  memory until then, up to 120 s, and the delayed publish and the held frame are
  both delivered before the reader notices the disconnect.
- **The delay is a minimum.** Each wait overshoots the requested delay by about
  1 ms (1.1 ms p50 in a load test), and chained delays accumulate it: about
  0.2 s after 400 chained publishes.
- **An older gateway ignores `delay_ms` silently.** It publishes immediately, and
  the frames alone do not tell the client its pacing was not applied. Check the
  gateway version.

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
- There is no access control on topics, and none is planned. Any admitted
  connection can subscribe to or publish on any key it can guess, so keys are
  not secrets. Only let clients you trust reach the gateway.

### Limits

Frames larger than 64 KiB, header included, are rejected at the WebSocket layer:
the sender's connection is dropped without a close frame, and nothing is relayed.
A connection holds at most 64 subscriptions.

## Layout

| Path                    | Responsibility                              |
| ----------------------- | ------------------------------------------- |
| `src/auth.rs`           | Token set and constant-time admission check |
| `src/protocol.rs`       | Frames the gateway emits                    |
| `src/registry.rs`       | Topic registry and subscriptions            |
| `src/state.rs`          | Shared state                                |
| `src/ws.rs`             | Connection lifecycle and tasks              |
| `src/lib.rs`            | Router and `run`                            |
| `src/main.rs`           | Config, logging and shutdown                |
| `tests/gateway.rs`      | End-to-end integration tests                |
| `examples/loadgen.rs`   | Load generator                              |
| `examples/refserver.rs` | Reference server for calibration            |
| `scripts/bench.sh`      | Benchmark sweep                             |
| `scripts/calibrate.sh`  | Harness self-check                          |

Each connection runs two tasks: one reading from the socket and one writing to
it. The first to finish tears down the other. The sender serializes the envelope
once and each subscriber only clones a refcounted buffer.

## Documentation

- [`docs/scope.md`](docs/scope.md) — what belongs here and what does not
- [`docs/architecture.md`](docs/architecture.md) — lifecycle, hot-path
  invariants, scalability limits
- [`docs/decisions.md`](docs/decisions.md) — why the design is the way it is
- [`docs/roadmap.md`](docs/roadmap.md) — what is done and what is planned next,
  in order

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

There is no per-connection rate limiting, no access control
on topics, and no backplane for running more than one instance.

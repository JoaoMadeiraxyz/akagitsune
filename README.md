# realtime-gateway

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

From then on, everything the client sends is relayed to every other connection.
Senders do not receive an echo of their own messages.

### Text frames

The payload must be valid JSON — any JSON: object, array, number, string or
`null`. It is validated and forwarded untouched, without being deserialized.

A client sends:

```json
{"hp": 42, "pos": [1, 2]}
```

Everyone else receives:

```json
{"type": "message", "from": "9f3c1a2e-...", "data": {"hp": 42, "pos": [1, 2]}}
```

The envelope exists so receivers know the origin and so the gateway can signal
errors and message loss on the same stream without colliding with the payload.

### Binary frames

Relayed byte for byte, with no envelope. There is no ambiguity with the JSON
above because they are distinct WebSocket frame types. The trade-off is that
receivers do not learn the origin — use text frames if you need it, or carry it
in your own framing.

### Control frames

```json
{"type": "warning", "dropped": 12}
{"type": "error", "message": "text frames must contain valid JSON; use binary frames otherwise"}
```

`warning` means the client did not drain its socket fast enough and the gateway
discarded `dropped` messages destined for it. Delivery is best-effort by design;
see [`docs/decisions.md`](docs/decisions.md).

### Limits

Frames larger than 64 KiB are rejected at the WebSocket layer.

## Layout

| Path                  | Responsibility                      |
| --------------------- | ----------------------------------- |
| `src/protocol.rs`     | Frames the gateway emits            |
| `src/state.rs`        | Shared state and the broadcast bus  |
| `src/ws.rs`           | Connection lifecycle and tasks      |
| `src/lib.rs`          | Router and `run`                    |
| `src/main.rs`         | Config, logging and shutdown        |
| `tests/gateway.rs`    | End-to-end integration tests        |
| `examples/loadgen.rs` | Load generator                      |
| `examples/refserver.rs`| Reference server for calibration   |
| `scripts/bench.sh`    | Benchmark sweep                     |
| `scripts/calibrate.sh`| Harness self-check                  |

Each connection runs three tasks: one reading from the socket, one bridging the
bus, one writing to the socket. The first to finish tears down the others. The
sender serializes the envelope once and each subscriber only clones a refcounted
buffer.

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

`bench.sh` refuses to produce numbers if the calibration fails. See
`docs/architecture.md` for the performance goal, current baselines, and how to
read them.

## Not implemented yet

Topic or room routing — today there is a single bus and every connection
receives everything, which makes fanout O(N²). There is also no authentication,
no per-connection rate limiting, and no backplane for running more than one
instance.

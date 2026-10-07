# realtime-gateway

A **generic** realtime WebSocket gateway. It manages connections and relays
messages between sockets. Nothing else. The gateway never learns what a message
means — the purpose is defined entirely by whoever uses it: chat, notifications,
multiplayer game state, telemetry, anything.

**Performance goal:** 1M deliveries/s, service p99 10–20 ms, ≥ 99.9% delivery.
Measure with `scripts/bench.sh --goal`. Details in `docs/architecture.md`.

## The scope test

> A feature belongs in this gateway only if it can be implemented **without
> knowing what the payload means**.

If implementing something requires reading inside `data`, it belongs in the
application consuming the gateway, not here. This project already drifted once
into being a chat server — see `docs/scope.md` before adding anything.

When a change is borderline, **stop and ask the user**. Do not decide silently.

## Hard rules

- **Never deserialize the payload.** The control frame is parsed, and `data` is
  validated as JSON via `&RawValue` and embedded verbatim. Never `Value`, never
  a typed struct for `data`, never a field lookup inside it.
- **Never introduce domain vocabulary.** No `username`, `room name`, `chat`,
  `notification`, `player`. Connections have UUIDs; that is the only identity.
- **No comments in code.** Not doc comments, not inline. Rationale goes in
  `docs/decisions.md`, the commit message, or the reply to the user. Names carry
  intent instead.
- **Serialize once.** The sender builds the envelope one time; subscribers only
  clone a refcounted buffer. Never serialize per receiver.
- **Gateway code uses no locks. Short internal locks inside tokio's channels and timers are allowed, and are never held across `.await`.**
  Shared state is the `AtomicUsize` and the lock-free topic registry.
- **Every protocol behavior gets an integration test** in `tests/gateway.rs`.
- **`clippy -D warnings` must be clean**, including `--all-targets`.

## Commands

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo run --release --example loadgen -- --help
scripts/bench.sh --quick
scripts/bench.sh --goal
scripts/bench.sh --all
```

## Layout

| Path                 | Responsibility                       |
| -------------------- | ------------------------------------ |
| `src/protocol.rs`    | Control frames and binary header     |
| `src/registry.rs`    | Topic registry and subscriptions     |
| `src/state.rs`       | Shared state                         |
| `src/ws.rs`          | Connection lifecycle and tasks       |
| `src/lib.rs`         | Router and `run`                     |
| `src/main.rs`        | Config, logging, shutdown            |
| `tests/gateway.rs`   | End-to-end integration tests         |
| `examples/loadgen.rs`| Load generator for perf measurement  |

## Documentation

- `docs/scope.md` — what belongs here and what does not, with borderline cases
- `docs/architecture.md` — lifecycle, hot-path invariants, scalability limits
- `docs/decisions.md` — why the design is the way it is

## Skills

- `scope-guard` — run before adding any feature, route, protocol field or dependency
- `gateway-review` — run before committing
- `perf-check` — run when touching the hot path or making a performance claim

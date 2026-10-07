# Proposal

## Why

Clients that publish a large burst of frames often need them to leave at a natural pace instead of all at once, with a different gap before each frame. Today the only way is for the client to run its own timers, which keeps the pacing logic outside the gateway and gives the gateway no say in how fast a connection's frames enter the topic.

The gateway can offer this without learning anything about the payload: a publish carries a delay, and the connection's reader waits that long before fanning the frame out.

## Scope verdict

**Borderline, lighter than first assessed, approved by the project owner.** `docs/scope.md` lists persistence of any kind, even a bounded queue, as borderline. This change does not retain a queue: the delay is served by the reader itself, so the only message held is the one currently being processed, plus at most one frame read ahead to detect a disconnect. No `data` is read, no domain noun appears (`delay_ms` names what the gateway knows, which is a duration), and nothing survives the connection.

It is still more than a drive-by change, so `docs/decisions.md` entry 17 and a row in the borderline table of `docs/scope.md` land with this proposal. Three things make it a decision and not a detail:

- The control frame gains an optional field, `delay_ms`, on `publish`.
- While a delayed publish waits, the connection's other inbound frames (subscribe, unsubscribe, a later publish) wait too. This is a new failure mode for the connection, stated in the client documentation.
- The hard rule on locks is reworded so that it covers tokio's timer, which takes an internal mutex like tokio's channels do (`design.md` decision 6).

Persistence across connections was considered and rejected by the project owner: everything that belongs to a connection dies with it (`design.md` decision 5).

## What Changes

- A `publish` text frame accepts an optional `delay_ms`, a non-negative integer number of milliseconds, at most `MAX_DELAY_MS` (60 000).
- The reader processes a connection's frames in order. A publish with `delay_ms > 0` waits that long before it is fanned out, counting from the moment it becomes the next frame to process. A publish with no delay is fanned out immediately.
- While waiting, the reader does not read ahead more than one data frame. That frame is held and processed right after the delayed publish. The socket is not read again until the wait ends, so TCP backpressure slows a client that keeps writing, and the gateway holds at most two frames per connection for this feature.
- If the connection closes while a publish is waiting, whether by a `Close` frame, end of stream or a socket error seen during the wait, the publish is dropped and never delivered.
- `delay_ms` above the cap is rejected with an `error` frame naming the topic, before any waiting, and the connection stays open.
- `delay_ms` on `subscribe` and `unsubscribe` is ignored, like every other unknown field.
- A non-integer or negative `delay_ms` makes the whole frame an invalid frame.
- Binary publishes cannot carry a delay in this version. The binary header has no field for it, and extending it is a second-wire-format decision left out on purpose.
- Documentation: `docs/decisions.md` entry 17, a row in `docs/scope.md`, the lock rule in `CLAUDE.md` and `docs/architecture.md`, and (with the implementation) a client-facing pacing section in `README.md` and `docs/architecture.md`.

## Capabilities

### New Capabilities

- `publish-pacing`: the `delay_ms` field, ordering, what a delayed publish holds, disconnect behavior, limits, and what the client can and cannot do while waiting.

### Modified Capabilities

- `message-relay`: the rule that fields other than `type`, `topic` and `data` are ignored gains the exception of `delay_ms` on `publish`.

## Impact

- `src/protocol.rs`: `ClientFrame.delay_ms`, `MAX_DELAY_MS`.
- `src/ws.rs`: `handle_text` is split into a parse step and an execute step, and `read_loop` waits before fanout (about 90 net lines in the spike).
- `tests/gateway.rs`: one integration test per scenario in `specs/publish-pacing/spec.md`.
- No new dependency. `tokio`'s `time` feature is already enabled.
- Wire compatibility: additive. A gateway without this change ignores `delay_ms` and publishes immediately, so a client cannot tell from the frames alone that its pacing was not applied. The client documentation says to check the gateway version.
- Performance: the no-delay path adds one comparison. The spike measured `scripts/bench.sh --quick` once per side, with p50 3.13 ms before and 3.45 ms after, and p99 8.22 ms before and 8.86 ms after. One sample per side cannot separate noise from cost, so the implementation PR must repeat the comparison (`tasks.md` 4.2) before the lookahead is kept.

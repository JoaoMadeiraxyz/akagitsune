# Tasks

Implementation starts only after connect-time authentication and subscription authorization land. This change is the proposal and its documentation.

## 1. Protocol

- [ ] 1.1 Add `delay_ms: Option<u64>` to `ClientFrame` in `src/protocol.rs` and `MAX_DELAY_MS = 60_000`. A negative or non-integer value must fail the parse, which makes the frame invalid
- [ ] 1.2 Add `INVALID_DELAY` to `src/ws.rs`, with no domain vocabulary
- [ ] 1.3 Add a unit test in `src/protocol.rs` that `delay_ms` is read as a number, is absent when not given, and that `-1`, `"5"` and `1.5` are rejected

## 2. Connection

- [ ] 2.1 Split `handle_text` into a parse step and an execute step so the delay is known before fanout. The parse step returns either a reply or a publish with its topic, data and delay
- [ ] 2.2 In `read_loop`, hold `Option<Message>` for the one frame read ahead and process it before reading the stream again
- [ ] 2.3 Add `wait_out`: select between the timer and `stream.next()` guarded by `ahead.is_none()`, skip `Ping` and `Pong`, hold the first `Text` or `Binary`, and return `Disconnected` on `Close`, end of stream or error. A `Disconnected` result ends the reader without fanning out
- [ ] 2.4 Reject `delay_ms > MAX_DELAY_MS` before waiting. Ignore `delay_ms` on `subscribe` and `unsubscribe`. Binary publishes never wait

## 3. Tests

- [ ] 3.1 One integration test in `tests/gateway.rs` per scenario in `specs/publish-pacing/spec.md`, using `tokio-tungstenite` against a real listener. Tests of timing use scaled-down delays (hundreds of milliseconds) and assert lower bounds and ordering, not tight upper bounds, so they do not flake on a loaded runner
- [ ] 3.2 Add the "unknown fields are still ignored" scenario from `specs/message-relay/spec.md`
- [ ] 3.3 Run the two disconnect tests against a mutant where `wait_out` ignores the stream, and confirm both fail. Without this they could pass for the wrong reason

## 4. Verification

- [ ] 4.1 Run `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings` and `cargo test`. CI runs the same on the PR but nothing blocks a merge on red, so the reviewer confirms the run is green
- [ ] 4.2 Compare `scripts/bench.sh --quick` between `main` and the branch: at least three runs per side, alternating, reporting every number. The lookahead stays only if the branch's p50 and p99 fall inside the run-to-run spread of `main`. Otherwise reopen `design.md` decision 3 with the numbers
- [ ] 4.3 Run `scripts/bench.sh --goal` once on the branch and put the numbers in the PR, as `openspec/config.yaml` requires for changes to `src/ws.rs`
- [ ] 4.4 Use the `perf-check` skill for any performance claim in the PR description

## 5. Documentation

- [ ] 5.1 `README.md`: a "Delaying a publish" section in the protocol part with the `delay_ms` field, the msg1/msg2/msg3 example, the maximum, and the limitations below, each with its reason
- [ ] 5.2 `docs/architecture.md`: the reader's wait in "The two tasks", the new row in "Scalability limits", and the updated lock invariant
- [ ] 5.3 State in the client documentation, plainly: text publishes only; the connection's other text frames wait while a publish waits; protocol pings are answered; a client cannot cancel a waiting publish except by disconnecting; a waiting publish is lost if the connection drops, with no persistence and no replay; one frame read ahead means a `Close` sent behind another frame is seen late; an older gateway ignores `delay_ms` silently
- [ ] 5.4 Remove "Not implemented yet" entries this change covers, if any
- [ ] 5.5 Archive the change and merge the deltas into `openspec/specs/` after verification

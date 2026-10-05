# Tasks

## 1. Protocol

- [ ] 1.1 Add `ClientFrame` (`subscribe`, `unsubscribe`, `publish` with `data: &RawValue`) to `src/protocol.rs`, with `topic` as `Cow<str>` and unknown fields ignored
- [ ] 1.2 Add `Subscribed { topic }`, `Unsubscribed { topic }` and `topic` on `Message` to `ServerMessage`, keeping the field order `type, topic, from, data`
- [ ] 1.3 Add the binary header: parse `[len][topic][payload]`, and build `[len][topic][uuid][payload]` once into a `Bytes`
- [ ] 1.4 Add `MAX_TOPIC_LEN = 255` and the `INVALID_FRAME`, `INVALID_TOPIC`, `INVALID_BINARY` and `SUBSCRIPTION_LIMIT` messages, with no domain vocabulary

## 2. Registry

- [ ] 2.1 Add the `papaya` dependency. Confirm it can update an entry atomically and remove it when it empties. If it cannot, stop and revise `design.md` decision 2 before going on
- [ ] 2.2 Create `src/registry.rs` with `Subscriber { id, queue, dropped }`, `TopicRegistry` (topic → `Arc<[Subscriber]>`, copy-on-write subscribe/unsubscribe, remove on empty) and a lock-free fanout function using `try_reserve` and `dropped.swap` as in `design.md` decision 4
- [ ] 2.3 Add `Subscriptions`: at most `MAX_SUBSCRIPTIONS_PER_CONNECTION = 64` topics per connection, with a `Drop` that unsubscribes from all of them
- [ ] 2.4 Replace the bus in `src/state.rs`: `AppState` holds the registry and the `AtomicUsize`. Remove `BroadcastMessage` and `BROADCAST_CAPACITY`

## 3. Connection

- [ ] 3.1 In `src/ws.rs`, remove the bridge task and leave two tasks (reader, writer) under the same `select!` teardown
- [ ] 3.2 Reader: dispatch `ClientFrame` and binary frames. Insert into the registry before queueing `subscribed`. Fan out publishes, skipping the connection's own id
- [ ] 3.3 Writer: the queue carries `Outbound { dropped_before, frame }` with capacity `SUBSCRIBER_QUEUE_CAPACITY = 256`. Emit `warning` before any frame whose `dropped_before > 0`. Keep batched flushes
- [ ] 3.4 Keep the lag total in the disconnect log line, with no per-message logging

## 4. Tests

- [ ] 4.1 Rewrite every existing test in `tests/gateway.rs` that relied on the global bus so it subscribes first and publishes with the new frames, following the MODIFIED scenarios in the deltas
- [ ] 4.2 Add one test per new scenario in `specs/topic-routing/spec.md` and the ADDED requirements in `specs/message-relay/spec.md`
- [ ] 4.3 Add a concurrency test: several connections subscribing and unsubscribing in a loop while one publishes to the same topic. A steadily subscribed receiver gets every frame in order, and the gateway stays responsive
- [ ] 4.4 Replace every `planned` `Fonte:` and `Teste:` line in the deltas with `path:line` — `Symbol` references to the implemented code and tests

## 5. Harness

This section requires `prepara-harness-para-topicos` to be merged (see `design.md` decision 10).

- [ ] 5.1 Make `topics` the default protocol in `examples/loadgen.rs`, `examples/refserver.rs` and `scripts/bench.sh`. Delete the `legacy` protocol, its `null` rules and its runs in `scripts/calibrate.sh`
- [ ] 5.2 Run `scripts/calibrate.sh` and get every case green before measuring the gateway. If a case fails, fix the harness rather than widen a tolerance

## 6. Documentation

- [ ] 6.1 `README.md`: rewrite the Protocol section (subscribe, unsubscribe, publish, envelope with `topic`, binary header in and out, limits, the unsubscribe window). Add a *Topic lifecycle* subsection from `design.md` decision 12: when a topic exists, publish-to-empty discards, no retention, no owner or namespace (with a recommended key prefix per application), and no access control, so keys are not secrets. Update Layout with `src/registry.rs`, change "three tasks" to two, and remove topic routing from "Not implemented yet"
- [ ] 6.2 `docs/architecture.md`: add a *Topic lifecycle* section (the stage table from `design.md` decision 12, including the atomic remove-on-empty), update the connection lifecycle, the task table, the message flow diagram, hot-path invariants (payload rule wording, shared state now includes the lock-free registry), backpressure (queue instead of `Lagged`), the scalability table (fanout O(topic size), membership churn cost, single-task fanout per publish) and "What would have to change"
- [ ] 6.3 `docs/architecture.md`: move the global-bus *Measured baselines* rows (the `--protocol legacy` rows recorded by `prepara-harness-para-topicos`) and the *Status* figures into a subsection titled as the global-bus architecture, with their commit. Then record the new baselines from task 7.2 in the main table and rewrite *Status*. The per-connection RSS and the cliff reading are re-derived from the new rows, not carried over
- [ ] 6.4 `docs/decisions.md`: append entries 12–15 from `design.md` verbatim, without editing entries 3, 4, 7 or 8
- [ ] 6.5 `CLAUDE.md`: reword the payload hard rule (the control frame is parsed, `data` stays `&RawValue`) and the no-locks rule (shared state is the `AtomicUsize` and the lock-free registry), and add `src/registry.rs` to Layout
- [ ] 6.6 `openspec/config.yaml`: update the service description (topic routing instead of a single global bus) and the code map
- [ ] 6.7 `.claude/skills/gateway-review/SKILL.md` and `.claude/skills/perf-check/SKILL.md`: replace the bridge, `BroadcastMessage` and `BROADCAST_CAPACITY` references with the registry, fanout and `SUBSCRIBER_QUEUE_CAPACITY`. In `perf-check`, describe `warning` as a full subscriber queue and remove every mention of `--protocol legacy`
- [ ] 6.8 `docs/architecture.md`, *Performance goal*: add `goal-1m-topics` to the goal scenario table, and remove any mention of the `legacy` protocol

## 7. Verify

- [ ] 7.1 Author runs `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test` and `openspec validate --all --strict` locally before opening the PR
- [ ] 7.2 Author runs `scripts/calibrate.sh` and then `scripts/bench.sh --all` locally on the new code, and puts the table in the PR, including `goal-1m-topics`, next to the global-bus rows for the same `topics = 1` scenarios as the before/after of this change. CI does not measure performance
- [ ] 7.3 CI runs fmt, clippy and `cargo test` on the PR. `main` has no branch protection, so the reviewer confirms the run is green before merging
- [ ] 7.4 An independent session fills `verificacao.md`: each requirement against its code and test with `path:line` evidence, each hot-path invariant from `design.md` decision 9 checked in the diff, and a grep of the diff for domain vocabulary and code comments

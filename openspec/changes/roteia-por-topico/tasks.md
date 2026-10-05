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

The harness can land before section 3, because it only needs `refserver`. See `design.md` decision 10.

- [ ] 5.1 `examples/loadgen.rs`, topology:
  - add `--topics T`, with connection `i` on `t-(i mod T)`, rejecting `connections % T != 0`;
  - make each sender publish to its own topic;
  - subscribe and await `subscribed` before the clock, counting `subscribe_failed`;
  - compute expected deliveries per topic from the acknowledged members, replacing `sent × (established − 1)`
- [ ] 5.2 `examples/loadgen.rs`, wire format:
  - publish `{"type":"publish","topic":…,"data":{stamp}}`;
  - add `--binary`, sending `[len][topic][stamp][padding]`;
  - read the envelope's `topic` and the binary header on the receive path without building a `Value`
- [ ] 5.3 `examples/loadgen.rs`, meters:
  - add `misrouted`, `dropped` (the sum of every `warning.dropped`), `unaccounted` (whole-run expected − received − dropped), and `subscribe_ack_p50_ms` / `subscribe_ack_p99_ms`;
  - add all of them to `--json`;
  - extend `USAGE` to describe them
- [ ] 5.4 `examples/loadgen.rs`, churn: add `--churn N`, with `N` extra connections alternating subscribe/unsubscribe on `t-0` once per second during the measured window, excluded from the expected deliveries
- [ ] 5.5 `examples/loadgen.rs`: add unit tests for the expected-delivery arithmetic, including uneven acknowledgements and the `T = 1` reduction to the old formula (`cargo test --examples`)
- [ ] 5.6 `examples/refserver.rs`:
  - speak the new protocol, including acknowledgements and the binary header, keeping its single broadcast bus with a per-connection topic filter so it stays independent from the gateway's registry;
  - add `--warn-drops` and `--ignore-topics`;
  - keep delay, silent loss and freeze unchanged
- [ ] 5.7 `scripts/calibrate.sh`: keep cases 1–4 at `T = 1` with their current predicted answers, and add cases 5–10 from `design.md` decision 10 (topics, the misrouting meter, silent loss, reported loss, binary, churn exclusion). Every predicted value must come from arithmetic
- [ ] 5.8 `scripts/bench.sh`:
  - change the scenario tuple to `name connections topics senders rate payload seconds [flags]`;
  - add the `topics`, `binary`, `churn`, `subscribe_failed`, `subscribe_ack_p99_ms`, `misrouted`, `dropped` and `unaccounted` columns;
  - change `offered()` to `senders × rate × (conns / topics − 1)`;
  - add the `incorrect` verdict, which overrides every other verdict;
  - compute RSS per connection over every open connection;
  - print the full invocation in the markdown table
- [ ] 5.9 `scripts/bench.sh`: add the `topics-1k`, `topics-5k`, `binary-200` and `churn-500` scenarios to the baseline sweep and `goal-1m-topics` to the goal set, with the invocations from `design.md` decision 10
- [ ] 5.10 Run `scripts/calibrate.sh` and get every case green before section 3 is measured. If a case fails, fix the harness rather than widen a tolerance

## 6. Documentation

- [ ] 6.1 `README.md`: rewrite the Protocol section (subscribe, unsubscribe, publish, envelope with `topic`, binary header in and out, limits, the unsubscribe window). Update Layout with `src/registry.rs`, change "three tasks" to two, and remove topic routing from "Not implemented yet"
- [ ] 6.2 `docs/architecture.md`: update the lifecycle, the task table, the message flow diagram, hot-path invariants (payload rule wording, shared state now includes the lock-free registry), backpressure (queue instead of `Lagged`), the scalability table (fanout O(topic size), membership churn cost, single-task fanout per publish) and "What would have to change"
- [ ] 6.3 `docs/architecture.md`: move every existing *Measured baselines* row and the *Status* figures into a subsection titled as the global-bus architecture, with commit `334cf1d`. Then record the new baselines from task 7.2 in the main table and rewrite *Status*. The per-connection RSS and the cliff reading are re-derived from the new rows, not carried over
- [ ] 6.4 `docs/decisions.md`: append entries 11–14 from `design.md` verbatim, without editing entries 3, 4, 7 or 8
- [ ] 6.5 `CLAUDE.md`: reword the payload hard rule (the control frame is parsed, `data` stays `&RawValue`) and the no-locks rule (shared state is the `AtomicUsize` and the lock-free registry), and add `src/registry.rs` to Layout
- [ ] 6.6 `openspec/config.yaml`: update the service description (topic routing instead of a single global bus) and the code map
- [ ] 6.7 `.claude/skills/gateway-review/SKILL.md` and `.claude/skills/perf-check/SKILL.md`: replace the bridge, `BroadcastMessage` and `BROADCAST_CAPACITY` references with the registry, fanout and `SUBSCRIBER_QUEUE_CAPACITY`. In `perf-check`, update *Reading the output* and *Methodology*:
  - the delivery formula with topics;
  - `warning` as a full subscriber queue;
  - the `misrouted`, `dropped`, `unaccounted` and `subscribe_ack` meters;
  - the `incorrect` verdict;
  - the new scenarios
- [ ] 6.8 `docs/architecture.md`, *Performance goal* and *Harness calibration*:
  - generalize the definition of a delivery to `senders × rate × (connections / topics − 1)`;
  - add `goal-1m-topics` to the goal table;
  - add calibration cases 5–10 with their predicted and measured values

## 7. Verify

- [ ] 7.1 Author runs `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test` and `openspec validate --all --strict` locally before opening the PR
- [ ] 7.2 Author runs `scripts/calibrate.sh` and then `scripts/bench.sh --all` locally on the new code, and puts the table in the PR, including `goal-1m-topics`. Old rows may appear only as a labelled before/after illustration, not as the baseline. CI does not measure performance
- [ ] 7.3 CI runs fmt, clippy and `cargo test` on the PR. `main` has no branch protection, so the reviewer confirms the run is green before merging
- [ ] 7.4 An independent session fills `verificacao.md`: each requirement against its code and test with `path:line` evidence, each hot-path invariant from `design.md` decision 9 checked in the diff, and a grep of the diff for domain vocabulary and code comments

# Verification

Independent pass over branch `spec/implementa-roteia-por-topico` (PR [akagitsune#33](https://github.com/JoaoMadeiraxyz/akagitsune/pull/33)) against `specs/`, `design.md` decision 9 and the hard rules in `CLAUDE.md`. Every `Fonte`/`Teste` line number cited in the deltas was re-read and resolves to the named symbol. Benchmarks were not run.

Legend: met / partial / not met. All tests are in `tests/gateway.rs`.

## 1. Requirements and scenarios

### connection-lifecycle

| Requirement / scenario | Code | Test | Status |
|---|---|---|---|
| Connecting is joining: new connection receives its identity | `src/ws.rs:54-62` welcome with `Uuid::new_v4`; `src/protocol.rs:15` | `:114` connection_is_welcomed_with_an_id | met |
| Assigned identity is the sender of relayed frames | `src/ws.rs:211` (`from`), `src/protocol.rs:80-86` (sender bytes) | `:164` payload_is_relayed_verbatim_to_others | met |
| New connection has no subscriptions | `src/registry.rs:98` (empty set); registry only filled by `src/ws.rs:198` | `:569`, `:557` | met |
| Frames above 64 KiB: frame at the limit is relayed | `src/ws.rs:26`, `:40` | `:121` frame_at_size_limit_is_relayed | met |
| Oversized frame drops the sender, nothing relayed | `src/ws.rs:40` (`max_message_size`), `:170` loop ends on `Err` | `:140` oversized_frame_drops_the_sender_without_relaying | met |

### delivery-backpressure

| Requirement / scenario | Code | Test | Status |
|---|---|---|---|
| Lagging connections warned: slow consumer gets warning | `src/ws.rs:27`, `:68` (256-slot inbox), `:140` Lagged to `Warning` `:110` | `:313` | met |
| Delivery resumes after a warning | `src/ws.rs:137-157` | `:333` | met |
| Dropped count is exact | `src/ws.rs:108-110`, `:140`, `:149` (count passed through unchanged) | `:371` | met |
| Own frames not counted in `n` | `src/registry.rs:68` filters the sender out before any inbox send | `:371` (A publishes, B counts) | met |
| Publishing never waits: other connections keep receiving | `src/registry.rs:63-77` synchronous `send`, no await | `:418` | met |

### message-relay

| Requirement / scenario | Code | Test | Status |
|---|---|---|---|
| Envelope: object payload | `src/ws.rs:210-212`, `src/protocol.rs:24-28` | `:164` | met |
| Envelope: payload bytes kept exactly | `src/protocol.rs:45` `&RawValue` | `:178` | met |
| Envelope: surrounding whitespace dropped | `src/protocol.rs:45` (RawValue trims) | `:178` | met |
| Any JSON value accepted | `src/protocol.rs:44-45`, `:48-53` (`null` is present) | `:199`; unit tests `src/protocol.rs:92-102` | met |
| Invalid text: error, no broadcast | `src/ws.rs:186-188` | `:453` | met |
| Bare JSON is not a publish | `src/ws.rs:186-188` (`type` required) | `:470` | met |
| Publish without data rejected, names topic | `src/ws.rs:207-209` | `:792` | met |
| Connection keeps working after an error | `src/ws.rs:170-181` loop continues | `:250` (publish after errors), `:453` | met |
| No echo, text | `src/registry.rs:68` | `:500` | met |
| No echo, binary | `src/registry.rs:68`, `src/ws.rs:221` | `:500` | met |
| Burst preserves order | single reader task, `src/ws.rs:164-183`; FIFO inbox | `:277` | met |
| Order holds across topics | one inbox per connection (`src/ws.rs:68`) | `:294` | met |
| Removed: binary passthrough | replaced by header requirement | `:212` | met |
| Binary payload arrives with topic and sender | `src/protocol.rs:80-86`, `src/ws.rs:218-222` | `:212` | met |
| Binary and text share topics, in order | same registry and inbox | `:236` (not cited in the delta, see non-blocking) | met |
| Truncated binary header rejected, topic null | `src/protocol.rs:72-74`, `src/ws.rs:224` | `:250` | met |
| Zero-length binary topic rejected, topic `""` | `src/protocol.rs:68-70` | `:250` | met |

### topic-routing

| Requirement / scenario | Code | Test | Status |
|---|---|---|---|
| Subscribe is acknowledged | `src/ws.rs:198-201` | `:516` | met |
| Duplicate subscribe delivers once | `src/registry.rs:33-35`, `:103-105` | `:523` | met |
| Only subscribers of the topic receive | `src/registry.rs:65-76` | `:537` | met |
| Publisher need not be subscribed | `src/ws.rs:206-213` (no membership check) | `:557` | met |
| Publish to empty topic is silent, connection open | `src/registry.rs:65-67`, `src/ws.rs:213` returns `None` | `:569` | met |
| Publish after `subscribed` is delivered | `src/ws.rs:198` registers before the ack is queued | `:580` | met |
| No delivery after unsubscribe | `src/ws.rs:203-204` removes before the ack is queued | `:597` | met |
| Unsubscribe without subscription acknowledged | `src/registry.rs:115` guard, `src/ws.rs:204` | `:616` | met |
| Disconnect removes from every topic | `src/registry.rs:129-135` (`Drop`) | `:623` | met |
| Publish before any subscription not retained | `src/registry.rs:65-67`; `publish` never inserts | `:643` | met |
| Emptied topic starts over | `src/registry.rs:54` `Operation::Remove` | `:660` | met |
| Topic emptied by disconnection starts over | `src/registry.rs:129-135`, `:54` | `:660` (second half, uses a 100 ms sleep) | met |
| Unrelated connections share a key | topic is the map key, `src/registry.rs:19` | `:690` | met |
| Topic outlives its first subscriber | `src/registry.rs:55-59` | `:690` | met |
| Topic at 255 bytes accepted | `src/protocol.rs:8`, `:55-57` | `:709` | met |
| Empty or oversized topic rejected, then still processed | `src/ws.rs:194-196` | `:709` | met |
| Escaped and literal forms are the same topic | `Cow<str>` unescapes, `src/protocol.rs:43`; byte-for-byte keys | `:739` | met |
| Sixty-fifth topic rejected, no delivery, slot reusable | `src/registry.rs:9`, `:106-108`; `src/ws.rs:200` | `:767` | met |
| Several subscribes, one rejected | `src/ws.rs:198-201` | `:792` | met |
| Error names topic: publish without data | `src/ws.rs:207-209` | `:792` | met |
| Error names topic: unknown type | `src/ws.rs:191-193` | `:792` | met |
| Error topic null when unreadable (non-JSON, no topic, `topic:5`) | `src/ws.rs:186-188`, `src/protocol.rs:43` | `:833` | met |
| Zero-length binary topic gives `""` | `src/protocol.rs:68-70` | `:792`, `:250` | met |
| Truncated binary header gives null | `src/protocol.rs:72-74` | `:833`, `:250` | met |

Summary: every requirement and scenario is met. No partial and no not met.

## 2. Hot-path invariants (design decision 9), `git diff main...HEAD -- src/`

| Invariant | Evidence | Status |
|---|---|---|
| Payload never deserialized | `data` is `Option<&RawValue>` (`src/protocol.rs:44-45`) and `ServerMessage::Message.data` is `&RawValue` (`:27`). No `Value`, no typed payload, no field lookup in the diff. The control frame (`type`, `topic`) is parsed, as the decision allows. | met |
| One serialization per message | `build` closure runs once, after the empty-target check (`src/registry.rs:72`); text built by `to_text` at `src/ws.rs:211`, binary by `delivered` at `:221`. Targets receive `frame.clone()` (`src/registry.rs:74`). | met |
| Clone is a refcount bump | Inbox type is `broadcast::Sender<Message>` (`src/registry.rs:14`); `Message::Text` wraps `Utf8Bytes` and `Message::Binary` wraps `Bytes`. The last target gets the original (`:76`). | met |
| No locks on the hot path | Registry is `papaya::HashMap` (lock-free reads, `src/registry.rs:19`, `:64`); `Arc<[Subscriber]>` is copy-on-write. Shared state is `AtomicUsize` plus the registry (`src/state.rs:6-9`). No `Mutex` or `RwLock` anywhere in the diff. Only tokio channel internals lock, as entry 16 allows. | met |
| Never `.await` holding a lock | `fanout`, `subscribe`, `unsubscribe` are sync (`src/registry.rs:27-77`); the papaya guard in `fanout` is dropped before returning, and `handle_text`/`handle_binary` are sync (`src/ws.rs:185`, `:218`). The only await in the reader (`control.send`, `:178`) holds nothing. | met |
| Bounded everywhere | `INBOX_CAPACITY = 256` (`src/ws.rs:27`), `CONTROL_QUEUE_CAPACITY = 16` (`:28`), 64 subscriptions (`src/registry.rs:9`, `:106`), topic at most 255 bytes (`src/protocol.rs:8`), 64 KiB frames (`src/ws.rs:26`). | met |
| No per-message logging | The only `info!` calls are connect and disconnect (`src/ws.rs:65`, `:93`). Drops are counted into `lagged_total` (`:109`). | met |

## 3. Greps over the diff

- Domain vocabulary (`username|room|chat|notification|player`, case-insensitive) on added lines of `git diff main...HEAD -- src/`: no matches.
- Code comments (`//` on added lines of the same diff): no matches. The same search over every `.rs` file in `src/`, `tests/` and `examples/` finds `//` only inside string literals (`ws://...` URLs), no comments.

## 4. Checks

| Command | Result |
|---|---|
| `cargo fmt --check` | pass (exit 0) |
| `cargo clippy --all-targets -- -D warnings` | pass (exit 0) |
| `cargo test` | pass: 17 unit + 36 integration + 13 example tests, 0 failed |
| `openspec validate --all --strict` | pass: 4 of 4 (three INFO notes that requirement text is long, not failures) |

## Blocking

None.

## Non-blocking

- `tests/gateway.rs:236` (`binary_and_text_share_topics`) covers the scenario "Binary and text share topics" but is not listed in the `Teste:` line of "Binary frames carry a topic header" in `specs/message-relay/spec.md`. The concurrency test `tests/gateway.rs:854` (`membership_churn_does_not_disturb_a_steady_receiver`, task 4.4) is not cited in any delta either.
- The scenario "Topic emptied by disconnection starts over" has no test of its own. It is the second half of `emptied_topic_starts_over` (`tests/gateway.rs:660`), which waits a fixed 100 ms for the disconnect to be processed. That sleep could be flaky on a loaded CI runner.
- `subscribe` and `unsubscribe` copy the member list (`src/registry.rs:37-40`, `:57`), so churn on a very large topic is O(topic size). Design decision 2 accepts this. It is not measured in this pass.
- The `papaya` dependency (`Cargo.toml`) adds lock-freedom that was checked here only from its documented behavior and the absence of locks in gateway code, not by a measurement.

## Verdict

ready to merge

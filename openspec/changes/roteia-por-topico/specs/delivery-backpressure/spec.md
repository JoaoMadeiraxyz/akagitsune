# Spec Delta

## MODIFIED Requirements

### Requirement: Lagging connections are warned of dropped frames
Each connection SHALL hold at most 256 frames pending delivery. When a frame is to be delivered to a connection that already holds 256, the gateway SHALL discard that connection's oldest pending frame, for that connection only. When the connection is next served after one or more discards, the gateway SHALL send it `{"type":"warning","dropped":<n>}` before any further frame, where `n` is greater than zero and equal to the number of frames discarded for it since the previous frame it was sent, and SHALL then continue with the oldest frame still pending. Frames a connection published itself SHALL NOT be counted in `n`.

Fonte: planned — `src/registry.rs` — `Subscriber`; `src/ws.rs` — `INBOX_CAPACITY`; `src/ws.rs` — `handle_socket`; `src/protocol.rs` — `ServerMessage`.
Teste: planned — `tests/gateway.rs` — `slow_consumer_receives_a_warning_frame`; `tests/gateway.rs` — `delivery_resumes_after_a_warning`; `tests/gateway.rs` — `dropped_count_matches_the_frames_skipped`.

#### Scenario: Slow consumer receives a warning
- **WHEN** connection B is subscribed to `k` and reads slower than frames arrive
- **AND** connection A publishes 128000 frames to `k`
- **THEN** B eventually receives a frame `{"type":"warning","dropped":<n>}` with `n > 0`

#### Scenario: Delivery resumes after a warning
- **WHEN** connection B is subscribed to `k` and lags
- **AND** connection A publishes 128000 frames to `k` with `data` `{"seq":<i>}`, followed by `{"marker":"end"}`
- **THEN** B receives at least one `warning`
- **AND** every `seq` B receives is strictly greater than the previous one
- **AND** B receives the `{"marker":"end"}` frame

#### Scenario: Dropped count is exact
- **WHEN** connection B is subscribed to `k` and lags
- **AND** connection A publishes 128000 frames to `k` with `data` `{"seq":<i>}`, followed by `{"marker":"end"}`
- **THEN** B receives at least one `warning`
- **AND** each `seq` B receives equals the previous `seq` plus one (or 0 for the first), plus the sum of `dropped` in the warnings received since the previous `seq`
- **AND** the number of frames B receives, marker included, plus the sum of every `dropped` equals 128001

### Requirement: Publishing never waits for a slow receiver
The gateway SHALL accept and deliver a publisher's frames regardless of how far behind any subscriber of the topic is. A slow subscriber SHALL only cause frames to be dropped for itself.

Fonte: planned — `src/ws.rs` — `handle_socket`; `src/registry.rs` — `TopicRegistry`.
Teste: planned — `tests/gateway.rs` — `slow_receiver_does_not_hold_back_others`.

#### Scenario: Other connections keep receiving
- **WHEN** connections B and C are subscribed to `k` and B stops reading
- **AND** connection A publishes 128000 frames to `k` with `data` `{"seq":<i>}` in chunks of 100, each chunk sent after C received the previous one, followed by `{"marker":"end"}`
- **THEN** C receives every `seq` from 0 to 127999 in order, followed by `{"marker":"end"}`
- **AND** B, when it reads again, receives a `warning`

# Spec Delta

## MODIFIED Requirements

### Requirement: Lagging connections are warned of dropped frames
When a connection falls more than 256 frames behind the relay bus, the gateway SHALL discard the frames it missed, SHALL send it `{"type":"warning","dropped":<n>}` with `n` greater than zero equal to the number of frames discarded, and SHALL continue delivering from the current position of the bus.

Fonte: `src/ws.rs:82` — `handle_socket`; `src/state.rs:7` — `BROADCAST_CAPACITY`; `src/protocol.rs:10` — `ServerMessage`.
Teste: `tests/gateway.rs:175` — `slow_consumer_receives_a_warning_frame`; `tests/gateway.rs:195` — `delivery_resumes_after_a_warning`; `tests/gateway.rs:236` — `dropped_count_matches_the_frames_skipped`.

#### Scenario: Slow consumer receives a warning
- **WHEN** connection A sends 128000 text frames while connection B reads slower than they arrive
- **THEN** B eventually receives a frame `{"type":"warning","dropped":<n>}` with `n > 0`

#### Scenario: Delivery resumes after a warning
- **WHEN** connection A sends 128000 text frames `{"seq":<i>}` followed by `{"marker":"end"}` while connection B lags
- **THEN** B receives at least one `warning`
- **AND** every `seq` B receives is strictly greater than the previous one
- **AND** B receives the `{"marker":"end"}` frame

#### Scenario: Dropped count is exact
- **WHEN** connection A sends 128000 text frames `{"seq":<i>}` followed by `{"marker":"end"}` while connection B lags
- **THEN** B receives at least one `warning`
- **AND** each `seq` B receives equals the previous `seq` plus one (or 0 for the first) plus the sum of `dropped` in the warnings received since the previous `seq`
- **AND** the number of frames B receives, marker included, plus the sum of every `dropped` equals 128001

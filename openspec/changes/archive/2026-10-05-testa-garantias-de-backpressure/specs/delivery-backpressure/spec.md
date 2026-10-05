# Spec Delta

## MODIFIED Requirements

### Requirement: Lagging connections are warned of dropped frames
When a connection falls more than 256 frames behind the relay bus, the gateway SHALL discard the frames it missed, SHALL send it `{"type":"warning","dropped":<n>}` with `n` greater than zero equal to the number of frames discarded, and SHALL continue delivering from the current position of the bus.

Fonte: `src/ws.rs:82` — `handle_socket`; `src/state.rs:7` — `BROADCAST_CAPACITY`; `src/protocol.rs:10` — `ServerMessage`.
Teste: `tests/gateway.rs:111` — `slow_consumer_receives_a_warning_frame`; `tests/gateway.rs:133` — `delivery_resumes_after_a_warning`.

#### Scenario: Slow consumer receives a warning
- **WHEN** connection A sends 128000 text frames while connection B reads slower than they arrive
- **THEN** B eventually receives a frame `{"type":"warning","dropped":<n>}` with `n > 0`

#### Scenario: Delivery resumes after a warning
- **WHEN** connection A sends 128000 text frames `{"seq":<i>}` followed by `{"marker":"end"}` while connection B lags
- **THEN** B receives at least one `warning`
- **AND** every `seq` B receives is strictly greater than the previous one
- **AND** B receives the `{"marker":"end"}` frame

### Requirement: Publishing never waits for a slow receiver
The gateway SHALL accept and relay a sender's frames regardless of how far behind any receiving connection is; a slow receiver SHALL only cause frames to be dropped for itself.

Fonte: `src/ws.rs:114` — `handle_socket`; `src/ws.rs:89` — `handle_socket`.
Teste: `tests/gateway.rs:174` — `slow_receiver_does_not_hold_back_others`.

#### Scenario: Other connections keep receiving
- **WHEN** connection B stops reading while connection A sends 128000 text frames `{"seq":<i>}` in chunks of 100, each chunk sent after connection C received the previous one, followed by `{"marker":"end"}`
- **THEN** C receives every `seq` from 0 to 127999 in order, followed by `{"marker":"end"}`
- **AND** B, when it reads again, receives a `warning`

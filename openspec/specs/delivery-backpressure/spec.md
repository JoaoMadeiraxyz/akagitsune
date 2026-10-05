# delivery-backpressure Specification

## Purpose
Defines what happens when a connection cannot keep up with the frames relayed to it: delivery is best-effort, the slow connection is told how much it lost, and publishers are never held back.

## Requirements

### Requirement: Lagging connections are warned of dropped frames
When a connection falls more than 256 frames behind the relay bus, the gateway SHALL discard the frames it missed, SHALL send it `{"type":"warning","dropped":<n>}` with `n` greater than zero equal to the number of frames discarded, and SHALL continue delivering from the current position of the bus.

Fonte: `src/ws.rs:82` — `handle_socket`; `src/state.rs:7` — `BROADCAST_CAPACITY`; `src/protocol.rs:10` — `ServerMessage`.
Teste: `tests/gateway.rs:172` — `slow_consumer_receives_a_warning_frame` (asserts the warning and `dropped > 0`; continued delivery after the warning is not asserted).

#### Scenario: Slow consumer receives a warning
- **WHEN** connection A sends 128000 text frames while connection B reads slower than they arrive
- **THEN** B eventually receives a frame `{"type":"warning","dropped":<n>}` with `n > 0`

### Requirement: Publishing never waits for a slow receiver
The gateway SHALL accept and relay a sender's frames regardless of how far behind any receiving connection is; a slow receiver SHALL only cause frames to be dropped for itself.

Fonte: `src/ws.rs:114` — `handle_socket`; `src/ws.rs:89` — `handle_socket`.
Teste: none; confirmed by reading the code: publishing to the bus does not await, and a full per-connection queue only stalls that connection's own forwarding.

#### Scenario: Other connections keep receiving
- **WHEN** connection B stops reading while connection A keeps sending
- **THEN** connection C continues to receive A's frames

# Spec Delta

## ADDED Requirements

### Requirement: A publish may carry a delay
A `publish` text frame MAY carry `delay_ms`, a non-negative integer number of milliseconds. The gateway SHALL wait `delay_ms` before delivering the publish to the subscribers of its topic. A `publish` without `delay_ms`, or with `delay_ms` of `0`, SHALL be delivered immediately. The waiting time SHALL start when the publish becomes the next frame the gateway processes for that connection.

Fonte: `src/protocol.rs:39` — `ClientFrame`; `src/ws.rs:164` — `read_loop`.
Teste: `tests/gateway.rs` — `delayed_publish_example_timeline` (to be added).

#### Scenario: Delays are relative to the previous frame
- **WHEN** connection B is subscribed to `k`
- **AND** connection A sends, back to back, a publish with `"delay_ms":300`, a publish with no delay, and a publish with `"delay_ms":400`
- **THEN** B receives the first about 300 ms after A sent it
- **AND** B receives the second immediately after the first
- **AND** B receives the third about 400 ms after the second

#### Scenario: A publish without a delay is not delayed
- **WHEN** connection A sends a publish with no `delay_ms` while no earlier publish of A is waiting
- **THEN** connection B, subscribed to the topic, receives it without added delay

### Requirement: Frames of one connection keep their order
The gateway SHALL deliver the frames of one connection in the order it sent them, including frames without a delay sent after a frame with one. A frame sent while a delayed publish is waiting SHALL be processed after that publish is delivered.

Fonte: `src/ws.rs:164` — `read_loop`.
Teste: `tests/gateway.rs` — `frame_sent_during_delay_is_processed_after_it` (to be added).

#### Scenario: A later frame waits behind a delayed one
- **WHEN** connection A sends a publish with `"delay_ms":400`, then a publish with no delay
- **THEN** connection B, subscribed to the topic, receives the delayed publish first and the other one after it

### Requirement: A waiting publish dies with its connection
If the connection that sent a delayed publish closes before the wait ends, whether by a `Close` frame, the end of the stream or a socket error, the gateway SHALL NOT deliver that publish. Nothing a connection holds SHALL survive it, and a new connection SHALL start with nothing.

Fonte: `src/ws.rs:164` — `read_loop`.
Teste: `tests/gateway.rs` — `close_during_delay_drops_the_publish`, `tcp_drop_during_delay_drops_the_publish` (to be added).

#### Scenario: Close during the wait
- **WHEN** connection A sends a publish with `"delay_ms":500` and then a `Close` frame before 500 ms pass
- **THEN** connection B, subscribed to the topic, receives nothing for it

#### Scenario: Abrupt drop during the wait
- **WHEN** connection A sends a publish with `"delay_ms":500` and drops its TCP connection without a `Close` frame before 500 ms pass
- **THEN** connection B, subscribed to the topic, receives nothing for it

### Requirement: The delay has a maximum
The gateway SHALL reject a publish whose `delay_ms` is greater than 120 000 with `{"type":"error","topic":"<key>","message":"<INVALID_DELAY>"}`, SHALL NOT deliver it, SHALL NOT wait, and SHALL keep the connection open.

Fonte: `src/protocol.rs:11` — `MAX_TOPIC_LEN` (the new `MAX_DELAY_MS` goes next to it); `src/ws.rs:34` — error messages.
Teste: `tests/gateway.rs` — `delay_above_cap_is_rejected` (to be added).

#### Scenario: Delay above the cap is rejected
- **WHEN** connection A sends a publish with `"delay_ms":120001`
- **THEN** A receives a frame with `"type":"error"` and the publish's `"topic"`
- **AND** connection B, subscribed to the topic, receives nothing
- **AND** a following publish from A is delivered normally

### Requirement: A malformed delay makes the frame invalid
A `delay_ms` that is not a non-negative integer SHALL make the frame an invalid frame, answered as described in `message-relay`.

Fonte: `src/protocol.rs:39` — `ClientFrame`.
Teste: `tests/gateway.rs` — `non_integer_delay_is_an_invalid_frame` (to be added).

#### Scenario: Negative or non-numeric delay
- **WHEN** connection A sends a publish with `"delay_ms":-1`, and another with `"delay_ms":"5"`
- **THEN** A receives a frame with `"type":"error"` for each
- **AND** connection B, subscribed to the topic, receives nothing

### Requirement: Delay applies only to text publishes
`delay_ms` on `subscribe` or `unsubscribe` SHALL be ignored. A binary publish SHALL always be delivered immediately, because the binary header carries no delay.

Fonte: `src/ws.rs:185` — `handle_text`; `src/ws.rs:218` — `handle_binary`.
Teste: `tests/gateway.rs` — `delay_on_subscribe_is_ignored` (to be added).

#### Scenario: Delay on subscribe is ignored
- **WHEN** connection A sends `{"type":"subscribe","topic":"k","delay_ms":500}`
- **THEN** A receives `{"type":"subscribed","topic":"k"}` without waiting

### Requirement: A waiting connection still answers protocol pings
While a delayed publish waits, the gateway SHALL still answer a protocol-level `Ping` from that connection with a `Pong`. Text control frames sent in that time (subscribe, unsubscribe, publish) SHALL be processed only after the wait ends.

Fonte: `src/ws.rs:164` — `read_loop`.
Teste: `tests/gateway.rs` — `ping_during_delay_is_answered` (to be added).

#### Scenario: Ping during the wait
- **WHEN** connection A sends a publish with `"delay_ms":600` and then a `Ping` frame
- **THEN** A receives a `Pong` before 600 ms pass

# publish-pacing Specification

## Purpose
A text `publish` may carry `delay_ms`, and the connection's reader waits that long before the gateway fans the frame out. There is no queue: frames of one connection stay in order, and nothing a connection holds survives it.

## Requirements

### Requirement: A publish may carry a delay
A `publish` text frame MAY carry `delay_ms`, a non-negative integer number of milliseconds. The gateway SHALL wait `delay_ms` before delivering the publish to the subscribers of its topic. A `publish` without `delay_ms`, or with `delay_ms` of `0`, SHALL be delivered immediately. The waiting time SHALL start when the publish becomes the next frame the gateway processes for that connection.

Fonte: `src/protocol.rs:40` — `ClientFrame`; `src/ws.rs:238` — `read_loop`.
Teste: `tests/gateway.rs:519` — `delayed_publish_example_timeline`.

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

Fonte: `src/ws.rs:238` — `read_loop`; `src/ws.rs:289` — `wait_out`.
Teste: `tests/gateway.rs:574` — `frame_sent_during_delay_is_processed_after_it`; `tests/gateway.rs:746` — `binary_frame_sent_during_delay_is_processed_after_it`.

#### Scenario: A later frame waits behind a delayed one
- **WHEN** connection A sends a publish with `"delay_ms":400`, then a publish with no delay
- **THEN** connection B, subscribed to the topic, receives the delayed publish first and the other one after it

### Requirement: A waiting publish dies with its connection
If the connection that sent a delayed publish closes before the wait ends and before another frame is held behind it, whether by a `Close` frame, the end of the stream or a socket error, the gateway SHALL NOT deliver that publish. Nothing a connection holds SHALL survive it, and a new connection SHALL start with nothing. Once a frame is held, the socket is not read until the wait ends, so a disconnect that arrives behind the held frame SHALL be seen only then: the delayed publish and the held frame are delivered first.

Fonte: `src/ws.rs:238` — `read_loop`; `src/ws.rs:289` — `wait_out`.
Teste: `tests/gateway.rs:594` — `close_during_delay_drops_the_publish`; `tests/gateway.rs:615` — `tcp_drop_during_delay_drops_the_publish`; `tests/gateway.rs:786` — `close_behind_held_frame_is_seen_after_the_wait`.

#### Scenario: Close during the wait
- **WHEN** connection A sends a publish with `"delay_ms":500` and then a `Close` frame before 500 ms pass
- **THEN** connection B, subscribed to the topic, receives nothing for it

#### Scenario: Abrupt drop during the wait
- **WHEN** connection A sends a publish with `"delay_ms":500` and drops its TCP connection without a `Close` frame before 500 ms pass
- **THEN** connection B, subscribed to the topic, receives nothing for it

#### Scenario: Close behind a held frame
- **WHEN** connection A sends a publish with `"delay_ms":400`, then a publish with no delay, then a `Close` frame
- **THEN** connection B, subscribed to the topic, receives the delayed publish after about 400 ms and then the other publish

### Requirement: The delay has a maximum
The gateway SHALL reject a publish whose `delay_ms` is an integer from 120 001 up to 18446744073709551615 with `{"type":"error","topic":"<key>","message":"<INVALID_DELAY>"}`, SHALL NOT deliver it, SHALL NOT wait, and SHALL keep the connection open.

Fonte: `src/protocol.rs:9` — `MAX_DELAY_MS`; `src/ws.rs:37` — `INVALID_DELAY`; `src/ws.rs:308` — `parse_text`.
Teste: `tests/gateway.rs:637` — `delay_above_cap_is_rejected`; `tests/gateway.rs:727` — `delay_at_cap_is_accepted`.

#### Scenario: Delay above the cap is rejected
- **WHEN** connection A sends a publish with `"delay_ms":120001`
- **THEN** A receives a frame with `"type":"error"` and the publish's `"topic"`
- **AND** connection B, subscribed to the topic, receives nothing
- **AND** a following publish from A is delivered normally

### Requirement: A malformed delay makes the frame invalid
A `delay_ms` that is not a JSON integer literal from 0 to 18446744073709551615 SHALL make the frame an invalid frame, on any frame type, answered as described in `message-relay` with `topic` null. This covers negative, fractional, string, `null` and boolean values, the forms `1e3`, `1.0` and `-0`, and an integer too large for an unsigned 64-bit integer.

Fonte: `src/protocol.rs:40` — `ClientFrame`.
Teste: `tests/gateway.rs:658` — `non_integer_delay_is_an_invalid_frame`; `tests/gateway.rs:769` — `malformed_delay_on_subscribe_is_an_invalid_frame`.

#### Scenario: Negative or non-numeric delay
- **WHEN** connection A sends a publish with `"delay_ms":-1`, and another with `"delay_ms":"5"`
- **THEN** A receives a frame with `"type":"error"` for each
- **AND** connection B, subscribed to the topic, receives nothing

#### Scenario: Malformed delay on subscribe
- **WHEN** connection A sends `{"type":"subscribe","topic":"k","delay_ms":-1}`
- **THEN** A receives a frame with `"type":"error"` and `"topic":null`
- **AND** A is not subscribed to `k`

### Requirement: Delay applies only to text publishes
A well-formed `delay_ms` on `subscribe` or `unsubscribe` SHALL be ignored, whatever its value, including one above 120 000. A malformed one SHALL make the frame invalid, as for any frame type. A binary publish SHALL always be delivered immediately, because the binary header carries no delay.

Fonte: `src/ws.rs:308` — `parse_text`; `src/ws.rs:346` — `handle_binary`.
Teste: `tests/gateway.rs:679` — `delay_on_subscribe_is_ignored`.

#### Scenario: Delay on subscribe is ignored
- **WHEN** connection A sends `{"type":"subscribe","topic":"k","delay_ms":500}`
- **THEN** A receives `{"type":"subscribed","topic":"k"}` without waiting

### Requirement: A waiting connection still answers protocol pings
While a delayed publish waits, the gateway SHALL still answer a protocol-level `Ping` from that connection with a `Pong`. Text control frames sent in that time (subscribe, unsubscribe, publish) SHALL be processed only after the wait ends.

Fonte: `src/ws.rs:238` — `read_loop`; `src/ws.rs:289` — `wait_out`.
Teste: `tests/gateway.rs:706` — `ping_during_delay_is_answered`.

#### Scenario: Ping during the wait
- **WHEN** connection A sends a publish with `"delay_ms":600` and then a `Ping` frame
- **THEN** A receives a `Pong` before 600 ms pass

# message-relay Specification

## Purpose
Defines how the gateway relays text and binary frames between connections without interpreting the payload, and what a sender observes when a text frame is not valid JSON.

## Requirements

### Requirement: Text frames are relayed in an envelope
The gateway SHALL deliver every valid `publish` text frame to the other subscribers of its topic as a text frame `{"type":"message","topic":"<key>","from":"<sender uuid>","data":<payload>}`, where `data` is the sender's `data` JSON value embedded byte for byte, without being deserialized. Whitespace before and after the value SHALL NOT be part of `data`; every byte from the first to the last byte of the value SHALL be kept, including interior whitespace, key order, escape sequences and number formatting.

Fonte: `src/ws.rs:185` — `handle_text`; `src/protocol.rs:14` — `ServerMessage`.
Teste: `tests/gateway.rs:164` — `payload_is_relayed_verbatim_to_others`; `tests/gateway.rs:178` — `text_payload_bytes_are_relayed_verbatim`.

#### Scenario: Object payload reaches another connection
- **WHEN** connection B is subscribed to `k`
- **AND** connection A sends `{"type":"publish","topic":"k","data":{"hp":42,"pos":[1,2],"nested":{"any":null}}}`
- **THEN** B receives `{"type":"message","topic":"k","from":"<A's id>","data":{"hp":42,"pos":[1,2],"nested":{"any":null}}}`

#### Scenario: Payload bytes are kept exactly
- **WHEN** connection B is subscribed to `k`
- **AND** connection A sends `{"type":"publish","topic":"k","data":{"b":1,  "a":[ 1,2 ],"u":"\u00e9","n":1.50}}`
- **THEN** B receives exactly the text `{"type":"message","topic":"k","from":"<A's id>","data":{"b":1,  "a":[ 1,2 ],"u":"\u00e9","n":1.50}}`

#### Scenario: Surrounding whitespace is dropped
- **WHEN** connection B is subscribed to `k`
- **AND** connection A sends `{"type":"publish","topic":"k","data":  42  }`
- **THEN** B receives exactly the text `{"type":"message","topic":"k","from":"<A's id>","data":42}`

### Requirement: Any JSON value is accepted
The gateway SHALL accept any JSON value as the `data` of a `publish`, including numbers, strings, arrays and `null`, and SHALL NOT require any field or shape inside it.

Fonte: `src/protocol.rs:39` — `ClientFrame`.
Teste: `tests/gateway.rs:199` — `any_json_shape_is_accepted`.

#### Scenario: Non-object payloads are relayed
- **WHEN** connection B is subscribed to `k`
- **AND** connection A publishes to `k` with `data` set to `42`, `"texto"`, `[1,2,3]` and `null`
- **THEN** B receives each one as the `data` of a `message` envelope

### Requirement: Invalid JSON text is rejected to the sender only
A text frame that is not valid JSON, or that is not a `subscribe`, `unsubscribe` or `publish` frame with the required fields, SHALL NOT be delivered to anyone. The gateway SHALL send the sender `{"type":"error","topic":<topic or null>,"message":"<INVALID_FRAME>"}`, with `topic` as defined in `topic-routing`, and SHALL keep the connection open. Fields other than `type`, `topic` and `data` SHALL be ignored.

Fonte: `src/ws.rs:30` — `INVALID_FRAME`; `src/protocol.rs:39` — `ClientFrame`.
Teste: `tests/gateway.rs:453` — `invalid_json_is_rejected_without_broadcasting`; `tests/gateway.rs:470` — `unrecognized_frame_is_rejected`.

#### Scenario: Invalid text produces an error and no broadcast
- **WHEN** connection B is subscribed to `k`
- **AND** connection A sends the text frame `not json at all`
- **THEN** A receives a frame with `"type":"error"` and `"topic":null`
- **AND** B receives nothing

#### Scenario: Bare JSON is not a publish
- **WHEN** connection B is subscribed to `k`
- **AND** connection A sends the text frame `{"hp":42}`
- **THEN** A receives a frame with `"type":"error"` and `"topic":null`
- **AND** B receives nothing

#### Scenario: Publish without data is rejected
- **WHEN** connection A sends `{"type":"publish","topic":"k"}`
- **THEN** A receives a frame with `"type":"error"` and `"topic":"k"`

#### Scenario: Connection keeps working after an error
- **WHEN** connection A, after receiving an `error` frame, publishes a valid frame to `k`
- **THEN** connection B, subscribed to `k`, receives it in a `message` envelope

### Requirement: Senders do not receive their own frames
The gateway SHALL NOT deliver a published text or binary frame back to the connection that published it, including when that connection is subscribed to the topic.

Fonte: `src/registry.rs:63` — `fanout`.
Teste: `tests/gateway.rs:500` — `subscribed_sender_gets_no_echo`.

#### Scenario: No echo for text
- **WHEN** connection A is subscribed to `k` and publishes a text frame to `k`
- **THEN** A receives no frame for it

#### Scenario: No echo for binary
- **WHEN** connection A is subscribed to `k` and publishes a binary frame to `k`
- **THEN** A receives no frame for it

### Requirement: Frames from one sender arrive in order
For a single sender, every receiving connection SHALL receive that sender's delivered frames in the order they were sent, including across different topics, as long as the receiver is not dropping frames.

Fonte: `src/ws.rs:164` — `read_loop`.
Teste: `tests/gateway.rs:277` — `fifo_order_holds_across_a_multi_batch_burst`; `tests/gateway.rs:294` — `order_holds_across_topics`.

#### Scenario: Burst preserves order
- **WHEN** connection B is subscribed to `k`
- **AND** connection A publishes 200 frames to `k` with `data` `{"seq":0}` through `{"seq":199}` back to back
- **THEN** B receives them with `seq` from 0 to 199 in that order

#### Scenario: Order holds across topics
- **WHEN** connection B is subscribed to `k1` and `k2`
- **AND** connection A publishes 200 frames alternating between `k1` and `k2` with `data` `{"seq":0}` through `{"seq":199}`
- **THEN** B receives them with `seq` from 0 to 199 in that order

### Requirement: Binary frames carry a topic header
The gateway SHALL read an inbound binary frame as `[len: u8][topic: len bytes of UTF-8][payload]`, and SHALL deliver it to the other subscribers of that topic as a binary frame `[len: u8][topic][sender uuid: 16 bytes in RFC 4122 byte order][payload]`, with the payload bytes unchanged. The payload MAY be empty.

Fonte: `src/protocol.rs:60` — `BinaryHeader`; `src/ws.rs:218` — `handle_binary`.
Teste: `tests/gateway.rs:212` — `binary_frames_carry_topic_and_sender`.

#### Scenario: Binary payload arrives with topic and sender
- **WHEN** connection B is subscribed to `k`
- **AND** connection A sends a binary frame with bytes `01 6b 00 ff 10 42`
- **THEN** B receives a binary frame with bytes `01 6b`, then A's id as 16 bytes, then `00 ff 10 42`

#### Scenario: Binary and text share topics
- **WHEN** connection B is subscribed to `k`
- **AND** connection A publishes one text frame and one binary frame to `k`
- **THEN** B receives both, in that order

### Requirement: Malformed binary frames are rejected to the sender only
A binary frame whose length byte is 0, that is shorter than `1 + len` bytes, or whose topic bytes are not UTF-8 SHALL NOT be delivered. The gateway SHALL send the sender `{"type":"error","topic":<topic or null>,"message":"<INVALID_BINARY>"}` as a text frame, with `topic` as defined in `topic-routing` (`""` for a zero length byte, `null` when the topic bytes cannot be read), and SHALL keep the connection open.

Fonte: `src/protocol.rs:60` — `BinaryHeader`; `src/ws.rs:32` — `INVALID_BINARY`.
Teste: `tests/gateway.rs:250` — `malformed_binary_is_rejected`.

#### Scenario: Truncated header is rejected
- **WHEN** connection A sends a binary frame with bytes `05 6b`
- **THEN** A receives a text frame with `"type":"error"` and `"topic":null`
- **AND** no connection receives anything for it

#### Scenario: Zero-length topic is rejected
- **WHEN** connection A sends a binary frame with bytes `00 ff`
- **THEN** A receives a text frame with `"type":"error"` and `"topic":""`

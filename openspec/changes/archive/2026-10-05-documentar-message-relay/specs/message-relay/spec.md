# Spec Delta

## Purpose

Defines how the gateway relays text and binary frames between connections without interpreting the payload, and what a sender observes when a text frame is not valid JSON.

## ADDED Requirements

### Requirement: Text frames are relayed in an envelope
The gateway SHALL relay every valid JSON text frame to every other connection as a text frame `{"type":"message","from":"<sender uuid>","data":<payload>}`, where `data` is the sender's JSON embedded without being deserialized.

Fonte: `src/ws.rs:98` — `handle_socket`; `src/protocol.rs:9` — `ServerMessage`.
Teste: `tests/gateway.rs:52` — `payload_is_relayed_verbatim_to_others`.

#### Scenario: Object payload reaches another connection
- **WHEN** connection A sends the text frame `{"hp":42,"pos":[1,2],"nested":{"any":null}}`
- **THEN** connection B receives `{"type":"message","from":"<A's id>","data":{"hp":42,"pos":[1,2],"nested":{"any":null}}}`

### Requirement: Any JSON value is accepted
The gateway SHALL accept any JSON value as a text payload, including numbers, strings, arrays and `null`, and SHALL NOT require any field or shape.

Fonte: `src/ws.rs:98` — `handle_socket`.
Teste: `tests/gateway.rs:68` — `any_json_shape_is_accepted`.

#### Scenario: Non-object payloads are relayed
- **WHEN** connection A sends `42`, `"texto"`, `[1,2,3]` and `null` as text frames
- **THEN** connection B receives each one as the `data` of a `message` envelope

### Requirement: Invalid JSON text is rejected to the sender only
A text frame that is not valid JSON SHALL NOT be relayed. The gateway SHALL send the sender `{"type":"error","message":"text frames must contain valid JSON; use binary frames otherwise"}` and SHALL keep the connection open.

Fonte: `src/ws.rs:100` — `handle_socket`; `src/ws.rs:22` — `INVALID_PAYLOAD`; `src/protocol.rs:11` — `ServerMessage`.
Teste: `tests/gateway.rs:133` — `invalid_json_is_rejected_without_broadcasting`.

#### Scenario: Invalid text produces an error and no broadcast
- **WHEN** connection A sends the text frame `not json at all`
- **THEN** A receives a frame with `"type":"error"`
- **AND** connection B receives nothing

#### Scenario: Connection keeps working after an error
- **WHEN** connection A, after receiving an `error` frame, sends a valid JSON text frame
- **THEN** connection B receives it in a `message` envelope

### Requirement: Binary frames pass through untouched
The gateway SHALL relay every binary frame to every other connection as a binary frame with identical bytes and no envelope.

Fonte: `src/ws.rs:109` — `handle_socket`.
Teste: `tests/gateway.rs:80` — `binary_frames_pass_through_untouched`.

#### Scenario: Binary bytes arrive unchanged
- **WHEN** connection A sends a binary frame with bytes `00 ff 10 42`
- **THEN** connection B receives a binary frame with bytes `00 ff 10 42`

### Requirement: Senders do not receive their own frames
The gateway SHALL NOT deliver a relayed text or binary frame back to the connection that sent it.

Fonte: `src/ws.rs:80` — `handle_socket`.
Teste: `tests/gateway.rs:52` — `payload_is_relayed_verbatim_to_others`; `tests/gateway.rs:80` — `binary_frames_pass_through_untouched`.

#### Scenario: No echo for text
- **WHEN** connection A sends a valid JSON text frame
- **THEN** A receives no frame for it

#### Scenario: No echo for binary
- **WHEN** connection A sends a binary frame
- **THEN** A receives no frame for it

### Requirement: Frames from one sender arrive in order
For a single sender, every other connection SHALL receive that sender's relayed frames in the order they were sent, as long as the receiver is not lagging.

Fonte: `src/ws.rs:96` — `handle_socket`; `src/ws.rs:61` — `handle_socket`.
Teste: `tests/gateway.rs:93` — `fifo_order_holds_across_a_multi_batch_burst`.

#### Scenario: Burst preserves order
- **WHEN** connection A sends 200 text frames `{"seq":0}` through `{"seq":199}` back to back
- **THEN** connection B receives them with `seq` from 0 to 199 in that order

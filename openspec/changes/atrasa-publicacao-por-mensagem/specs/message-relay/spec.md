# Spec Delta

## MODIFIED Requirements

### Requirement: Invalid JSON text is rejected to the sender only
A text frame that is not valid JSON, or that is not a `subscribe`, `unsubscribe` or `publish` frame with the required fields, SHALL NOT be delivered to anyone. The gateway SHALL send the sender `{"type":"error","topic":<topic or null>,"message":"<INVALID_FRAME>"}`, with `topic` as defined in `topic-routing`, and SHALL keep the connection open. Fields other than `type`, `topic`, `data` and, on `publish`, `delay_ms` (see `publish-pacing`) SHALL be ignored.

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

#### Scenario: Unknown fields are still ignored
- **WHEN** connection A sends `{"type":"subscribe","topic":"k","extra":[1]}`
- **THEN** A receives `{"type":"subscribed","topic":"k"}`

# Spec Delta

## MODIFIED Requirements

### Requirement: Text frames are relayed in an envelope
The gateway SHALL relay every valid JSON text frame to every other connection as a text frame `{"type":"message","from":"<sender uuid>","data":<payload>}`, where `data` is the sender's JSON value embedded byte for byte, without being deserialized. Whitespace before and after the value SHALL NOT be part of `data`; every byte from the first to the last byte of the value SHALL be kept, including interior whitespace, key order, escape sequences and number formatting.

Fonte: `src/ws.rs:98` — `handle_socket`; `src/protocol.rs:9` — `ServerMessage`.
Teste: `tests/gateway.rs:52` — `payload_is_relayed_verbatim_to_others`; `tests/gateway.rs` — `text_payload_bytes_are_relayed_verbatim` (to be created; line added at implementation).

#### Scenario: Object payload reaches another connection
- **WHEN** connection A sends the text frame `{"hp":42,"pos":[1,2],"nested":{"any":null}}`
- **THEN** connection B receives `{"type":"message","from":"<A's id>","data":{"hp":42,"pos":[1,2],"nested":{"any":null}}}`

#### Scenario: Payload bytes are kept exactly
- **WHEN** connection A sends the text frame `{"b":1,  "a":[ 1,2 ],"u":"é","n":1.50}`
- **THEN** connection B receives exactly the text `{"type":"message","from":"<A's id>","data":{"b":1,  "a":[ 1,2 ],"u":"é","n":1.50}}`

#### Scenario: Surrounding whitespace is dropped
- **WHEN** connection A sends the text frame `  42  `
- **THEN** connection B receives exactly the text `{"type":"message","from":"<A's id>","data":42}`

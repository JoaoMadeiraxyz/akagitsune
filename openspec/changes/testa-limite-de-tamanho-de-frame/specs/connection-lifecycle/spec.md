# Spec Delta

## MODIFIED Requirements

### Requirement: Frames above 64 KiB terminate the connection
The gateway SHALL accept frames whose payload is at most 65536 bytes. A frame larger than that SHALL terminate the sending connection without a WebSocket close handshake, and SHALL NOT be relayed to any other connection.

Fonte: `src/ws.rs:20` — `MAX_MESSAGE_SIZE`; `src/ws.rs:28` — `websocket_handler`; `src/ws.rs:96` — `handle_socket`.
Teste: `tests/gateway.rs:56` — `frame_at_size_limit_is_relayed`; `tests/gateway.rs:72` — `oversized_frame_drops_the_sender_without_relaying`.

#### Scenario: Frame at the limit is relayed
- **WHEN** a client sends a text frame whose payload is exactly 65536 bytes of valid JSON
- **THEN** the other connections receive it in a `message` envelope with that payload as `data`

#### Scenario: Oversized frame drops the sender
- **WHEN** a client sends a text frame whose payload is 65537 bytes of valid JSON
- **THEN** the sender's connection ends without the sender receiving a close frame
- **AND** no other connection receives anything for that frame

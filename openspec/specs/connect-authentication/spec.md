# connect-authentication Specification

## Purpose
Defines how the gateway admits a WebSocket upgrade only when it carries a configured token, without interpreting the payload or any in-band frame.

## Requirements

### Requirement: Authentication is off unless tokens are configured
The gateway SHALL read `GATEWAY_AUTH_TOKENS` as a comma-separated list of tokens, trimming each entry and skipping empty ones. When no token remains, the gateway SHALL admit every upgrade exactly as it did before this requirement existed, and SHALL log one `warn` line at startup saying authentication is off. A token SHALL be 1 to 128 bytes of the HTTP token charset, and a configured entry outside it SHALL stop startup with an error that names the entry's position and never its value.

Fonte: `src/main.rs:10` — `main`; `src/auth.rs:31` — `parse_tokens`.
Teste: `tests/gateway.rs:1034` — `connection_is_admitted_without_credentials_when_auth_is_off`; `tests/gateway.rs:1304` — `invalid_token_entry_stops_startup_naming_only_its_position`; `tests/gateway.rs:1315` — `empty_token_configuration_logs_that_authentication_is_off`; `tests/gateway.rs:1321` — `configured_tokens_do_not_log_that_authentication_is_off`; `tests/gateway.rs:1330` — `non_utf8_token_configuration_stops_startup`; `src/auth.rs:65` — `parse_tokens` unit tests.

#### Scenario: No configuration admits everyone
- **WHEN** `GATEWAY_AUTH_TOKENS` is unset or empty
- **AND** a client opens a WebSocket to `/ws` with no credential
- **THEN** the upgrade succeeds and the first frame is `welcome`

#### Scenario: A token outside the charset stops startup
- **WHEN** `GATEWAY_AUTH_TOKENS` contains an entry with a space or an entry longer than 128 bytes
- **THEN** the gateway does not start
- **AND** the error names the entry's position and does not contain its value

### Requirement: Admission requires a configured token when authentication is on
When at least one token is configured, the gateway SHALL complete a WebSocket upgrade only if the request carries a token equal to a configured one, in `Authorization: Bearer <token>` or in the `Sec-WebSocket-Protocol` header as the offered protocols `bearer` and `<token>`. If both are present, only the `Authorization` header SHALL be considered. The comparison SHALL be constant-time for tokens of the same length.

Fonte: `src/ws.rs:77` — `websocket_handler`; `src/auth.rs:53` — `Tokens::admits`.
Teste: `tests/gateway.rs:1041` — `bearer_header_token_is_admitted`; `tests/gateway.rs:1050` — `subprotocol_token_is_admitted`; `tests/gateway.rs:1065` — `authorization_header_wins_over_subprotocol`.

#### Scenario: Bearer header with a configured token is admitted
- **WHEN** authentication is on with tokens `t1` and `t2`
- **AND** a client upgrades with `Authorization: Bearer t2`
- **THEN** the upgrade succeeds and the first frame is `welcome`

#### Scenario: Subprotocol token is admitted and not echoed
- **WHEN** authentication is on with token `t1`
- **AND** a client upgrades offering `Sec-WebSocket-Protocol: bearer, t1`
- **THEN** the upgrade succeeds
- **AND** the response `Sec-WebSocket-Protocol` is exactly `bearer`
- **AND** the first frame is `welcome`

#### Scenario: Header takes precedence over subprotocol
- **WHEN** authentication is on with token `t1`
- **AND** a client upgrades with `Authorization: Bearer wrong` and `Sec-WebSocket-Protocol: bearer, t1`
- **THEN** the upgrade is refused with `401`

### Requirement: A request without a valid token is refused before the upgrade
When authentication is on, a request with no credential, a malformed credential or a token that matches no configured token SHALL be answered with HTTP `401`, the header `WWW-Authenticate: Bearer` and an empty body, and SHALL NOT be upgraded. The response SHALL be identical for all three cases. No `welcome` frame SHALL be sent, no connection identity SHALL be allocated and the connection count SHALL NOT change. A token passed in the URL query string SHALL NOT be accepted.

Fonte: `src/ws.rs:77` — `websocket_handler`.
Teste: `tests/gateway.rs:1079` — `missing_credential_is_refused_with_401`; `tests/gateway.rs:1086` — `wrong_token_is_refused_with_401`; `tests/gateway.rs:1113` — `query_string_token_is_not_accepted`; `tests/gateway.rs:1120` — `refused_requests_do_not_count_as_connections`; `tests/gateway.rs:1145` — `each_refusal_logs_one_warn_with_the_peer_address`.

#### Scenario: No credential is refused
- **WHEN** authentication is on
- **AND** a client upgrades with no `Authorization` header and no offered subprotocols
- **THEN** the response status is `401` with `WWW-Authenticate: Bearer` and an empty body
- **AND** no WebSocket is established

#### Scenario: Wrong token is refused identically
- **WHEN** authentication is on with token `t1`
- **AND** a client upgrades with `Authorization: Bearer t3`
- **THEN** the response is byte for byte the one given for a missing credential

#### Scenario: Token in the query string is refused
- **WHEN** authentication is on with token `t1`
- **AND** a client upgrades to `/ws?token=t1` with no header and no subprotocol
- **THEN** the response status is `401`

#### Scenario: A refusal allocates nothing
- **WHEN** authentication is on
- **AND** a client is refused
- **THEN** the gateway's connection count is unchanged

### Requirement: An admitted connection behaves as before
After admission the gateway SHALL treat a connection identically whether authentication is on or off: the same `welcome` frame with a generated UUID, the same frames, the same limits and the same backpressure. The gateway SHALL NOT associate the token with the connection, SHALL NOT include it in any frame, and SHALL NOT write any token to a log. A token removed from the configuration SHALL NOT close connections that were already admitted.

Fonte: `src/ws.rs:113` — `handle_socket`.
Teste: `tests/gateway.rs:1173` — `authenticated_connection_is_welcomed_with_an_id`; `tests/gateway.rs:1184` — `authenticated_connections_relay_to_each_other`.

#### Scenario: Authenticated connection gets the same welcome
- **WHEN** authentication is on and a client is admitted with a configured token
- **THEN** the first frame is `{"type":"welcome","id":"<uuid>"}` and nothing in it derives from the token

#### Scenario: Relay between authenticated connections is unchanged
- **WHEN** two connections are admitted with different configured tokens
- **AND** both subscribe to `k` and one publishes to `k`
- **THEN** the other receives the `message` frame with the sender's connection UUID as `from`

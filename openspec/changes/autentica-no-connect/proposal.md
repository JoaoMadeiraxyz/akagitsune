# Proposal

## Why

Anyone who can reach `GET /ws` can connect, subscribe to any topic and publish on it. The README lists "no authentication" under *Not implemented yet*, and the topic routing change records authentication as the prerequisite for topic authorization (`docs/architecture.md`, *What would have to change*). Decision 4 in `docs/decisions.md` already fixes where it goes: at connect time, never as an in-band frame. This change adds the smallest admission check that satisfies it.

## Scope verdict

**In scope, with a decision entry.** `docs/scope.md` lists "authentication at connect time" under *Admission control*. The check reads the HTTP upgrade request only. `data` is never touched. The credential is an opaque string compared byte for byte, in the same way the gateway already treats a topic key.

It is not in the *Borderline cases* table, but it still gets a decision entry, because it fixes a credential model that later changes build on (`design.md` decision 1).

Vocabulary: the new nouns are `token` and `credential`. There is no `user`, `account`, `role` or `principal` in a frame, type, constant, log line or error message. A token does not map to an identity. Identity stays the connection UUID.

## What Changes

- New optional config `GATEWAY_AUTH_TOKENS`: a comma-separated list of opaque tokens. Each token is 1 to 128 bytes of the RFC 6455 subprotocol token charset (`design.md` decision 3). An invalid entry stops startup.
- **Unset or empty means authentication is off**, exactly as today. The gateway logs one `warn` line at startup saying so. Existing deployments and the test suite keep working without configuration.
- When set, `GET /ws` accepts the upgrade only if the request carries a configured token in one of two places:
  - `Authorization: Bearer <token>`, for clients that can set headers;
  - `Sec-WebSocket-Protocol: bearer, <token>`, for browsers, which cannot set headers on an upgrade. The gateway answers with `Sec-WebSocket-Protocol: bearer` and never echoes the token.
- A request with no credential or a wrong one is refused with HTTP `401` and `WWW-Authenticate: Bearer`, before the upgrade. No WebSocket exists, no `welcome` is sent, no connection id is allocated and the connection counter does not move. The body does not say which part was wrong.
- A valid credential changes nothing after the upgrade: same `welcome`, same frames, same limits.
- The token is never put in a URL, so it never lands in access logs. A query-string credential is not accepted.
- The comparison is constant-time per configured token (`subtle`).
- The credential is checked once, at connect. A token removed from the configuration does not close connections already open. They end when they close or the process restarts.
- Tokens are never logged, not even their length.

## Capabilities

### New Capabilities

- `connect-authentication`: when authentication is on, which requests are admitted, how the credential travels, what a refusal looks like, and what stays unchanged for admitted connections.

### Modified Capabilities

- `connection-lifecycle`: "Connecting is joining" gains a precondition, namely that an unauthenticated upgrade is accepted only while authentication is off. The `welcome` and identity behavior is unchanged.

## Impact

- **Code:**
  - `src/lib.rs`: `app` and `run` take the authentication setting.
  - `src/state.rs`: `AppState` holds an immutable token set (no lock, no atomic).
  - `src/ws.rs`: `websocket_handler` checks the headers before `on_upgrade`, and selects the `bearer` subprotocol through `WebSocketUpgrade::requested_protocols` and `set_selected_protocol`.
  - New `src/auth.rs` with parsing and the constant-time check.
  - `src/main.rs`: reads `GATEWAY_AUTH_TOKENS`.
- **Dependencies:** `subtle` (constant-time equality). It is not in `Cargo.lock` today, so it is a new crate (`design.md` decision 4).
- **Tests:** every new scenario gets an integration test in `tests/gateway.rs`. The existing suite runs unchanged with authentication off.
- **Clients:** none break. Clients of a deployment that turns authentication on must send a token.
- **Performance:** the check runs once per connection, before the upgrade, and touches nothing on the publish path. The implementation PR still runs `scripts/bench.sh --quick` to show connection setup did not regress. A `--goal` run is not required, since `src/registry.rs` and the reader and writer in `src/ws.rs` are not touched.
- **Hot-path invariants** (`docs/architecture.md`): none are touched. State added is immutable and bounded by configuration.
- **Documentation:** `README.md` (protocol and *Not implemented yet*), `docs/architecture.md`, `docs/decisions.md` (entries from `design.md`), `openspec/config.yaml` (Boundaries no longer says there is no auth), and the roadmap row.
- **Out of scope:**
  - mapping a token to an identity, a label or a permission;
  - token expiry, rotation endpoints and revocation of live connections;
  - JWT, OAuth or a call to an external verifier;
  - TLS termination. The gateway still expects a reverse proxy for `wss://`, and a bearer token over plain `ws://` is readable on the wire;
  - authorization of subscribe and publish, which is roadmap milestone 3.
- **Expected next step:** subscription authorization. This change deliberately gives it no hook: the token does not reach `handle_socket`. Its proposal must decide how a connection acquires permissions without the gateway learning domain vocabulary, and may need to extend what a token carries.

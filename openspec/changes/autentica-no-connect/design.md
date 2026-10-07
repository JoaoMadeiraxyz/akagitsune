# Design

## Context

Today a connection takes this path:

```
GET /ws ──► websocket_handler (src/ws.rs:36) ──► on_upgrade ──► handle_socket ──► welcome
```

Nothing looks at the request. Authentication has to answer four questions together:

1. What does the gateway compare the credential with?
2. Where does the client put it?
3. What does a refusal look like, and when does it happen?
4. What does the gateway remember about a credential afterwards?

The flow below follows a connection from request to teardown.

```
request ──► auth off?  ──yes──► upgrade (as today)
              │no
              ▼
        Authorization: Bearer <t>  or  Sec-WebSocket-Protocol: bearer, <t>
              │
              ├─ none / no match ──► 401 + WWW-Authenticate: Bearer   (no socket, no id, no counter)
              └─ match ───────────► upgrade ──► [select "bearer" if offered] ──► welcome ──► as today
                                                                                  token forgotten
```

## Goals / Non-Goals

**Goals:**

- Refuse a request without a valid credential before any per-connection resource exists.
- Work for server clients and for browsers.
- Zero cost after admission, and no change to the publish path.
- Keep every existing deployment and test working with no configuration.

**Non-Goals:**

- Knowing who a token belongs to. Identity stays the connection UUID.
- Expiry, rotation endpoints, revocation of live connections, JWT, an external verifier.
- Authorization of subscribe and publish. It was removed from the roadmap by decision 18 in `docs/decisions.md`.

## Decisions

### 1. The credential is an opaque token from a configured set

`GATEWAY_AUTH_TOKENS` holds the accepted tokens. A request is admitted if its token equals one of them byte for byte. A set, not a single value, so a token can be rotated by running with old and new at once, then removing the old one at the next restart.

Alternatives considered:

- **Signed token (JWT).** The gateway would verify a signature and an expiry, which needs a key, a clock and a library. Worse, a JWT carries claims, and the first thing a reader asks is whether the gateway may read them. The gateway has no use for claims, since it admits or refuses and nothing more.
- **External verifier.** A call to a URL at connect time puts I/O and a new failure mode (verifier down) on the admission path, and makes every connect as slow as that service.

A static set has no claims, no I/O and no state beyond the configuration. It passes the noun test: the gateway knows a token, not who holds it.

### 2. Credential transport: `Authorization` header and a subprotocol, never the URL

Two accepted forms, checked in this order:

1. `Authorization: Bearer <token>`.
2. `Sec-WebSocket-Protocol: bearer, <token>`: the client offers two subprotocols, the fixed name `bearer` and the token.

Browsers cannot set arbitrary headers on a WebSocket upgrade, so the header alone would lock them out. The subprotocol form is the same trick Kubernetes uses for its WebSocket API. In the subprotocol form the response must name one of the offered protocols, because a browser fails the connection otherwise. The gateway selects `bearer` and never the token, so the secret does not travel back.

A query-string token is rejected as a design choice: it ends up in access logs, proxy logs and browser history. The cost is that a client unable to set either form cannot authenticate, and the README says so.

If both forms are present the header wins and the subprotocol is ignored.

Facts verified in the pinned `axum 0.8.9` source (`src/extract/ws.rs`): `WebSocketUpgrade::requested_protocols` returns the offered protocols (line 279), `set_selected_protocol` sets the one echoed in the `101` response (line 293), and the selected value is inserted as `Sec-WebSocket-Protocol` (line 404).

### 3. Token charset and length

A token must be 1 to 128 bytes from the HTTP token charset (`!#$%&'*+-.^_`|~`, digits, ASCII letters), the characters RFC 6455 allows in a subprotocol name. That makes every accepted token valid in both forms. `GATEWAY_AUTH_TOKENS` entries outside it stop startup with an error naming the entry's position, never its value. Entries are split on `,` and trimmed, and empty entries are skipped.

### 4. Constant-time comparison, one new dependency

The presented token is compared against every configured token with `subtle::ConstantTimeEq`, without stopping at the first match, so response time does not reveal which token matched or how long a correct prefix is. Length is compared first and a length mismatch returns early. The length of a token is not treated as secret, and the 128-byte cap bounds the work.

`subtle` is the standard crate for this and is not in `Cargo.lock` today. A hand-written byte loop is not used, because the compiler is free to turn it back into an early exit.

### 5. Refusal is an HTTP 401 before the upgrade

A missing, malformed or wrong credential gets `401 Unauthorized` with `WWW-Authenticate: Bearer` and an empty body. It is produced in `websocket_handler` before `on_upgrade`, so:

- no `welcome` frame, no UUID and no `connections` increment happen for a refused request;
- the response is identical for "no credential", "malformed" and "wrong", so it does not help a guesser;
- a refusal is logged at `warn` level with the peer address and no credential material.

This follows decision 4 (no in-band authentication frame) and keeps the "connecting is joining" rule for admitted connections.

### 6. Authentication is off when unset

An unset or empty `GATEWAY_AUTH_TOKENS` means the gateway behaves exactly as today. The alternative, refusing to start without tokens, would break local runs, `scripts/bench.sh`, `examples/loadgen.rs` and every existing test for no benefit.

The risk is a deployment that forgets the variable and runs open without anyone noticing. Mitigation: one `warn` line at startup, `authentication is off: any client can connect`. The README states plainly that exposing the gateway beyond a trusted network with authentication off is the operator's decision.

### 7. State

`AppState` gains an immutable `Arc<[Box<[u8]>]>`-style token set built at startup and read-only afterwards. No lock, no atomic and no allocation per request beyond the header lookup. It is bounded by configuration, which satisfies the rule in `openspec/config.yaml` that new state is bounded and lock-free on the hot path. The set is never reachable from `handle_socket`, so nothing after admission can see a token.

### 8. Tokens are checked at connect only

A removed token does not close open connections. Closing them would need a registry of which connection came from which token, which is the token-to-identity mapping this change excludes, and it would add per-connection state for a case an operator can handle with a restart. The consequence is written into the decision entry so nobody assumes otherwise.

### 9. Hot-path invariants (`docs/architecture.md`)

Checked against every invariant in the list:

- **The payload is never deserialized.** Not touched. The check reads headers only.
- **One serialization per message.** Not touched.
- **Cloning a message is a refcount bump.** Not touched.
- **Gateway code uses no locks.** The check reads an immutable set and takes no lock.
- **Never `.await` while holding a lock.** Nothing is held.
- **Bounded everywhere.** The token set is bounded by configuration, and nothing is added per connection.
- **No per-message logging in the fanout path.** Not touched. A refusal logs once per refused request, at admission.

## Decision entries for `docs/decisions.md`

These are written into `docs/decisions.md` by the implementation PR. The number `N` is the next free one at that time: entry 17 is already taken by the publish pacing proposal ([akagitsune#35](https://github.com/JoaoMadeiraxyz/akagitsune/pull/35)).

### N. Connect-time authentication uses a configured set of opaque tokens

**Context.** Any client that can reach `/ws` could connect, subscribe to any topic and publish on it. Entry 4 fixed where authentication goes (connect time, not an in-band frame) but not what is verified. A signed token would put claims in front of the gateway, and an external verifier would put I/O on the admission path.

**Decision.** `GATEWAY_AUTH_TOKENS` is a comma-separated set of opaque tokens. When set, an upgrade is admitted only if `Authorization: Bearer <token>` or `Sec-WebSocket-Protocol: bearer, <token>` carries one of them, compared in constant time. Anything else is refused with `401` before the upgrade. When unset, authentication is off and the gateway logs a warning at startup. A token does not map to an identity: identity stays the connection UUID.

**Consequence.** Admission is checked once, costs nothing after it, and adds no per-connection state. A token removed from the configuration does not close live connections. A browser can authenticate through the subprotocol, and no credential travels in a URL. There is no per-topic permission, by decision 18, so an admitted connection can use any topic.

## Risks / Trade-offs

- **A bearer token over plain `ws://` is readable on the wire.** The gateway does not terminate TLS, so the operator puts it behind a proxy that does. The README says so. Not mitigated in code.
- **A shared static token identifies a deployment, not a client.** Two clients with the same token are indistinguishable, and there is no per-client revocation. This is accepted: it matches the stated goal of admission control, and anything finer is not planned (decision 18).
- **Off by default can ship an open gateway by mistake.** Mitigated only by the startup warning (decision 6).
- **A token sent to a browser is visible to whoever uses that page.** The subprotocol form lets a browser authenticate, but it does not make the token secret from that browser's user. It fits clients the operator controls, which is the stated deployment.
- **Subprotocol tokens appear in the `Sec-WebSocket-Protocol` request header,** which some proxies log. They are not in the URL, and the operator controls proxy logging.

## Open Questions

None blocking. If an untrusted client ever connects directly, it needs a credential issued per connection, which is a separate proposal (decision 18).

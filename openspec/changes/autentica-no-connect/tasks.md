# Tasks

## 1. Token set

- [ ] 1.1 Add the `subtle` dependency (`design.md` decision 4)
- [ ] 1.2 Create `src/auth.rs` with `Tokens`: `parse_tokens` over the `GATEWAY_AUTH_TOKENS` string (split on `,`, trim, skip empty, 1 to 128 bytes of the HTTP token charset, error naming the entry's position and never its value) and `Tokens::admits(&[u8])`, which compares against every token with `ConstantTimeEq` without stopping at the first match. A length mismatch may return early
- [ ] 1.3 Unit tests in `src/auth.rs`: empty and whitespace-only input give an empty set, entries are trimmed, a space, a control byte, a non-ASCII byte and 129 bytes are rejected, the error text contains the position and not the value, a prefix of a token is not admitted

## 2. Admission

- [ ] 2.1 `AppState` holds the immutable `Tokens`. `app` and `run` take it as a parameter, and nothing after admission can reach it
- [ ] 2.2 In `websocket_handler`, before `on_upgrade`: if the set is empty, proceed as today. Otherwise read `Authorization: Bearer <t>` first and, only if the header is absent, the offered protocols `bearer` and `<t>` through `requested_protocols`. Anything else returns `401`, `WWW-Authenticate: Bearer` and an empty body, the same for no credential, malformed and wrong
- [ ] 2.3 On a subprotocol admission, call `set_selected_protocol` with `bearer` and never with the token
- [ ] 2.4 Log one `warn` per refusal with the peer address and no credential material. Never log a token, nor its length
- [ ] 2.5 `src/main.rs` reads `GATEWAY_AUTH_TOKENS`, stops with the parse error, and logs the `authentication is off` `warn` line when the set is empty

## 3. Tests

- [ ] 3.1 Add a way for `tests/gateway.rs` to start a gateway with tokens, next to the existing helper that starts one without. The existing tests run unchanged with authentication off
- [ ] 3.2 One integration test per scenario in `specs/connect-authentication/spec.md` and the added scenario in `specs/connection-lifecycle/spec.md`. The refusal tests assert the status, `WWW-Authenticate` and an empty body, and that wrong, missing and malformed responses are identical. The query-string test asserts `401`
- [ ] 3.3 Mutation check before finishing: make `admits` return `true`, then make the header check ignore the subprotocol form, then make a refusal still send `welcome`. Each must turn at least one test red. Record the result in the PR
- [ ] 3.4 Replace every `planned` `Fonte:` and `Teste:` line in the deltas with `path:line` — `Symbol` references to the implemented code and tests

## 4. Documentation

- [ ] 4.1 Add decision entry `N` from `design.md` to `docs/decisions.md`, numbered after the last entry on `main` at that time
- [ ] 4.2 `README.md`: document `GATEWAY_AUTH_TOKENS`, the two credential forms with a browser example for the subprotocol, the TLS caveat, and remove "no authentication" from *Not implemented yet*
- [ ] 4.3 `docs/architecture.md`: say authentication happens at admission and what stays out of it. Keep the note on subscription authorization, which already points at decision 18, and say it is not planned
- [ ] 4.4 `openspec/config.yaml`: the Boundaries paragraph and the config line stop saying there is no auth and list `GATEWAY_AUTH_TOKENS`
- [ ] 4.5 `docs/roadmap.md`: mark milestone 2 as in progress with the PR link, and mark it done when merged

## 5. Verification

- [ ] 5.1 The author runs `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings` and `cargo test` locally
- [ ] 5.2 The author runs `scripts/bench.sh --quick` once and puts the numbers in the PR to show connection setup did not regress. `--goal` is not required, because `src/registry.rs` and the reader and writer in `src/ws.rs` are untouched
- [ ] 5.3 The reviewer confirms the CI run on the PR is green before merging. CI runs fmt, clippy and test, but nothing blocks a merge on red
- [ ] 5.4 The reviewer greps the diff and the test output for any token value, and checks the new code for comments

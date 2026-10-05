# Tasks

## 1. Tests

- [ ] 1.1 Add `frame_at_size_limit_is_relayed` to `tests/gateway.rs`: A sends a 65536-byte JSON string, B receives it in a `message` envelope with identical `data`
- [ ] 1.2 Add `oversized_frame_drops_the_sender_without_relaying` to `tests/gateway.rs`: A sends a 65537-byte JSON string; A's stream ends with no `Close` frame; B stays silent

## 2. Docs

- [ ] 2.1 Update README "Limits" to say the sender's connection is dropped without a close frame
- [ ] 2.2 Replace the requirement's `Teste:` line in the delta with the new tests and their `tests/gateway.rs:<line>`

## 3. Verify

- [ ] 3.1 Author runs `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings` and `cargo test` locally before opening the `spec:implementa` PR
- [ ] 3.2 CI runs the same three commands on that PR; `main` has no branch protection, so the reviewer confirms the run is green before merging
- [ ] 3.3 An independent session fills `verificacao.md`, including the discrimination sensor: change `MAX_MESSAGE_SIZE` to `64 * 1024 + 1` in an isolated worktree and confirm `oversized_frame_drops_the_sender_without_relaying` fails

# Tasks

## 1. Tests

- [x] 1.1 Add `text_payload_bytes_are_relayed_verbatim` to `tests/gateway.rs`: A sends `{"b":1,  "a":[ 1,2 ],"u":"é","n":1.50}` and then `  42  `; B's raw text frames equal `{"type":"message","from":"<A's id>","data":<exact payload>}` and `{"type":"message","from":"<A's id>","data":42}`

## 2. Spec

- [x] 2.1 Replace the requirement's `Teste:` line in the delta with the test and its `tests/gateway.rs:<line>`

## 3. Verify

- [x] 3.1 Author runs `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings` and `cargo test` locally before opening the `spec:implementa` PR
- [ ] 3.2 CI runs the same three commands on that PR; `main` has no branch protection, so the reviewer confirms the run is green before merging
- [ ] 3.3 An independent session fills `verificacao.md`, including the discrimination sensor: in an isolated worktree, round-trip `data` through `serde_json::Value` at `src/ws.rs:98` and confirm the new test fails while `payload_is_relayed_verbatim_to_others` still passes

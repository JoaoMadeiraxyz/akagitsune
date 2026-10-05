# Tasks

## 1. Helper

- [x] 1.1 Add the `send` helper to `tests/gateway.rs` as described in design.md

## 2. Call sites

- [ ] 2.1 Route every `.send(..)` in `tests/gateway.rs` through the helper, including tests merged by other changes before implementation; `rg '\.send\(' tests/gateway.rs` must only match the helper itself

## 3. Verify

- [ ] 3.1 Author runs `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings` and `cargo test` locally, plus `cargo test --test gateway` 20 times in a row, and records the suite time before and after
- [ ] 3.2 CI runs fmt, clippy and `cargo test` on that PR; `main` has no branch protection, so the reviewer confirms the run is green before merging
- [ ] 3.3 An independent session fills `verificacao.md`, including the discrimination sensor: in an isolated worktree, make publishing wait while the bus is nearly full (before the `tx_global.send` in `src/ws.rs`) and confirm every gateway test either passes or fails within 30 s, none hanging

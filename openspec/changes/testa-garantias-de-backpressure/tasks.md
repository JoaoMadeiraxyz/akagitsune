# Tasks

## 1. Tests

- [x] 1.1 Add `delivery_resumes_after_a_warning` to `tests/gateway.rs` as described in design.md
- [x] 1.2 Add `slow_receiver_does_not_hold_back_others` to `tests/gateway.rs` as described in design.md: chunks of 100 in lockstep with C, then B must hold a `warning`

## 2. Spec

- [ ] 2.1 Replace both requirements' `Teste:` lines in the delta with the tests and their `tests/gateway.rs:<line>`

## 3. Verify

- [ ] 3.1 Author runs `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings` and `cargo test` locally, and `cargo test --test gateway` 20 times in a row to check for flakiness, before opening the `spec:implementa` PR
- [ ] 3.2 CI runs fmt, clippy and `cargo test` on that PR; `main` has no branch protection, so the reviewer confirms the run is green before merging
- [ ] 3.3 An independent session fills `verificacao.md`, including discrimination sensors in an isolated worktree: replace `continue`-on-lag with `break` after the warning at `src/ws.rs:82` and confirm `delivery_resumes_after_a_warning` fails; make publishing wait on every subscriber's queue and confirm `slow_receiver_does_not_hold_back_others` fails

# Tasks

## 1. Test

- [x] 1.1 Add `dropped_count_matches_the_frames_skipped` to `tests/gateway.rs`, asserting the gap form and the balance form described in design.md, and requiring at least one warning

## 2. Spec

- [x] 2.1 Point the requirement's `Teste:` line in the delta to the new test with its `tests/gateway.rs:<line>`

## 3. Verify

- [x] 3.1 Author runs `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings` and `cargo test` locally, plus `cargo test --test gateway` 20 times in a row for flakiness, before opening the `spec:implementa` PR
- [ ] 3.2 CI runs fmt, clippy and `cargo test` on that PR; `main` has no branch protection, so the reviewer confirms the run is green before merging
- [ ] 3.3 An independent session fills `verificacao.md`, including discrimination sensors in an isolated worktree: send `dropped - 1` and `dropped + 1` in the warning at `src/ws.rs:84` and confirm the new test fails for both while `slow_consumer_receives_a_warning_frame` still passes

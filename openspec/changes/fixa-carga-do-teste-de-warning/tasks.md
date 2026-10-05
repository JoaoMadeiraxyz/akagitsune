# Tasks

## 1. Test

- [ ] 1.1 In `slow_consumer_receives_a_warning_frame`, send `128_000` frames instead of `BROADCAST_CAPACITY * 500`; `rg BROADCAST_CAPACITY tests` must return nothing

## 2. Verify

- [ ] 2.1 Author runs `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings` and `cargo test` locally, plus `cargo test --test gateway` 20 times in a row, before opening the `spec:implementa` PR
- [ ] 2.2 CI runs fmt, clippy and `cargo test` on that PR; `main` has no branch protection, so the reviewer confirms the run is green before merging
- [ ] 2.3 An independent session fills `verificacao.md`, including the discrimination sensor in an isolated worktree: set `BROADCAST_CAPACITY` to `1 << 18` and confirm `slow_consumer_receives_a_warning_frame` fails within 60 s (on `main` it runs past 90 s); set it to `512` and confirm the test still passes

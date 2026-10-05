# Tasks

## 1. Document

- [x] 1.1 Read the implementing code on `main` and write one requirement per observable behavior, with its `Fonte:` line
- [x] 1.2 Map each requirement to its test in `tests/gateway.rs`, or record it as a coverage gap

## 2. Verify

- [x] 2.1 Run `openspec validate --strict` locally before archiving (author, before opening the PR)
- [x] 2.2 Run `cargo test` locally to confirm the cited tests pass on `main` (author); CI runs fmt, clippy and `cargo test` on the PR, but `main` has no branch protection, so the reviewer confirms the run is green before merging

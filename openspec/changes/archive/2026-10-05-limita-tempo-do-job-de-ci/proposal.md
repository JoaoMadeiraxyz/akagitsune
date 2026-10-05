# Proposal

## Why

`.github/workflows/ci.yml` sets no `timeout-minutes`, so a stuck `check` job runs until the GitHub Actions default of 360 minutes. The send helper merged in `limita-tempo-de-envio-nos-testes` removed the known way the tests hung, but it only covers sends in `tests/gateway.rs`. A hang anywhere else (a receive loop with no overall deadline, a deadlock in a future test, a stuck `cargo` step) would still hold the job and its runner minutes for six hours before anyone sees a failure.

Measured from the last 45 runs of the CI workflow (`gh run list --workflow CI`): all succeeded, min 19 s, median 53 s, max 59 s. The first run in the history, with a cold cache, took about 45 s end to end, of which `cargo clippy` 17 s and `cargo test` 18 s.

## What Changes

- `timeout-minutes: 10` on the `check` job in `.github/workflows/ci.yml`.
- The verification section of `openspec/config.yaml` states the limit, so future tasks describe CI accurately.

No requirement changes (`skip_specs: true`). No change in `src/` or `tests/`.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

None.

## Impact

`.github/workflows/ci.yml` and `openspec/config.yaml`. Branch protection on `main` stays out of scope; it is a repository setting, not a file in this repo.

# Verification of limita-tempo-do-job-de-ci

Change with no requirements (skip_specs). Each goal of the proposal needs file:line or run evidence.

### `timeout-minutes: 10` on the `check` job, at job level
- evidence: `.github/workflows/ci.yml:14` — `timeout-minutes: 10`, indented as a sibling of `runs-on: ubuntu-latest` (`.github/workflows/ci.yml:13`) under `jobs.check` (`.github/workflows/ci.yml:12`).
- evidence: parsed with `ruby -ryaml`: `jobs.keys == ["check"]` and `jobs.check` without `steps` is `{"runs-on"=>"ubuntu-latest", "timeout-minutes"=>10}` (an integer, at job level, not on a step).
- evidence: `git diff main...HEAD -- .github/workflows/ci.yml` is a single added line; triggers, env and steps are unchanged.

### A hung job fails within minutes; normal runs never hit the limit
- evidence: the limit is 600 s. Over 47 completed past CI runs (`gh run list --workflow CI --limit 200`, all `success`), duration min 19 s, median 53 s, max 59 s. The PR's own run took 59 s. Margin between the slowest run and the limit: 541 s, about 10x the slowest run.

### GitHub accepts the workflow and the PR run used it
- evidence: run https://github.com/JoaoMadeiraxyz/akagitsune/actions/runs/37341574507, event `pull_request`, path `.github/workflows/ci.yml`, head branch `spec/implementa-limita-tempo-do-job-de-ci`, head SHA `b5df2489a725a1afb33364fbde1af856d0e29f21`, which equals this branch's HEAD and the PR's `headRefOid`. Job `check` concluded `success`.
- evidence: the workflow file fetched via `GET repos/JoaoMadeiraxyz/akagitsune/contents/.github/workflows/ci.yml?ref=b5df248...` contains `timeout-minutes: 10` at line 14.

### `openspec/config.yaml` states the limit and matches the workflow
- evidence: `openspec/config.yaml:37` — "The job fails after `timeout-minutes: 10` (slowest of 45 measured runs: 59 s)." Same value as `.github/workflows/ci.yml:14`. The 45-run figure was measured at proposal time; the recount here (47 runs) gives the same max of 59 s.

### Scope
- evidence: `git diff --stat main...HEAD` touches only `.github/workflows/ci.yml` (+1), `openspec/config.yaml` (+1) and `openspec/changes/limita-tempo-do-job-de-ci/tasks.md`. Nothing in `src/`, `tests/` or `examples/`. PR files list matches.
- evidence: `openspec validate --all --strict`: 4 passed, 0 failed.

## Discrimination sensor
- No local mutation applies. The timeout is enforced by the GitHub Actions runner, not by code in this repository, so there is no test suite that could detect its removal or a wrong value. Mutating the line locally and running `cargo test` would prove nothing.
- What the evidence proves: the key is at job level with the value 10 (YAML parse plus file:line), GitHub accepted the workflow file containing it (a run on that exact SHA started and finished green), and the configured limit sits well above every observed duration.
- What it does not prove: the timeout has not been observed firing. No run has hung or exceeded 10 minutes, so cancellation at 10 minutes is inferred from the documented behavior of `jobs.<id>.timeout-minutes`, not measured.

## Who ran it, and when
- checks: YAML parse, `git diff main...HEAD`, `openspec validate --all --strict` (passed), run on 2026-10-05 in the worktree at HEAD `b5df248`.
- CI run: https://github.com/JoaoMadeiraxyz/akagitsune/actions/runs/37341574507, success, 59 s (job `check` 56 s), head b5df2489a725a1afb33364fbde1af856d0e29f21
- historical runs: 47 completed runs, all success, min 19 s / median 53 s / max 59 s
- independent session: verifier subagent in a new session, without the implementation history

## Verdict
approved

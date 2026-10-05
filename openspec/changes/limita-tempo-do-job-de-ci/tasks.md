# Tasks

## 1. CI

- [x] 1.1 Add `timeout-minutes: 10` to the `check` job in `.github/workflows/ci.yml`, at job level next to `runs-on`

## 2. Context

- [x] 2.1 Add the 10-minute job limit to the verification section of `openspec/config.yaml`

## 3. Verify

- [x] 3.1 Author confirms locally that the workflow file still parses as YAML and that `timeout-minutes` sits under `jobs.check`, and that `openspec validate --all --strict` passes
- [x] 3.2 CI runs on the implementation PR; a run that starts and finishes green shows GitHub accepted the workflow. `main` has no branch protection, so the reviewer confirms the run is green before merging
- [x] 3.3 An independent session fills `verificacao.md`: placement of the key, the PR's CI run (link, duration, and that the job's recorded configuration came from this branch), and that `openspec/config.yaml` matches the workflow

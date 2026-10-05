# Design

## Context

One workflow, one job (`check`), triggered on push to `main` and on every pull request: checkout, Rust toolchain, `Swatinem/rust-cache`, then `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test`. Over 45 runs the job never exceeded 59 s.

## Goals / Non-Goals

**Goals:**

- A hung job fails within minutes instead of six hours.
- Normal runs, including a cold cache or a slow runner, never hit the limit.

**Non-Goals:**

- Per-step timeouts. The job-level limit is enough with three short steps; per-step limits would need tuning each time a step grows.
- Branch protection, which is a repository setting.

## Decisions

- **10 minutes, at job level.** Roughly ten times the slowest measured run, which absorbs a cold cache, a toolchain update recompiling everything, and a slow shared runner. A hang costs at most 10 minutes instead of 360.
- Record the limit in `openspec/config.yaml` next to the existing description of what CI runs, because that text is injected into every future artifact and must stay true.

## Risks / Trade-offs

- If the build or test suite grows past 10 minutes, CI fails with a timeout. That is visible and cheap to adjust deliberately; the measured margin is large.
- The limit cannot be exercised by a unit test. Evidence is the workflow run on the implementation PR itself, which only starts if GitHub accepts the workflow file, plus reading the key's placement.

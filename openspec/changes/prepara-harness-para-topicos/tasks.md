# Tasks

The `topics` protocol is the one in `roteia-por-topico/specs`. Start implementation only after that proposal is approved.

## 1. loadgen

- [x] 1.1 Add `--protocol legacy|topics` (default `legacy`). Under `legacy`, reject `--topics` other than 1 and reject `--churn`
- [x] 1.2 Topology (`topics`):
  - add `--topics T`, with connection `i` on `t-(i mod T)`, rejecting `connections % T != 0`;
  - make each sender publish to its own topic;
  - subscribe and await `subscribed` before the clock, counting `subscribe_failed`
- [x] 1.3 Expected deliveries: compute them per topic from acknowledged members, replacing `sent × (established − 1)`. Under `legacy` this is a single topic containing every connection
- [x] 1.4 Wire format:
  - `publish` frames under `topics`, and the bare stamp under `legacy`;
  - `--binary` with `[len][topic][stamp][padding]` under `topics`, and `[stamp][padding]` under `legacy`;
  - on the receive path, read `topic` and the binary header without building a `Value`
- [x] 1.5 Meters:
  - add `misrouted`, `dropped`, `unaccounted` and `unaccounted_scope`, and `subscribe_ack_p50_ms` / `subscribe_ack_p99_ms`;
  - follow the `null` rules for `legacy` from `design.md` decision 2;
  - add every meter to `--json` and describe them in `USAGE`
- [x] 1.6 Extra topic: add `--extra-topic-rate R` (`topics` only). Every measured connection also subscribes to `t-extra`, and one extra publish-only connection publishes to it. Report `extra_sent`, `extra_expected`, `extra_received` and `extra_delivery_pct` apart from the home-topic meters, and include them in `unaccounted` (`design.md` decision 1)
- [x] 1.7 Churn: add `--churn N --churn-rate R` (`topics` only), with `N` extra connections alternating subscribe/unsubscribe on `t-0` at `R` ops/s each during the measured window. Exclude them from `--connections`, `offered()`, expected deliveries and `subscribe_failed`, and report `churn_failed` per `design.md` decision 1
- [x] 1.8 Drain: replace the fixed 1 s `DRAIN` with drain-until-quiet (`DRAIN_QUIET = 500 ms`, `--drain-max-ms`, default 30 000). Report `drained` and `drain_seconds`, make `unaccounted` and `churn_failed` `null` when not drained, and leave `closed_early` connections out of `unaccounted` (`design.md` decision 4)
- [x] 1.9 Unit tests (`cargo test --examples`) for:
  - the drain stop rule (quiet period and limit);
  - expected deliveries with uneven acknowledgements;
  - the `T = 1` reduction to the old formula;
  - the `legacy` scope of `unaccounted`

## 2. refserver

- [x] 2.1 Add `--protocol legacy|topics`. `legacy` keeps today's behavior byte for byte. `topics` speaks the `roteia-por-topico` protocol, including acknowledgements and the binary header, through the single bus with a per-connection topic filter
- [x] 2.2 Add `--warn-drops` (the 1-in-K loss is reported with `warning` frames) and `--ignore-topics` (`topics` only: every publish goes to every connection). Keep delay, silent loss and freeze unchanged

## 3. calibrate.sh

- [x] 3.1 Run cases 1–4 under both protocols with their current predicted answers
- [x] 3.2 Add cases 5–13 from `design.md` decision 1. Cases 7–9, 11 and 12 run under both protocols, and cases 5, 6, 10 and 13 run under `topics` only. Case 7 under `legacy` uses the expected deliveries of the non-publishing connections only. Every predicted value comes from arithmetic
- [ ] 3.3 Get every case green. If one fails, fix the harness rather than widen a tolerance

## 4. bench.sh

- [x] 4.1 Add `--protocol legacy|topics` (default `legacy`), passed through to `loadgen`
- [x] 4.2 Change the scenario tuple to `name connections topics senders rate payload seconds [flags]`. A scenario that needs `topics` is skipped under `legacy` with a printed line
- [x] 4.3 CSV and output:
  - add the `protocol`, `topics`, `binary`, `churn`, `churn_rate`, `extra_topic_rate`, `extra_expected`, `extra_received`, `extra_delivery_pct`, `subscribe_failed`, `subscribe_ack_p99_ms`, `churn_failed`, `misrouted`, `dropped`, `unaccounted`, `unaccounted_scope`, `drained` and `drain_seconds` columns, the same list as `design.md` decision 1;
  - print the full invocation in the markdown table;
  - compute RSS per connection over every open connection
- [x] 4.4 Verdict:
  - `offered()` becomes `senders × rate × (conns / topics − 1)`;
  - add `incorrect` when `misrouted > 0`, `subscribe_failed > 0`, or, on a drained run, `unaccounted > 0` or `churn_failed > 0`, overriding every other verdict. Print `undrained` next to the verdict of a run that did not drain;
  - treat `null` meters as not measured, never as a pass
- [x] 4.5 Add the `topics-1k`, `topics-5k`, `binary-200`, `overlap-200`, `overlap-cliff` and `churn-500` scenarios to the baseline sweep and `goal-1m-topics` to the goal set, with the invocations from `design.md` decision 1

## 5. Re-baseline

- [ ] 5.1 On a quiet machine, run `scripts/calibrate.sh`, then `scripts/bench.sh --all --protocol legacy` against the current gateway on `main`

## 6. Documentation

- [ ] 6.1 `docs/architecture.md`:
  - replace the *Measured baselines* rows with the task 5.1 rows, moving the 2026-08-03 rows to a subsection titled as measured with the previous harness;
  - rewrite *Status* from the new rows, re-deriving the per-connection RSS and the cliff reading;
  - add calibration cases 5–13, with predicted and measured values, to *Harness calibration*, and describe drain-until-quiet;
  - generalize the definition of a delivery to `senders × rate × (connections / topics − 1)`
- [x] 6.2 `docs/decisions.md`: append entry 11 from `design.md` verbatim
- [x] 6.3 `.claude/skills/perf-check/SKILL.md`:
  - `--protocol`;
  - the new meters and how to read them;
  - the `incorrect` verdict and the `null` rule;
  - the new scenarios;
  - the delivery formula with topics
- [x] 6.4 `README.md`: add `--protocol` to the bench commands under Development

## 7. Verify

- [x] 7.1 Author runs `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test` (examples included) and `openspec validate --all --strict` locally before opening the PR
- [ ] 7.2 Author puts the calibration report and the re-baseline table in the PR. CI does not measure performance
- [ ] 7.3 CI runs fmt, clippy and `cargo test` on the PR. `main` has no branch protection, so the reviewer confirms the run is green before merging
- [ ] 7.4 An independent session fills `verificacao.md`:
  - each meter against its calibration case;
  - that `legacy` reproduces the cases 1–4 answers;
  - that no file in `src/` or `tests/gateway.rs` changed;
  - a grep of the diff for code comments

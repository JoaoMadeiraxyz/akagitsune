# Re-verification of prepara-harness-para-topicos

- date: 2026-10-06
- commit verified: `add2222` (main, after [akagitsune#29](https://github.com/JoaoMadeiraxyz/akagitsune/pull/29)). Harness code is unchanged since `0bc858b`; `3d6d368` only touched docs and `tasks.md`.
- session: independent verifier, no implementation history. Replaces the verification made at `6e2f55f`.
- scope: the change has no spec deltas (`skip_specs: true`, `.openspec.yaml`), so the requirements verified are the decisions and the case table of `design.md`. Benchmarks and `scripts/calibrate.sh` were not re-run, as instructed. Their records come from task 8.7 at `0bc858b` (calibrate 15 of 15). The only run done here is a synthetic one of `bench.sh`'s verdict awk (section 2).

Legend: met, partial, not met.

## 1. Requirements and scenarios (design.md)

Code lines are on `add2222`. Lines after `examples/loadgen.rs:546` moved by 2 since the earlier verification.

| Requirement | Code | Case or check | Status |
|---|---|---|---|
| Topology: connection `i` on `t-(i mod T)`, `connections % T != 0` rejected | `examples/loadgen.rs:916`, `examples/loadgen.rs:359` | case 5 (`scripts/calibrate.sh:274-285`) | met |
| Sender publishes to its own home topic | `examples/loadgen.rs:974` | case 5 | met |
| Subscribe and await `subscribed` before the clock, count `subscribe_failed` | `examples/loadgen.rs:18`, `examples/loadgen.rs:426`, `examples/loadgen.rs:453` | case 5 (0), case 14 (20, `scripts/calibrate.sh:322-329`) | met |
| Expected deliveries per topic from acknowledged members | `examples/loadgen.rs:847` (`expectation`) | unit tests `examples/loadgen.rs:1513`, `:1535`, `:1567`; case 5 (95,000) | met |
| Publish text and binary, borrowed read, no `Value` | `examples/loadgen.rs:497` (binary header), `examples/loadgen.rs:1625` (`binary_frames_round_trip`) | case 9 (`scripts/calibrate.sh:161-172`) | met |
| `misrouted` counted by publish stamp in the measured window, `null` under legacy | `examples/loadgen.rs:474-484`, `examples/loadgen.rs:527-531`, `examples/loadgen.rs:1236` | case 6 (900,000, `scripts/calibrate.sh:296`), case 5 (0) | met |
| `dropped` sums every `warning.dropped` | `examples/loadgen.rs:582` | case 8 (`scripts/calibrate.sh:227`), case 7 (0, `:210`) | met |
| `unaccounted` over the whole run, scope rule, `null` when undrained or scope empty | `examples/loadgen.rs:876`, `examples/loadgen.rs:1203-1207`, `examples/loadgen.rs:1233` | cases 7, 8, 11 (`scripts/calibrate.sh:210-229`, `:254`); unit test `examples/loadgen.rs:1590` | met |
| `subscribe_ack_p50_ms` and `p99`, `null` under legacy | `examples/loadgen.rs:1237-1238` | observed in case 5 (not asserted, by design) | met |
| Extra topic and its separate meters, included in `unaccounted` | `examples/loadgen.rs:22`, `examples/loadgen.rs:1240-1243` | case 13 (`scripts/calibrate.sh:309-320`) | met |
| Churn, `churn_failed`, churners outside `offered()` and `subscribe_failed` | `examples/loadgen.rs:708`, `examples/loadgen.rs:1234` | case 10 (`scripts/calibrate.sh:298-307`), case 15 (50, `:331-338`), case 11 (`null`, `:255-257`) | met |
| Drain until quiet, `--drain-max-ms`, `drained`, `drain_seconds` | `examples/loadgen.rs:19`, `examples/loadgen.rs:782`, `examples/loadgen.rs:798`; unit test `examples/loadgen.rs:1598` | cases 11 and 12 (`scripts/calibrate.sh:246-268`) | met |
| `refserver`: one bus, per-connection topic set, independent of the gateway registry | `examples/refserver.rs:202`, `examples/refserver.rs:252`, `examples/refserver.rs:414` | cases 5 and 6 | met |
| `refserver` `--warn-drops`, `--ignore-topics` | `examples/refserver.rs:252`, `examples/refserver.rs:266` | cases 8 and 6 | met |
| `refserver` `--drop-subscribe-acks`, `--drop-unsubscribe-acks` | `examples/refserver.rs:211-226`, `examples/refserver.rs:402-403` | cases 14 and 15 | met |
| Bus lag is fatal under topics, reported as before under legacy | `examples/refserver.rs:235-241` | none (no calibration case provokes a lag, by construction) | partial |
| Two protocols, `legacy` default, legacy rejects topic-only flags | `examples/loadgen.rs:283`, `examples/loadgen.rs:365-373`, `scripts/bench.sh:38-45` | cases 1-4 under both protocols | met |
| `legacy` reproduces cases 1-4 | `scripts/calibrate.sh:138-159`, `:201-244` | 15 of 15 in task 8.7 | met |
| Case 2 checks the delay the server held | `examples/refserver.rs:286-288`, `examples/refserver.rs:450-451`, `scripts/calibrate.sh:174-199` | case 2 (floor 0.49-0.50 ms, hold 51.2 ms, difference -0.13 ms, record of an earlier run in `/tmp/calibrate6.out`) | met |
| `bench.sh` tuple, CSV columns, scenarios, skip under legacy | `scripts/bench.sh:69-89`, `scripts/bench.sh:107`, `scripts/bench.sh:156-166` | newest local CSV (`bench-results/20261006-142007-all.csv`) has 16 rows and the 49 columns | met |
| `offered()` = `senders * rate * (conns / topics - 1)` | `scripts/bench.sh:271` | goal rows of the CSV | met |
| `incorrect` overrides everything, negative `unaccounted` included, `null` never triggers | `scripts/bench.sh:273-274`, `scripts/bench.sh:291-293` | synthetic run, see below | met |
| `undrained` printed next to the verdict | `scripts/bench.sh:307` | synthetic run | met |
| RSS per connection over every open connection | `scripts/bench.sh:178`, `scripts/bench.sh:218` | CSV `rss_per_conn_kib` 142.0-167.9 | met |
| Re-baseline from the calibrated harness under `legacy` | `docs/architecture.md:192-260` | `bench-results/20261006-142007-all.csv` | partial (see sections 3 and 5) |

Synthetic verdict run: the awk of `scripts/bench.sh:270-339`, run on rows derived from `fanout-200` with one meter edited each. Result: clean gives `pass`; `unaccounted -5` drained gives `incorrect`; `unaccounted 3` drained gives `incorrect`; `unaccounted -5` undrained gives `pass (undrained)`; `churn_failed 2` drained gives `incorrect`; `misrouted 1` gives `incorrect`.

## 2. Findings of the earlier verification and section 8 of tasks.md

| Item | Evidence on main | Status |
|---|---|---|
| Observation 1 and task 8.1: positive known answer for `subscribe_failed` and `churn_failed` | `examples/refserver.rs:211-226`, `scripts/calibrate.sh:128-130`, `scripts/calibrate.sh:322-338`; case 14 gave 20 and case 15 gave 50 in the recorded run | fixed |
| Observation 2: `misrouted` window narrower than the design text | `design.md` now states the window (diff of `design.md:59`); code unchanged at `examples/loadgen.rs:527-531` | fixed (task 8.5) |
| Observation 3 and task 8.7: record the baseline on an idle machine | `docs/architecture.md:217-221` says the machine was not idle, load average 3.4-11, and cliff repeats ran at 11-20 | not fixed (finding B4) |
| Observation 4: claims about the rebuilt loadgen that did not reproduce | `docs/architecture.md:272-290` replaced them with the 8.7 numbers; the 17 s and 62 s figures and the lower-p99 claim are gone | fixed, but the replacements have no artifact in the repo (finding B3) |
| Observation 5: `cliff-300` inference from CPU | the inference is gone; `docs/architecture.md:253-258` now says load-sensitive and to quote it with the load | fixed |
| Observation 6 and task 8.4: case 11 `churn_failed is null` only under topics | `scripts/calibrate.sh:255-257` | fixed |
| Observation 7 and task 8.2: lag under topics | `examples/refserver.rs:235-241` | fixed |
| Observation 7 and task 8.3: negative `unaccounted` is `incorrect` | `scripts/bench.sh:274`, `scripts/bench.sh:291-293`, synthetic run above | fixed |
| Observation 8: subscribe ack latency under delay | unchanged, cosmetic, not in section 8 | open, non-blocking |
| Observation 9: protocols of cases 11-15 | `design.md` case table and `scripts/calibrate.sh:246-338` agree | fixed (task 8.5) |
| Observation 10 and task 8.3: `churn_rate` default 1 | `scripts/bench.sh:174`, `examples/loadgen.rs:294` | fixed |
| Observation 11 and task 8.3: header comment | the 3-line `--protocol` header comment is gone from `scripts/bench.sh:10-16`; the remaining `#` lines (`scripts/bench.sh:69-70`, `scripts/calibrate.sh:231-232` and the header) are edits of comments that already existed on `main` at `dbe04ae` | fixed |
| Task 8.6: stop future created once | `examples/loadgen.rs:546-551` (`tokio::pin!(stopped)`, `_ = &mut stopped`) | fixed in code; no before/after measurement of this fix alone is recorded (the doc compares the pre-rebuild loadgen `2612f71` with `0bc858b`) |
| Task 8.6a: case 2 against the held delay | `scripts/calibrate.sh:174-199` | fixed |
| Task 8.7 | calibrate 15 of 15 and a bench run recorded; the CSV matches the doc rows | partial (not idle, finding B4) |
| Task 8.8 | `docs/architecture.md:192-260`, `:268-290` | partial (findings B1, B2) |
| Tasks 7.3 and 7.4 (still unchecked in `tasks.md`) | 7.3 CI is green on the merged PRs; 7.4 is covered by this file and the earlier one | left for the owner |

## 3. Claims in docs/architecture.md

Every number of the baseline tables (`docs/architecture.md:192-200`, `:227-233`) matches `bench-results/20261006-142007-all.csv` after rounding. For example, `fanout-200` 2.584/6.384 ms, `fanout-500` 12.384 ms, `cliff-300` 3,588,000 at 100% and p99 3.688 ms, `beyond-3m-explore` 2,994,931 at 99.831% with 500 warnings. Throughput equals `senders * rate * (connections - 1)` in every clean row. The 3.1x ratio is consistent with its inputs (69.3 / 22.25 = 3.1), and both CPU-per-frame ranges imply the same 13.0 million frames (13 measured seconds of 50 senders at 100 per second into 199 receivers is 12.9 million).

| Claim | Evidence | Status |
|---|---|---|
| Status: goal 3 of 3, stretch 5 of 6, service p99 5.1-6.0 ms, `beyond-3m-explore` is the first `cliff` | CSV rows and the bench awk | consistent |
| Padding 4 KiB costs about 1.3 ms at p50 | 3.848 - 2.584 = 1.264 ms (CSV) | consistent |
| Fanout p50 and p99 from 200 to 1000 connections | CSV | consistent |
| "p99 values vary by 2x between neighbouring rows of the same shape" | `fanout-200` 6.384 against `binary-200` 11.104 is 1.7x | slight overstatement |
| Old loadgen CPU per frame 5.30-5.37 us against 1.69-1.73 us, 3.1x | no artifact in the repo; recorded only in the commit message of `3d6d368` and in `/tmp` | no measurement kept |
| Old loadgen p99 6.26-6.67 ms against 6.16-7.02 ms | same | no measurement kept |
| `cliff-300` old 70.5-72.8% with 206, 481 and 868 warnings; new repeats 0, 80 and 5081 warnings at load 11-20 | same; the only old-harness rows kept are single runs at 75.0-77.4% (`bench-results/*.csv`) | no measurement kept |
| Cause of the old cliff is the per-frame timer | the repeat in 8.7 compares old with new loadgen as a whole; pinning the timer alone is not in the record | inference, not isolated |
| Machine "lightly loaded" for the clean `cliff-300` | the conditions list load average up to 11 | loosely worded |
| Calibration table, `docs/architecture.md:371-395` | lists cases 1-13 only | stale, see B1 |
| Case 2 row: "50 ms injected delay -> service p50 50 ms (+-2)" (`docs/architecture.md:377`) | the check is now `service p50 - (floor + hold)` (`scripts/calibrate.sh:190-193`) | stale, see B1 |
| "The 1.58 ms above the injected 50 ms is the measurement floor ... one loopback hop plus the timer granularity" (`docs/architecture.md:394-395`) | the case 2 record has a floor of 0.49-0.50 ms and a server hold of 51.2 ms, so about 1.2 ms of the 1.58 is the server's timer overshoot (`design.md` says so) | contradicted, see B1 |
| Row 1 "Server's own counter 597,000 / 597,000 (topics: plus 200 acknowledgements)" (`docs/architecture.md:376`) | topics predicts 597,200 and measures 597,000, inside the 0.5% tolerance (`scripts/calibrate.sh:154`) | unclear wording |
| `docs/decisions.md:207` says `refserver` gains only `--warn-drops` and `--ignore-topics` | entry 11 is verbatim from `design.md`, as task 6.2 requires, but the design adds two more faults | stale, non-blocking |

## 4. Rules

| Check | Result |
|---|---|
| Code comments in `*.rs` added since `dbe04ae` | none; the only matches are two string literals containing `ws://` |
| Comments in shell scripts | edits of comments that already existed on `main`; the header comment added by this change was removed (task 8.3) |
| Domain vocabulary (`username`, `room`, `chat`, `notification`, `player`) in the diff of code, scripts and docs | none |
| `src/` unchanged since `334cf1d` and `dbe04ae` | `git diff --stat` is empty |
| `cargo fmt --check` | pass |
| `cargo clippy --all-targets -- -D warnings` | pass |
| `cargo test` | pass: `tests/gateway.rs` 13, `examples/loadgen.rs` 14 |
| `openspec validate --all --strict` | 5 passed, 0 failed |

## 5. Blocking findings

- B1. `docs/architecture.md:371-395` (Harness calibration) is stale. It lists cases 1-13 while the doc says 15 passed (`docs/architecture.md:222`) and `perf-check` says 1-15. Cases 14 and 15 are missing. The case 2 row and the closing paragraph still describe the old absolute check, and the paragraph's cause (loopback and timer granularity) is contradicted by the floor and hold measured in case 2.
- B2. `docs/architecture.md:192-260`. The task 8.8 re-baseline dropped the gateway CPU, loadgen CPU, peak RSS and RSS per connection columns, and the per-connection RSS statement that task 6.1 requires to be re-derived. The data exists (`rss_per_conn_kib` 142.0-149.7 in the CSV), and `perf-check` tells the reader to use the CPU columns to detect a harness-bound row. In the new `cliff-300` row the gateway used 547% against 386% for the loadgen, which the doc does not mention.
- B3. The numbers behind the old-versus-new loadgen comparison and the `cliff-300` repeats (`docs/architecture.md:272-290`, `:253-258`) are not stored anywhere in the repo, so they cannot be checked. Either keep the raw runs (for example in `docs/`), or reduce the text to what `bench-results` supports.
- B4 (resolved by decision: measurements do not require an idle machine; task 8.7 and `design.md` now require recording the load average next to each run, which `docs/architecture.md` does). Task 8.7 asks for an idle machine and `design.md:194` forbids recording noisy runs, yet `docs/architecture.md:217-221` records load average 3.4-11 and the repeats ran at 11-20. The doc discloses it. Either re-run on an idle machine, or change 8.7 and `design.md` to accept the deviation explicitly.

Non-blocking: the `legacy` bus-lag branch of `refserver` has no case, observation 8 (ack latency under delay), `docs/decisions.md:207` omits the two new faults, task 8.6 has no isolated before/after, and the "2x" and "lightly loaded" wording.

## 6. Verdict

Not ready to archive. The harness code meets every decision and case of `design.md`, every finding of the earlier verification is fixed in code (cases 14 and 15 close the main one), and all gates pass. The remaining blockers are in the documentation recorded by tasks 8.7 and 8.8: B1 to B4 above. Task 8.9 stays unchecked.

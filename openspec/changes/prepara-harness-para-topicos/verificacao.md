# Verification of prepara-harness-para-topicos

Change with no requirements (`skip_specs: true`, `.openspec.yaml`). Each goal and decision of `design.md`, and task 7.4, needs `path:line` or run evidence. A claim without evidence counts as NOT covered.

Run numbers below come from this session (HEAD `6e2f55f`), in isolated worktrees with their own `CARGO_TARGET_DIR`, on a machine under load: `BTLEServer` at 99% of one core, plus `backupd` and `mdworker_shared` active, load average 22–49 during the network runs. Counts do not depend on load. Latency and CPU figures do.

### Decision 1, loadgen topology

- evidence: `examples/loadgen.rs:911-916`: `Protocol::Topics => format!("t-{}", index % topics)`. Connection `i` goes on `t-(i mod T)`.
- evidence: `examples/loadgen.rs:359-361`: `!config.connections.is_multiple_of(config.topics)` is rejected. Run: `loadgen --protocol topics --connections 10 --topics 3` exits 2 with `--topics must be at least 1 and divide --connections`.
- evidence: `examples/loadgen.rs:972`: `publishes_to: (i < config.senders).then_some(home)`. A sender publishes to its own home topic, so it is a member of the topic it publishes to.
- evidence: `examples/loadgen.rs:416-458` (`open`): it sends `subscribe` for each topic, waits up to `SUBSCRIBE_TIMEOUT` (`examples/loadgen.rs:18`, 10 s) for a matching `subscribed`, and returns `failed: pending.len()` (`examples/loadgen.rs:453`). This all happens before `Clock { start: Instant::now() ... }` (`examples/loadgen.rs:1035-1039`), so it stays outside the clock.
- evidence: `examples/loadgen.rs:995` sums `subscribe_failed`. `examples/loadgen.rs:1003-1009` keeps only acknowledged topics in the role, so an unacknowledged connection drops out of the expected arithmetic.

### Decision 1, expected deliveries per topic

- evidence: `examples/loadgen.rs:845-872` (`expectation`): for each topic the receiver acknowledged, it takes the frames sent to that topic minus the receiver's own (`examples/loadgen.rs:857-863`), and keeps the extra topic apart (`examples/loadgen.rs:864-868`). `examples/loadgen.rs:1146-1148` builds `sent_by_topic` from what each sender actually published.
- evidence: unit tests `single_topic_reduces_to_the_old_formula` (`examples/loadgen.rs:1510-1530`, asserts `senders * 500 * (connections - 1)`), `uneven_acknowledgements_shrink_only_their_topic` (`examples/loadgen.rs:1532-1562`) and `extra_topic_is_counted_apart` (`examples/loadgen.rs:1564-1585`). All 14 loadgen tests pass (gate below).
- evidence (run): case 5 on HEAD: `"sent":5000,"received":95000,"expected":95000,"delivery_pct":100.0000`. That is 5,000 × 19, as in the case table (`design.md:94`).

### Decision 1, publish and read

- evidence: `examples/loadgen.rs:633-637` writes `{"type":"publish","topic":"…","data":{"t":…,"s":…,"seq":…,"pad":"…"}}`. Under legacy, `examples/loadgen.rs:638-641` writes the bare stamp.
- evidence: `examples/loadgen.rs:617-627` writes binary as `[len][topic]` (topics only) + `due` u64 LE + `sent` u64 LE + padding.
- evidence: `examples/loadgen.rs:224-234`: the borrowed `Frame<'a>` reads `topic: Option<&'a str>` and `data: Option<Stamp>`, with no `Value`. `examples/loadgen.rs:495-504` reads the binary header, skipping the 16-byte sender id (`1 + len + 16`), which matches `openspec/changes/roteia-por-topico/specs/message-relay/spec.md:102`. `binary_frames_round_trip` (`examples/loadgen.rs:1622-1641`) covers it.
- evidence (run): case 9 (binary) passed under both protocols: 497,500 delivered, 0 malformed, `unaccounted` 0.

### Decision 1, `misrouted`

- evidence: `examples/loadgen.rs:474-487` (`classify`): a topic that is neither the home topic nor the joined extra topic is `Class::Misrouted`. `examples/loadgen.rs:527-531` counts it. A misrouted frame never increments `stats.all` or `measured`, so it is never a delivery. `examples/loadgen.rs:1234`: `misrouted_out = topics_mode.then_some(...)`, so it is `null` under legacy.
- calibration: case 5 predicts 0 and case 6 predicts `TOPIC_MISROUTED_EXP=$((TOPIC_SENT_EXP * (TOPIC_CONNS - 1 - (TOPIC_CONNS / TOPIC_COUNT - 1))))` (`scripts/calibrate.sh:123`) = 5,000 × 180 = 900,000. That is pure arithmetic and matches `design.md:95`. Asserted at `scripts/calibrate.sh:261` and `scripts/calibrate.sh:274`, with `received` still 95,000 at `scripts/calibrate.sh:272`.
- evidence (run): case 6 on HEAD: `"received":95000,"delivery_pct":100.0000,"misrouted":900000`.

### Decision 1, `dropped`

- evidence: `examples/loadgen.rs:578-581`: `stats.dropped += parsed.dropped.unwrap_or(0)` for each `warning`.
- calibration: case 7 predicts `dropped` 0 under silent loss (`scripts/calibrate.sh:190`). Case 8 predicts 10% of the scope's expected deliveries under topics, and `10 * 99 / 95` under legacy (`scripts/calibrate.sh:202-208`). The legacy figure is arithmetic: all 100 receivers report drops (95 × 5N + 5 × 4N = 495N) while the scope covers only the 95 non-publishers (475N), and 495/475 = 99/95.
- evidence (run): case 8 on HEAD, topics: `"dropped":29700,"scope_expected":297000` (10.0000%). Legacy: `"dropped":29700,"scope_expected":285000` (10.4211%).

### Decision 1 and 2, `unaccounted`, its scope and `null` rules

- evidence: `examples/loadgen.rs:1200-1206`: `unaccounted_sum += expect.all - received.all - received.dropped`, only for connections where `in_unaccounted_scope(...)` holds. `expect.all` and `received.all` include warmup and the after-window frames (`examples/loadgen.rs:869`, `examples/loadgen.rs:510`), so the whole run is covered, as `design.md:61` requires.
- evidence: `examples/loadgen.rs:874-876`: `!closed_early && (protocol == Protocol::Topics || !publishes)`. That gives legacy the non-publishers only, topics every connection, and leaves out connections that closed early (decision 2 and 4). `legacy_scope_excludes_publishers_and_closed_sockets` (`examples/loadgen.rs:1587-1593`) covers it.
- evidence: `examples/loadgen.rs:1231`: `unaccounted = (drained && scope > 0).then_some(unaccounted_sum)`. It is `null` when not drained, and `null` when the scope is empty (`ingest-50`). `unaccounted_scope` and `scope_expected` are emitted at `examples/loadgen.rs:1260`.
- calibration: case 7 (`scripts/calibrate.sh:187-193`) predicts a scope of 95 (legacy) or 100 (topics), and `unaccounted` 10% of `scope_expected`. That matches `design.md:96`. Case 8 predicts 0 (`scripts/calibrate.sh:209`), and so do cases 1, 5, 9, 10, 12 and 13.
- evidence (run): `bench-results/20261006-081045-all.csv`, `ingest-50` row: `unaccounted` `null`, `unaccounted_scope` 0.

### Decision 1, `subscribe_failed` and subscribe-ack latency

- evidence: `examples/loadgen.rs:1233`: `subscribe_failed_out = topics_mode.then_some(...)`. `examples/loadgen.rs:1235-1236` gives `subscribe_ack_p50_ms` and `subscribe_ack_p99_ms` from `ack_hist`, `null` under legacy.
- calibration: only case 5 predicts 0 (`scripts/calibrate.sh:263`). No case predicts a positive value. See observation 1 and mutant m6.

### Decision 1, extra topic

- evidence: `examples/loadgen.rs:962-964`: every measured connection also subscribes to `t-extra`. `examples/loadgen.rs:976-978` and `examples/loadgen.rs:1090-1100` add one extra publisher that subscribes to nothing (`open(..., Vec::new())`). `examples/loadgen.rs:1237-1241` gives `extra_sent`, `extra_expected`, `extra_received` and `extra_delivery_pct`, apart from `delivery` (`examples/loadgen.rs:1220-1224`, home only). `unaccounted` covers both, because `expect.all` sums every joined topic (`examples/loadgen.rs:869`).
- calibration: case 13 (`scripts/calibrate.sh:124-126`, `scripts/calibrate.sh:293-298`) predicts `EXTRA_SENT_EXP=10*10=100` and `EXTRA_EXPECTED_EXP=100*200=20000`, at 100% with `unaccounted` 0. That matches `design.md:102`. It passed on HEAD.

### Decision 1, churn and `churn_failed`

- evidence: `examples/loadgen.rs:690-717`: during the measured window, the churn loop alternates `subscribe` and `unsubscribe` on `t-0` at `period_micros`. Churners are extra roles (`examples/loadgen.rs:980-983`), are not counted in `established` (`examples/loadgen.rs:1021-1024`) or in `subscribe_failed` (they open with no topics), and their `t-0` frames are `Class::Ignored` (`examples/loadgen.rs:482-484`).
- evidence: `examples/loadgen.rs:1208-1211` and `examples/loadgen.rs:1232`: `churn_failed = (topics_mode && drained).then(|| churn_ops - churn_acks)`, which is `null` when not drained.
- calibration: case 10 predicts 100%, `unaccounted` 0, `churn_failed` 0 and `misrouted` 0 (`scripts/calibrate.sh:282-285`). Case 11 predicts `null` (`scripts/calibrate.sh:235`). No case predicts a positive `churn_failed` (observation 1).
- bench: RSS per connection divides by `OPEN_CONNS=$((CONNS + CHURN + EXTRA_SOCKETS))` (`scripts/bench.sh:182`, `scripts/bench.sh:222-223`).

### Decision 4, drain until quiet, `drained` and `drain_seconds`

- evidence: `examples/loadgen.rs:19` sets `DRAIN_QUIET_MICROS: u64 = 500_000`. `examples/loadgen.rs:553-556` makes each reader `fetch_max` the latest arrival into a shared `AtomicU64`. `examples/loadgen.rs:780-794` (`drain_state`) returns quiet → `Some(true)`, limit → `Some(false)`. `examples/loadgen.rs:796-831` (`coordinate`) waits for the writers, then polls until one of the two holds. `--drain-max-ms` defaults to 30 000 (`examples/loadgen.rs:295`). `drained` and `drain_seconds` are emitted at `examples/loadgen.rs:1261`. `drain_waits_for_quiet_and_gives_up_at_the_limit` (`examples/loadgen.rs:1595-1602`) covers the rule.
- calibration: case 11 (`scripts/calibrate.sh:226-235`) predicts `drained` false and `unaccounted` and `churn_failed` `null`. Case 12 (`scripts/calibrate.sh:237-246`) predicts `drained` true, `drain_seconds` ≥ 2, 100% and `unaccounted` 0. That matches `design.md:100-101` and `design.md:165`.
- evidence (run): case 11 on HEAD, topics: `"delivery_pct":85.0000,"unaccounted":null,"drained":false,"drain_seconds":0.516,"churn_failed":null`. Case 12: `"delivery_pct":100.0000,"unaccounted":0,"drained":true,"drain_seconds":2.516`.

### Decision 2, both protocols with `legacy` as the default

- evidence: the default `protocol: Protocol::Legacy` is at `examples/loadgen.rs:283`, `scripts/bench.sh:29` and `examples/refserver.rs:356`. `examples/loadgen.rs:365-375` rejects `--topics ≠ 1`, `--churn` and `--extra-topic-rate` under legacy. Runs: each exits 2 with `--topics needs --protocol topics`, `--churn needs --protocol topics` and `--extra-topic-rate needs --protocol topics`.
- evidence: under legacy, `misrouted`, `subscribe_failed`, the ack percentiles and `churn_failed` are `null` (`examples/loadgen.rs:1232-1236`). All 16 rows of `bench-results/20261006-081045-all.csv` show `misrouted`, `subscribe_failed` and `churn_failed` as `null`.

### `legacy` reproduces the cases 1–4 answers

- evidence: `git show main:scripts/calibrate.sh` lines 93-158 against `scripts/calibrate.sh:113-224`: the constants (`CONNS=200 SENDERS=5 RATE=50 SECS=12 WARMUP=2`), the loadgen flags of cases 1–4 and every assertion (labels, predicted values, tolerances and modes) are identical. The new script adds `case1 unaccounted` (`scripts/calibrate.sh:148`) and runs the block once per protocol (`scripts/calibrate.sh:130`).
- evidence (run, full `scripts/calibrate.sh` on HEAD in an isolated worktree, exit 0, `all calibration checks passed`), legacy:
  - `legacy case1 frames published 2500 2500 ±0 ok`
  - `legacy case1 frames delivered 497500 497500 ±0.05% ok`
  - `legacy case1 server-counted deliveries 597000 597000 ±0.5% ok`
  - `legacy case2 service p50 ms 50 51.584 ±2 ok`
  - `legacy case3 delivery % 90 90.0000 ±0.2 ok`
  - `legacy case4 response max ms (freeze-gap) 497 510.485 min ok`
- The same run under topics: case 1 gave 2500 / 497500; case 3 gave 90.0000; case 4 gave 513.064 ms. Every one of the 13 cases passed under each protocol where it applies.

### `refserver` independence and new faults

- evidence: there is one bus, `broadcast::channel(BUS_CAPACITY)` (`examples/refserver.rs:377`). Each connection keeps a `HashSet<Arc<str>>` of its topics (`examples/refserver.rs:186`) and drops non-members after `bus.recv()` (`examples/refserver.rs:221-229`). It has no registry and no per-topic structure, so the design stays different from the gateway's (`design.md:80-81`).
- evidence, `--ignore-topics`: `examples/refserver.rs:221`, `if behaviour.protocol == Protocol::Topics && !behaviour.ignore_topics`. The filter is skipped, so every publish reaches every other connection (the source filter at `examples/refserver.rs:209` remains). Run (case 6): `misrouted` 900,000 = 5,000 × 180.
- evidence, `--warn-drops`: `examples/refserver.rs:234-238`. For every Kth delivery it sends `warning(1)` instead of the frame. Run (case 8, topics): `"warnings":29700,"dropped":29700,"unaccounted":0`.
- evidence: the existing faults are unchanged. `git diff main...HEAD -- examples/refserver.rs` leaves `wait_out_stall`, the delay `sleep_until(arrival + delay)` and the `seen.is_multiple_of(drop_1_in)` counter untouched. The legacy text path builds the same `{"type":"message","from":…,"data":…}` envelope (`examples/refserver.rs:276-286`), and legacy binary is relayed as is (`examples/refserver.rs:288-292`).

### `bench.sh`: scenarios, CSV and verdict

- evidence: the tuple is `name connections topics senders rate payload seconds [flags]` (`scripts/bench.sh:73`, `scripts/bench.sh:167`). The new scenarios are at `scripts/bench.sh:82-87` and `scripts/bench.sh:93`, with the invocations of `design.md:111-117`: `topics-1k 1000 100 100 100`, `topics-5k 5000 500 500 100`, `binary-200 … --binary`, `overlap-200 … --extra-topic-rate 5`, `overlap-cliff 300 1 30 400 … --extra-topic-rate 5`, `churn-500 500 1 5 50 … --churn 200 --churn-rate 50`, `goal-1m-topics 2100 100 100 500` (100 × 500 × 20 = 1,000,000).
- evidence: under legacy, `needs_topics` (`scripts/bench.sh:111-119`) skips with a printed line (`scripts/bench.sh:169-172`). Run of that function over every scenario: `topics-1k`, `topics-5k`, `overlap-200`, `overlap-cliff`, `churn-500` and `goal-1m-topics` are skipped; `binary-200` and the 15 global-bus scenarios run. That matches the 16 rows of the newest CSV.
- evidence, CSV: `scripts/bench.sh:146-150` and `scripts/bench.sh:160` contain all 18 columns listed in `design.md:119`. The newest CSV header has them all. The markdown table prints the full invocation (`scripts/bench.sh:248-255`).
- evidence, verdict: `offered()` is `senders * rate * (conns / topics - 1)` (`scripts/bench.sh:275`). `incorrect` is the first branch (`scripts/bench.sh:294-296`), so it overrides every other verdict. `positive()` (`scripts/bench.sh:277`) is false for `null` and empty. `(undrained)` is appended at `scripts/bench.sh:310`.
- evidence (run): bench.sh's own verdict awk on synthetic rows derived from the fanout-200 row gave:
  - `clean -> pass`
  - `misrouted-1 -> incorrect`
  - `subfail-1 -> incorrect`
  - `unacc-1-drained -> incorrect`
  - `unacc-1-undrained -> pass (undrained)`
  - `unacc-null -> pass`
  - `churnfail-1-drained -> incorrect`
  - `churnfail-1-undrained -> pass (undrained)`
  - `misrouted-null -> pass`
  - `misrouted-1-undrained -> incorrect (undrained)`
  - `cliff-undrained -> cliff (undrained)`

### Decision 3, re-baseline and the status claims in `docs/architecture.md`

- evidence: every number in the new tables (`docs/architecture.md:192-200`, `docs/architecture.md:227-233`) matches `bench-results/20261006-081045-all.csv`, after rounding. For example, fanout-200 has p50 2.392 / p99 9.056 / response p99 11.744, CPU 18/26, 31.5 MiB and 145.4 KiB. cliff-300 has 3587927.4, 99.9980%, 7 warnings, 962 dropped, 46,644,000 expected, CPU 496/557. goal-1m-fanout/ingest/mesh have p99 8.864/6.384/7.408. beyond-3m-explore has 4.240.
- evidence: the derived statements check out.
  - p99 "6.4–8.9 ms" (`docs/architecture.md:142`).
  - RSS "145.4, 143.4, 142.1" (`docs/architecture.md:245`).
  - Padding cost 4.056 − 2.392 = 1.66 ms and 181.8 − 145.4 = 36.4 KiB (`docs/architecture.md:251-252`).
  - "drained in about 0.5 s": drain_seconds runs 0.500–0.521 in every row.
  - `unaccounted` 0 except `ingest-50` `null` (`docs/architecture.md:235-236`).
  - Throughput equals `senders × rate × (connections − 1)` in every non-cliff row.
  - "13 measured seconds": `measured_seconds` 13.
- evidence: running bench.sh's verdict awk (`scripts/bench.sh:246-341`) over that CSV gives the same verdicts as `docs/architecture.md:192-200`. It prints `1M goal: HIT (3 of 3 goal-1m-* scenarios passed SLO at offered load)` and `stretch: 6 of 6`, and `cliff-300` comes out `cliff`. The Status bullets (`docs/architecture.md:136-147`) are supported by that one run.
- evidence: "Gateway code identical to `main` at `dbe04ae`": `git diff --stat dbe04ae HEAD -- src tests Cargo.toml Cargo.lock` is empty. "No gateway code has changed since `334cf1d`" (`docs/architecture.md:276`): `git diff 334cf1d main -- src` is empty; `Cargo.toml` only gained the two `[[example]]` entries. Calibration at `26404a6` used case 8 `10 ±0.5` for both protocols (`git diff 26404a6 HEAD -- scripts/calibrate.sh`), which the legacy 10.42% passes. So "after `scripts/calibrate.sh` passed every case" (`docs/architecture.md:218-219`) is consistent with that commit.
- evidence: the 2026-08-03 rows moved under "Superseded: measured with the previous harness" (`docs/architecture.md:272-308`). `docs/decisions.md` entry 11 is verbatim from `design.md:178-184` (`diff` of the two extracts is empty).
- not verifiable from artifacts: "1-minute load average was 5.5 at the start" (`docs/architecture.md:221`). No calibration report file is kept. The calibration table (`docs/architecture.md:376-390`) agrees in counts with my own full calibration run, and in latency within normal spread (case 2 p50 51.584 / 51.840, case 4 510.485 / 513.064).

### Superseded explanation: the old loadgen's per-frame timer

- evidence: `git show main:examples/loadgen.rs` lines 279-281: `loop { tokio::select! { _ = sleep_until(TokioInstant::from_std(timeline.read_end)) => break,`. A new `Sleep` is created on every iteration, so once per received frame, as `docs/architecture.md:279-281` says. The new reader selects on `stop.changed()` instead (`examples/loadgen.rs:549`).
- re-measured (gateway built from HEAD, `--connections 201 --senders 50 --rate 100 --seconds 15`, 15,000,000 frames, `/usr/bin/time -p`):
  - old loadgen: user 11.49 + sys 51.04 = 62.5 s, and user 11.49 + sys 51.75 = 63.2 s. The claim is 62 s, 51 s kernel: reproduced.
  - old loadgen with only the timer pinned once: user 11.34 + sys 10.44 = 21.8 s. The claim is 13–15 s: same direction, higher under this load.
  - rebuilt loadgen: 14.28 + 17.06 = 31.3 s and 12.17 + 15.02 = 27.2 s. The claim is 17 s, about 1.1 µs/frame: not reproduced (observation 4).
  - `cliff-300`, old unchanged: `delivery_pct` 78.3956, 520 warnings, response p99 4702 ms. The claim is 76.5%, 750, 5.2 s: same shape.
  - `cliff-300`, old with the timer pinned: 100.0000%, 0 warnings, service p99 3.512 ms. The claim is 100%, 0, 3.5 ms: reproduced.
  - `cliff-300`, rebuilt: 99.9967%, 13 warnings, service p99 5.712 ms. This is consistent with the recorded row.

### Scope: nothing in `src/` or `tests/gateway.rs` changed

- evidence: `git diff --stat main...HEAD` touches only `.claude/skills/perf-check/SKILL.md`, `README.md`, `docs/architecture.md`, `docs/decisions.md`, `examples/loadgen.rs`, `examples/refserver.rs`, `openspec/changes/prepara-harness-para-topicos/tasks.md`, `scripts/bench.sh` and `scripts/calibrate.sh`. `git diff main...HEAD -- src tests | wc -l` prints `0`.

### No code comments, no domain vocabulary

- evidence: `git diff main...HEAD -- '*.rs' | grep -E '^\+' | grep -E '//|/\*'` matches only two string literals with URLs (`ws://127.0.0.1:3000/ws` in `USAGE`, `ws://127.0.0.1:{port}/ws` in a log line). There is no comment in either Rust file.
- evidence: `git diff main...HEAD | grep -E '^\+' | grep -inE 'username|\broom|chat|player|notification'` has no match (exit 1).

Observations (not blocking):

1. **`subscribe_failed` and `churn_failed` are not proven to detect anything.** No calibration case has a known positive answer for either one. Case 5 predicts `subscribe_failed` 0 (`scripts/calibrate.sh:263`). Case 10 predicts `churn_failed` 0 (`scripts/calibrate.sh:284`), and case 11 predicts `null`. Mutant m6 (`subscribe_failed` never counted) passed every case-5 assertion. A `churn_failed` that always reads 0 would also pass, by construction. This falls short of goal 2 (`design.md:18`) and of `design.md:45` ("each meter gets a calibration case with a known answer"), though the case table in `design.md:92-102` never asked for such a case either. Both meters feed `incorrect` (`scripts/bench.sh:294-295`), so a dead meter would hide a real defect once `roteia-por-topico` runs. Suggested fix: a `refserver` fault that withholds acknowledgements, for example for one topic or every Nth churn operation, plus a case with an arithmetic prediction.
2. **`misrouted` counts only frames stamped inside the measured window** (`examples/loadgen.rs:527-531`, `if in_window`). A leak during warmup or after the window is not counted. Case 6's 900,000 depends on this. It is narrower than the wording in `design.md:59` ("frames whose `topic` is not the receiver's").
3. **The re-baseline was recorded from a run on a loaded machine.** `docs/architecture.md:220-221` discloses it. `design.md:194` says "Numbers from a noisy run must not be recorded", and task 5.1 (`tasks.md:57`, "On a quiet machine") is checked. The goal verdicts are unlikely to flip under less load, but this is a deviation from the design's own rule.
4. **Doc claims about the rebuilt loadgen that did not reproduce here** (single runs, under load):
   - The rebuilt loadgen used 27–31 s of CPU, not 17 s (`docs/architecture.md:285`).
   - At `goal-1m-mesh` it measured a *higher* service p99 than the old one (8.144 / 7.792 ms against 7.440 / 7.248 ms). `docs/architecture.md:267-268` says it measured lower, and uses that to say the instrument does not explain the +1 ms clean-row latency. That argument is not supported by this re-measurement.
5. **The `cliff-300` inference** "the generator used 557% CPU against the gateway's 496%, so this run measured the gateway, not the harness" (`docs/architecture.md:259-260`) does not follow. In this session, at the same load:
   - the old loadgen with the timer pinned once delivered 100% with 0 warnings, on 51.7 s of CPU;
   - the rebuilt loadgen delivered 99.9967% with 13 warnings, on 80.1 s of CPU.

   So part of the residual warnings may still come from the generator.
6. `scripts/calibrate.sh:235` asserts `churn_failed` `null` under legacy, which always holds (`examples/loadgen.rs:1232`). Mutant m5 under legacy shows that assertion passing while the others fail. It is harmless but proves nothing for legacy.
7. `refserver` reports bus lag as `warning(dropped)` with the broadcast `Lagged(n)` count (`examples/refserver.rs:211-215`), and under topics that count includes other topics' frames and the connection's own. If `refserver` ever lagged, `dropped` would over-count and `unaccounted` would go negative. `bench.sh` never flags a negative `unaccounted` (`scripts/bench.sh:277`). Calibration loads stay far below `BUS_CAPACITY = 4096` (`examples/refserver.rs:21`), so the calibration results are not affected.
8. Subscribe acknowledgements go through `refserver`'s delayed queue (`examples/refserver.rs:203`, `examples/refserver.rs:253-255`), so under `--delay-ms 2000` `subscribe_ack_p50_ms` reads 2002.944 ms. This only matters when reading ack latency on delayed calibration cases.
9. `tasks.md:38` says cases 11 and 12 run under both protocols and 13 under topics only. `design.md:90` does not mention 11–13. The implementation follows `tasks.md`.
10. The CSV `churn_rate` column defaults to 0 when there is no churn (`scripts/bench.sh:178`), while loadgen's default is 1 (`examples/loadgen.rs:294`). This is cosmetic.
11. The shell scripts gained or edited header and inline `#` comments (`scripts/bench.sh:18-20`, `scripts/bench.sh:73-74`, `scripts/calibrate.sh:8-9`). The same style already existed on `main`, and the Rust files are clean.

## Discrimination sensor

Each mutation ran in its own worktree (`git worktree add --detach /tmp/vh-mN HEAD`) with its own `CARGO_TARGET_DIR=/tmp/vt-mN`, built with `cargo build --release --examples --bins` from the mutated copy. Each targeted case ran by hand: `refserver --port P --protocol X <flags>`, then `loadgen --url ws://127.0.0.1:P/ws --protocol X --json <flags>`, with the same flags `scripts/calibrate.sh` uses. The output was scored with `scripts/calibrate.sh`'s own `assert` and `pct_of` (copied verbatim from `scripts/calibrate.sh:76-111`) and its arithmetic (`scripts/calibrate.sh:113-126`). The unmutated HEAD build passed every one of these assertions. The real working tree was never mutated.

- mutation m1: `examples/loadgen.rs:529`, `stats.misrouted += 1` → `+= 0` (misrouted never counts).
- result: caught. Case 6 (topics, `--ignore-topics`): `topics case6 misrouted 900000 0 ±0 FAIL`.
- mutation m2: `examples/loadgen.rs:1205`, `expect.all - received.all - received.dropped` → `expect.all - received.all` (unaccounted ignores dropped).
- result: caught under both protocols. Case 8: `topics case8 unaccounted 0 29700 ±0 FAIL`; `legacy case8 unaccounted 0 28500 ±0 FAIL`.
- mutation m3: `examples/loadgen.rs:856`, the per-topic `sent.get(topic)` → the sum over every home topic (the old global-bus assumption, per receiver).
- result: caught. Case 5: `topics case5 delivery % 100 9.5477 ±0.01 FAIL`, `topics case5 unaccounted 0 1080000 ±0 FAIL`.
- mutation m4: `examples/loadgen.rs:815-816`, the drain stops immediately: `stop.send(true); return (true, 0.0)` right after the writers finish.
- result: caught by both drain cases.
  - Case 12: `drain seconds >= 2 2 0.000 min FAIL`, `delivery % 100 80.0000 FAIL`, `unaccounted 0 760 FAIL`.
  - Case 11: `drained (0 = false) 0 1 FAIL`, `unaccounted is null null 760 FAIL`, `churn_failed is null null 0 FAIL`.
- mutation m5: `examples/loadgen.rs:791`, the limit branch returns `Some(true)` (it never reports undrained).
- result: caught under both protocols. Case 11, topics: `drained 0 1 FAIL`, `unaccounted is null null 570 FAIL`, `churn_failed is null null 0 FAIL`. Case 11, legacy: `drained 0 1 FAIL`, `unaccounted is null null 570 FAIL`. The legacy `churn_failed` check passed, see observation 6.
- mutation m6 (chosen by the verifier): `examples/loadgen.rs:995`, `subscribe_failed += joined.failed` → `+= 0 * joined.failed`.
- result: survived. Case 5 passed all six assertions, including `topics case5 subscribe_failed 0 0 ±0 ok`. No calibration case can detect it (observation 1).

## Who ran it, and when

- gate: `cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test`. Result: pass, on 2026-10-06 in the real working tree at HEAD `6e2f55f725519ba66f34f4aa89928555b2ec476d`. fmt was clean, clippy printed no warnings, `tests/gateway.rs` gave 13 passed, `examples/loadgen.rs` gave 14 passed, and the lib and main suites have 0 tests.
- validation: `openspec validate --all --strict`: `Totals: 5 passed, 0 failed (5 items)`.
- calibration: full `bash scripts/calibrate.sh` in an isolated HEAD worktree printed `all calibration checks passed`, exit 0, every case 1–13 under each protocol where it applies. Each case took 14–18 s, for 20 case runs.
- targeted runs: cases 5, 6, 8 (both protocols), 11 and 12 on HEAD and on the mutants, as above.
- re-measurement: old, old with the timer pinned, and rebuilt loadgen against the HEAD gateway at `--connections 201 --senders 50 --rate 100` and at `cliff-300`, with `/usr/bin/time -p`.
- machine: Apple M4 Pro with background load. `BTLEServer` was at 99.1% of one core; `backupd` and `mdworker_shared` were active; the load average was 22–49 during the runs. Latency and CPU figures from this session carry that caveat.
- CI run: https://github.com/JoaoMadeiraxyz/akagitsune/actions/runs/37458329421, workflow `CI`, event `pull_request`, job `check`, success in 51 s, head SHA `6e2f55f725519ba66f34f4aa89928555b2ec476d`. That equals this branch's HEAD and the `headRefOid` of [akagitsune#27](https://github.com/JoaoMadeiraxyz/akagitsune/pull/27).
- independent session: verifier subagent in a new session, without the implementation history

## Verdict

approved with observations. Every meter and decision in `design.md` is implemented and calibrated with arithmetic predictions, `legacy` reproduces cases 1–4, the four required mutants were all caught, and the gate and CI are green. However, `subscribe_failed` and `churn_failed` have no case that proves they detect anything, and some of the re-baseline commentary (rebuilt-loadgen CPU and latency, the `cliff-300` reading) did not reproduce and was recorded on a loaded machine.

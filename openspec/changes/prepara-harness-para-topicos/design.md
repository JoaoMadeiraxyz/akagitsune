# Design

## Context

`docs/decisions.md` entry 10 records why the harness exists in its current form:
- an absolute publish schedule;
- service and response latency side by side;
- a log-linear histogram;
- calibration against `refserver` with known answers.

None of that changes. What changes is everything that assumed one global bus. Topic routing (`roteia-por-topico`) is the next gateway change, and measuring it with the current harness would give wrong numbers and miss wrong behavior.

## Goals / Non-Goals

**Goals:**

- The harness measures topic routing correctly: expected deliveries per topic, topic-partitioned scenarios, binary, membership churn.
- The harness detects routing and accounting errors, and each detector is proven by a calibration case with a known answer.
- `scripts/bench.sh` keeps working on `main` against the current gateway until `roteia-por-topico` lands.
- The current gateway gets a baseline measured with the new harness, so topic routing has a before/after comparison on the same instrument.

**Non-Goals:**

- Changing the gateway, its tests or its specs.
- Overlapping subscriptions (one connection in several topics) and publish-only connections. Those are fan-in shapes for a later scenario. This change covers one topic per connection.
- Running benchmarks in CI.

## Decisions

### 1. What has to change, and why


The harness was designed around the global bus, and that assumption runs through every layer of it, not just one formula:

| Where | Global-bus assumption |
|---|---|
| `examples/loadgen.rs` — `expected` | Every published frame reaches every other connection: `sent × (established − 1)`. |
| `examples/loadgen.rs` — `connect_one` | A connection is ready once it receives `welcome`. There is no subscribe step to wait for. |
| `examples/loadgen.rs` — `write_loop` | It publishes the bare stamp `{"t":…,"s":…}` as the whole frame. |
| `examples/loadgen.rs` — `read_loop` | It only knows `message`, `warning` and `error`, ignores binary frames, counts `warning` frames without adding up `dropped`, and has no notion of a frame arriving on the wrong topic. |
| `examples/refserver.rs` | The reference routes through one broadcast bus with a source filter. Its injected loss is silent, with no `warning`. |
| `scripts/calibrate.sh` | Every predicted value uses `CONNS − 1`. |
| `scripts/bench.sh` | Scenarios are `connections senders rate payload seconds`. The verdict's `offered()` is `senders × rate × (conns − 1)`. Nothing in the CSV can show a routing error. |

With topics, a run can be wrong in ways the old meters cannot see: a frame delivered to a non-subscriber, a frame lost without a `warning`, a subscriber missing from a topic. So the harness gains meters for those, and each meter gets a calibration case with a known answer.

**`loadgen`**

- **Topology.**
  - `--topics T` (default 1) puts connection `i` on topic `t-(i mod T)`. `connections` must be divisible by `T`.
  - Sender `j` publishes to the topic of the connection it is, so it is a member of the topic it publishes to.
  - Before the clock starts, every connection sends `subscribe` and waits for `subscribed`. A missing acknowledgement counts as `subscribe_failed`, and the connection is left out of the expected arithmetic.
- **Expected deliveries.** For each topic, the frames sent to it × (its acknowledged members − 1). With every connection acknowledged, this reduces to `senders × rate × measured_seconds × (connections / T − 1)`, which is today's formula at `T = 1`.
- **Publish.** `write_loop` sends `{"type":"publish","topic":…,"data":{stamp}}`. With `--binary`, it sends `[len][topic][stamp: two u64 little-endian][padding]` instead.
- **Read.**
  - `read_loop` decodes the envelope's `topic` and `data`, or the binary header, and drops neither path.
  - Text is parsed with the same borrowed decoding as today, so the timing path is unchanged.
- **New meters, all in `--json`:**
  - `misrouted`: frames whose `topic` is not the receiver's. It must be 0.
  - `dropped`: the sum of `dropped` over every `warning`.
  - `unaccounted`: over the whole run (warmup included, since a `warning` carries no timestamp), expected − received − dropped. Because the gateway's `warning` counts are exact (`delivery-backpressure`), a non-zero value means frames vanished without a warning. It must be 0.
  - `subscribe_ack_p50_ms` and `subscribe_ack_p99_ms`: the setup cost of joining topics, which for the gateway is the registry's copy-on-write, measured outside the clock.
- **Churn.** `--churn N` opens `N` extra connections that, during the measured window, alternately subscribe to and unsubscribe from topic `t-0` at one operation per second each. Their receptions are excluded from expected deliveries. The steady members' delivery and latency show what membership churn costs a publish (the copy-on-write risk in `roteia-por-topico` design decision 2).
- **Unit tests.** `cargo test --examples` covers the expected-delivery arithmetic for uneven acknowledgements, alongside the existing histogram tests.

**`refserver`**

- **Routing.** It speaks the new protocol, including acknowledgements and the binary header. Internally it keeps its single broadcast bus, and each connection filters by its own topic set.
  - This deliberately stays different from the gateway's registry. A reference that copies the code under test cannot catch that code's bugs.
  - It is slow at large `T`, which does not matter because it only runs calibration loads.
- **New injected faults, each with a known answer:**
  - `--warn-drops`: the existing 1-in-K loss is reported with `warning` frames.
  - `--ignore-topics`: delivers every publish to every connection, as the old bus did.
- **Existing faults.** Delay, silent loss and freeze keep their current behaviour.

**`calibrate.sh`**

The existing cases 1 to 4 run under both protocols at `T = 1` with their current predicted answers (2,500 published, 497,500 delivered, 90% under loss, ≥ 497 ms freeze). Holding them proves the rebuild did not move the instrument. Cases 7–9 also run under both protocols. Cases 5, 6 and 10 are `topics` only. New cases:

| Case | Setup | Predicted |
|---|---|---|
| 5 topics | 200 connections, `--topics 10`, 10 senders at 50/s, 10 s measured | 5,000 published, 95,000 delivered (5,000 × 19), 100%, `misrouted` 0, `unaccounted` 0 |
| 6 misrouting meter | case 5 against `--ignore-topics` | every publish reaches all 199 other connections: 95,000 correctly routed and `misrouted` = 900,000 (5,000 × 180), which shows the meter detects leaks |
| 7 silent loss | case 3 (1-in-10 loss, no warnings) | delivery 90%, `dropped` 0, `unaccounted` ≈ 10% of expected |
| 8 reported loss | case 3 with `--warn-drops` | delivery 90%, `dropped` ≈ 10% of expected, `unaccounted` 0 |
| 9 binary | case 1 with `--binary` | same counts as case 1 |
| 10 churn exclusion | case 5 with `--churn 20` | steady members' delivery 100% and `unaccounted` 0, so churners are excluded correctly |

**`bench.sh`**

- **Scenario tuple:** `name connections topics senders rate payload seconds [flags]`. The existing scenarios keep their names and offered load at `topics = 1` and run under either protocol. A scenario with `topics > 1` or `--churn` needs `--protocol topics`, and under `legacy` it is skipped with a printed line, not silently.
- **New scenarios:**

  | Scenario | Invocation | Offered | What it shows |
  |---|---|---|---|
  | `topics-1k` | `--connections 1000 --topics 100 --senders 100 --rate 100` | 90 000 deliveries/s | Topic routing at moderate size. |
  | `topics-5k` | `--connections 5000 --topics 500 --senders 500 --rate 100` | 450 000 deliveries/s | Same topic size, five times the connections: latency should stay flat if fanout is bounded by topic size. |
  | `binary-200` | `fanout-200` with `--binary` | same as `fanout-200` | Binary relay cost. Under `topics`, that includes building the header once per publish. |
  | `churn-500` | `fanout-500` with `--churn 100` | same as `fanout-500` | Membership churn under load. |
  | `goal-1m-topics` (goal set) | `--connections 2100 --topics 100 --senders 100 --rate 500` | exactly 1 000 000 deliveries/s across 100 topics of 21 | The goal delivery rate with fanout bounded by topic size. This is the row that shows whether topic routing pays off. |

- **CSV:** gains `topics`, `binary`, `churn`, `subscribe_failed`, `subscribe_ack_p99_ms`, `misrouted`, `dropped` and `unaccounted`. The markdown table prints the full invocation.
- **Verdict.** `offered()` becomes `senders × rate × (conns / topics − 1)`. A new verdict, `incorrect`, applies when `misrouted > 0`, `unaccounted > 0` or `subscribe_failed > 0`, and it overrides every other verdict: a fast run that routes wrongly is not a performance result.
- **RSS per connection** divides by every open connection, churners included.

### 2. Both protocols until topic routing lands

**Choice:** `--protocol legacy|topics` on `loadgen`, `refserver` and `bench.sh`, with `legacy` as the default. `roteia-por-topico` flips the default to `topics` and deletes `legacy` in the same PR that changes the gateway.

**Why:** if this change shipped only `topics`, `scripts/bench.sh` (named in `CLAUDE.md` and the `perf-check` skill) would fail against `main` from the moment this change merged until the gateway learned topics. Keeping `legacy` costs one branch in the frame writer and the frame reader, and it buys a usable `main` plus a same-instrument baseline of the current gateway.

Under `legacy`:

- The frame format is today's: a bare JSON stamp in, `{"type":"message","from":…,"data":…}` out, and binary relayed as is with the stamp in its first 16 bytes.
- `--topics` must be 1 and `--churn` is rejected. `misrouted` and `subscribe_*` are reported as `null`, not as 0, so a legacy row can never look like it passed a routing check.
- `unaccounted` is computed only over connections that do not publish. On the current gateway, a lagging publisher's `Lagged(n)` also counts its own frames, which the bridge would have skipped anyway. Its `dropped` therefore over-counts, and `unaccounted` would go negative for it. The existing exact-count test (`tests/gateway.rs:235`) uses a non-publishing receiver for the same reason. Scenarios where every connection publishes (`ingest-50`) report `unaccounted` as `null`, with `unaccounted_scope` naming how many connections were counted.

Under `topics`, the publisher never sends to its own inbox (`roteia-por-topico` design decision 4), so `dropped` never includes a connection's own frames and `unaccounted` covers every connection. That design drops the oldest frames and reports `Lagged(n)` as soon as the receiver reads again, so every discard is eventually reported. Even on an overloaded run, `unaccounted` stays 0 once the drain finishes. A design that dropped the newest frames and reported them only before the next delivered frame would leave the last discards unreported, and every overloaded run would wrongly come out `incorrect`.

**Alternative rejected:** shipping `topics` only and accepting a broken `bench.sh` on `main` until the gateway change. The window would have no end date, and the current gateway could never be measured with the new instrument.

### 3. Re-baselining the current gateway

The `perf-check` skill already states that a change to `loadgen` or `refserver` invalidates every baseline taken with the old one. So this change:

1. passes `scripts/calibrate.sh` (cases 1–10);
2. runs `scripts/bench.sh --all --protocol legacy` against the current gateway on `main`;
3. replaces the 2026-08-03 rows in *Measured baselines* with the new rows, with hardware, profile, commit and invocation. The 2026-08-03 rows move to a subsection titled as measured with the previous harness;
4. rewrites *Status* from the new rows.

These rows are the global-bus "before" of `roteia-por-topico`. That change marks them as the old architecture once it lands.

## Decision entry for `docs/decisions.md`

---

## 11. The harness measures routing correctness, not just speed

**Context.** The harness from entry 10 assumed one global bus: every frame reaches every other connection, a connection is ready on `welcome`, and nothing can be misrouted. Topic routing breaks each of those assumptions. It also makes new failures possible, such as a frame delivered to a non-subscriber or lost without a `warning`, which a speed-only harness reports as a clean pass.

**Decision.** `loadgen` computes expected deliveries per topic from acknowledged members. It reports `misrouted`, `dropped`, `unaccounted` and subscribe-acknowledgement latency, and it can publish binary frames and churn memberships. `refserver` gains `--warn-drops` and `--ignore-topics`, so each new meter has a calibration case with a known answer. `bench.sh` gains an `incorrect` verdict that overrides any other. Both the old and the topic protocol are supported until topic routing lands, so `main` stays measurable and the current gateway gets a same-instrument baseline.

**Consequence.** The 2026-08-03 baselines are superseded by a re-measurement with the new harness. A fast run that routes wrongly can no longer pass. The `legacy` protocol is temporary and is removed by the topic-routing change.

---

## Risks / Trade-offs

- **The `topics` side cannot be checked against the real gateway yet.** It is calibrated only against `refserver`. The first gateway run happens in `roteia-por-topico`, and any disagreement there is investigated, not tolerated.
- **Protocol drift.** If review changes the `topics` protocol in `roteia-por-topico`, this harness must follow. The proposal states the dependency, and implementation waits for that proposal's approval.
- **`refserver` grows.** It is still deliberately naive (one bus with per-connection filters), so its correctness stays obvious by reading it. That independence from the gateway's registry is the point of having it.
- **Temporary `legacy` branch.** This is a small amount of code with a known removal point, tracked as a task in `roteia-por-topico`.
- **Re-baselining takes a full `--all` run** on a quiet machine. Numbers from a noisy run must not be recorded.

# Proposal

## Why

Topic routing (change `roteia-por-topico`) replaces the global bus. The benchmark harness was built around that bus in every layer, not just in one formula:

- `examples/loadgen.rs` expects `sent × (established − 1)` deliveries, treats a connection as ready on `welcome`, publishes the bare stamp as the whole frame, ignores binary frames, and counts `warning` frames without adding up `dropped`.
- `examples/refserver.rs` routes through one broadcast bus, and its injected loss is silent.
- Every predicted value in `scripts/calibrate.sh` uses `CONNS − 1`.
- `scripts/bench.sh` scenarios have no topic, and its verdict computes `offered()` as `senders × rate × (conns − 1)`.

With topics, a run can also be wrong in ways the current meters cannot see: a frame delivered to a connection not subscribed to its topic, a frame lost without a `warning`, a subscriber missing from a topic. A harness that cannot see those would report a fast, broken gateway as a pass.

Rebuilding the harness is a change of its own. It is calibrated against `refserver` alone, so it can land and be trusted before the gateway learns topics. That keeps `roteia-por-topico` focused on the gateway.

## Scope verdict

**In scope.** This changes only measuring instruments (`examples/`, `scripts/`) and their documentation. The gateway's code and wire behavior stay as they are, and no payload is interpreted by the gateway. The harness reads its own stamp from `data`, as it already does.

## What Changes

- **Two protocols during the transition.** `loadgen`, `refserver` and `bench.sh` gain `--protocol legacy|topics`, with `legacy` as the default.
  - `legacy` is today's wire format: a bare JSON frame, relayed to everyone.
  - `topics` is the protocol defined in `roteia-por-topico`.
  - `scripts/bench.sh` keeps working against the current gateway on `main` the whole time. `roteia-por-topico` switches the default and deletes `legacy`.
- **New `loadgen` dimensions:**
  - `--topics T`, with expected deliveries computed per topic from acknowledged members;
  - `--binary`;
  - `--churn N`, for membership churn during the measured window.
- **New `loadgen` correctness meters:**
  - `misrouted`: frames on a topic the receiver is not subscribed to;
  - `dropped`: the sum of every `warning.dropped`;
  - `unaccounted`: frames that vanished without a `warning`;
  - `subscribe_failed` and the subscribe-acknowledgement latency.
- **`refserver`:** speaks both protocols and gains two faults with known answers, `--warn-drops` and `--ignore-topics`.
- **`calibrate.sh`:**
  - the existing cases keep their predicted answers under both protocols;
  - new cases prove each new meter against a known answer.
- **`bench.sh`:**
  - a `topics` column and the new meter columns;
  - topic scenarios that run only under `--protocol topics`;
  - a new `incorrect` verdict that overrides every other verdict when routing or accounting is wrong.
- **Re-baselining.** A harness change invalidates every baseline taken with the old harness (`perf-check`). So the current gateway is re-measured with the new harness under `legacy`, and those rows replace the 2026-08-03 table. They become the "before" of topic routing, measured with the same instrument that will measure the "after".

No requirement changes (`skip_specs: true`). No change in `src/` or `tests/gateway.rs`.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

None.

## Impact

- **Code:** `examples/loadgen.rs`, `examples/refserver.rs`, `scripts/calibrate.sh`, `scripts/bench.sh`, and `scripts/lib.sh` if a helper is needed.
- **Docs:**
  - `docs/architecture.md`: *Measured baselines*, *Status*, *Harness calibration*, and the definition of a delivery;
  - `docs/decisions.md`: a new entry 11;
  - `.claude/skills/perf-check/SKILL.md`;
  - the bench commands in `README.md`.
- **Dependency on `roteia-por-topico`.** The `topics` protocol implemented here is the one in that change's specs. This change should not be implemented before that proposal is approved. If the protocol changes during review, this change follows it.
- **Order:** this change lands first. `roteia-por-topico` then switches `bench.sh` to `topics`, removes `legacy`, and measures the gateway.

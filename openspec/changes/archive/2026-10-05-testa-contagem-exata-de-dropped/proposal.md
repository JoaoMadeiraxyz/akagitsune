# Proposal

## Why

"Lagging connections are warned of dropped frames" promises that `dropped` equals the number of frames discarded, but no test checks it: the tests only assert `dropped > 0`. The verification of `testa-garantias-de-backpressure` recorded this gap, and its design had listed the exact count as a non-goal on the assumption that it depends on scheduling.

That assumption was wrong. A throwaway probe against `main` (not committed) ran the existing overflow load five times: in every run the frames B received plus the sum of every `dropped` equaled exactly the frames A sent, and the jump in `seq` around each `warning` equaled its `dropped`. The count is deterministic, so the guarantee can be tested instead of weakened. Clients can rely on `dropped` to know how much they lost, which is worth keeping.

## What Changes

- New integration test asserting the exact count, in two forms: the `seq` gap around each `warning`, and the total balance of received plus dropped.
- The requirement gains the matching scenario and points to the test. Its text is unchanged.

No behavior change.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `delivery-backpressure`: "Lagging connections are warned of dropped frames" gains a scenario for the exact count and its test.

## Impact

`tests/gateway.rs` (new test), `openspec/specs/delivery-backpressure/spec.md`. No change in `src/`.

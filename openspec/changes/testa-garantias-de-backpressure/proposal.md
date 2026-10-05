# Proposal

## Why

Both requirements in `openspec/specs/delivery-backpressure/spec.md` are only partly protected. `slow_consumer_receives_a_warning_frame` asserts that a `warning` arrives with `dropped > 0`, but not that delivery continues after it. "Publishing never waits for a slow receiver" has no test at all and rests on reading `src/ws.rs:114` and `src/ws.rs:89`. A regression that stalls the bus behind one slow socket, or that stops delivery after a lag, would pass CI today.

## What Changes

- New integration test: after a `warning`, the lagging connection keeps receiving frames, in increasing order, up to a final marker sent after the burst.
- New integration test: with one connection that never reads, another connection receives the whole stream from a sender, including a final marker.
- Each requirement gains the scenario its new test asserts, and its `Teste:` line points to the tests.

No behavior change.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `delivery-backpressure`: both requirements gain a scenario and test coverage; observable behavior is unchanged.

## Impact

`tests/gateway.rs` (new tests), `openspec/specs/delivery-backpressure/spec.md`. No change in `src/`.

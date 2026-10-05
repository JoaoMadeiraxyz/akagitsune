# Proposal

## Why

`slow_consumer_receives_a_warning_frame` (`tests/gateway.rs:175`) computes its load as `realtime_gateway::state::BROADCAST_CAPACITY * 500` (`tests/gateway.rs:180`). The scenario it covers, "Slow consumer receives a warning" in `openspec/specs/delivery-backpressure/spec.md`, says A sends 128000 frames. The two only agree while the capacity is 256.

Because the load follows the constant under test, raising the capacity makes the test send more frames instead of exposing the change. Measured on `main` in an isolated worktree (not committed): with `BROADCAST_CAPACITY` set to `1 << 18` the test would send 131072000 frames, and it was still running when a 90 s alarm killed it. The verification of `testa-contagem-exata-de-dropped` reported the same run going past 10 minutes. None of the 5 s timeouts fire, because each send and each receive is fast; the loop is just enormous.

The other three burst tests in the file already use the literal `128_000`.

## What Changes

- `slow_consumer_receives_a_warning_frame` sends exactly 128000 frames, as its scenario states, and no longer reads `BROADCAST_CAPACITY`.

Test only. The scenario already says 128000, so no requirement changes and the change sets `skip_specs: true`. No change in `src/`.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

None.

## Impact

`tests/gateway.rs` only.

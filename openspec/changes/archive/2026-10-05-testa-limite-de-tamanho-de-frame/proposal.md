# Proposal

## Why

The frame size limit is a protocol behavior with no test. `openspec/specs/connection-lifecycle/spec.md` records it as confirmed only by a throwaway probe, so a change to `MAX_MESSAGE_SIZE` or to how axum handles oversized frames would pass CI unnoticed. The README also says only that oversized frames are "rejected at the WebSocket layer", without saying the sender's connection is dropped.

## What Changes

- Two integration tests in `tests/gateway.rs` lock the current behavior: a frame of exactly 65536 bytes is relayed, and a larger one resets the sender without a close frame and reaches nobody.
- The requirement's `Teste:` line points to those tests instead of "none".
- README "Limits" states that the sender's connection is dropped.

No behavior change. Replying with close code 1009 instead of a reset would be a behavior change and is not part of this change.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `connection-lifecycle`: "Frames above 64 KiB terminate the connection" gains test coverage; the observable behavior is unchanged.

## Impact

`tests/gateway.rs` (new tests), `README.md` (Limits paragraph), `openspec/specs/connection-lifecycle/spec.md`. No change in `src/`.

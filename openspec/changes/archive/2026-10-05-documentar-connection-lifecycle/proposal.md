# Proposal

## Why

The gateway has no consolidated spec yet. Before any behavior change is proposed, the existing behavior of this area has to be written down as it is, so future deltas modify a true baseline instead of an assumed one.

## What Changes

Documentation only. No code, test or protocol change. The requirements below describe the current behavior of `main`, each one traced to the code that implements it and, where one exists, to the test in `tests/gateway.rs` that asserts it.

## Capabilities

### New Capabilities

- `connection-lifecycle`: how a connection is accepted, identified and admitted (frame size limit).

### Modified Capabilities

None.

## Impact

Spec only: `openspec/specs/connection-lifecycle/spec.md`. Code described: `src/lib.rs`, `src/ws.rs`, `src/protocol.rs`.

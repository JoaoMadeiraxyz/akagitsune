# Proposal

## Why

Every receive in `tests/gateway.rs` goes through helpers with a 5 s timeout (`next_msg`, `assert_silent`), but every send is a bare `client.send(..).await.unwrap()`. When the gateway stops reading a client's socket, that send blocks forever once the TCP buffers fill, and the test hangs instead of failing.

Measured on `main` with a throwaway mutation in an isolated worktree (not committed): making publishing wait while the bus is nearly full (a regression of "Publishing never waits for a slow receiver") left `slow_consumer_receives_a_warning_frame` and `delivery_resumes_after_a_warning` hanging past a 40 s cutoff. `slow_receiver_does_not_hold_back_others` failed properly after 5 s, and `fifo_order_holds_across_a_multi_batch_burst` passed. `.github/workflows/ci.yml` sets no `timeout-minutes`, so in CI such a regression would hold the job until the GitHub Actions default of 6 hours instead of failing in seconds. The verification of `testa-garantias-de-backpressure` had already reported the same hang.

## What Changes

- A send helper in `tests/gateway.rs` with the same 5 s timeout as the receive helpers, failing with a message that says the send timed out.
- Every send in `tests/gateway.rs` goes through it, including tests added by other changes merged before this one is implemented.

Test infrastructure only. No requirement changes, so the change sets `skip_specs: true`. No change in `src/`.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

None.

## Impact

`tests/gateway.rs` only. Changing `.github/workflows/ci.yml` (for example adding `timeout-minutes`) is out of scope; it would be a separate decision about CI.

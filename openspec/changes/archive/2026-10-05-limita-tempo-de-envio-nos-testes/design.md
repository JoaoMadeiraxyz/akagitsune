# Design

## Context

Receive helpers: `next_msg` wraps `client.next()` in `tokio::time::timeout(Duration::from_secs(5), ..)` and `assert_silent` uses 200 ms. Sends: 15 call sites of `.send(..).await.unwrap()` on `main`, three of them inside 128000-frame loops.

## Goals / Non-Goals

**Goals:**

- No test in `tests/gateway.rs` can block forever on a send; a stalled gateway fails the test within 5 s of the send that stalled.

**Non-Goals:**

- A CI-level `timeout-minutes`. Useful as a backstop but a CI decision, kept out of this change.
- Changing what any test asserts.

## Decisions

- One helper, `send(client: &mut Client, msg: WsMessage)`, wrapping `client.send(msg)` in `tokio::time::timeout(Duration::from_secs(5), ..)` and panicking with `"timed out sending a frame"` on expiry, mirroring `next_msg`.
- 5 s per send, not per test. A healthy send on loopback completes far below that, and a per-test deadline would need tuning per load.
- Replace every call site, not only the burst loops, so new tests copy the safe pattern.

## Risks / Trade-offs

- A per-send timeout adds one timer per frame in the 128000-frame loops. The cost is small compared with the socket write it wraps; the author's 20-run check in tasks.md catches a meaningful slowdown.

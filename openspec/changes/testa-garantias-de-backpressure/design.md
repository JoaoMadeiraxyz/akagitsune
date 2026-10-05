# Design

## Context

The bridge task turns `Lagged(n)` into a `warning` and keeps looping (`src/ws.rs:82` — `handle_socket`); the reader publishes with a non-awaiting `broadcast::Sender::send` (`src/ws.rs:114`); a full local queue only blocks the bridge of that same connection (`src/ws.rs:89`). The existing overflow load is `BROADCAST_CAPACITY * 500` frames (`tests/gateway.rs:111`).

## Goals / Non-Goals

**Goals:**

- Fail CI if delivery stops after a lag, if order breaks across a lag, or if a non-reading connection holds back the others.

**Non-Goals:**

- Asserting the exact `dropped` count, which depends on scheduling and socket buffers.
- Performance numbers; that stays with `scripts/bench.sh`.

## Decisions

- **Delivery after warning:** reuse the overflow load from the existing test, then A sends `{"marker":"end"}`. B reads until the marker, asserting it saw at least one `warning`, that every `seq` it received is strictly greater than the previous one, and that the marker arrived. Strictly increasing rather than contiguous, because frames are dropped by design.
- **Non-reading receiver:** B connects, takes its welcome and stops reading. A sends 128000 frames in chunks of 100, and sends the next chunk only after C has received the previous one; then A sends the marker. C must receive every `seq` in order and the marker, each within the 5 s helper timeout. Chunks of 100 stay below the 256-frame bus capacity, so C cannot lag by construction and the expectation stays exact. If publishing waited on B, A would stall once B's socket buffers fill and C would time out.
- **The test proves its own precondition:** after the marker, B reads until it finds a `warning`. Without that, a run where B never actually fell behind would pass without testing anything.
- Both tests live next to `slow_consumer_receives_a_warning_frame`; that test stays as is.

## Risks / Trade-offs

- Lockstep chunks make the non-reading test slower than a free-running burst: 1280 round trips on loopback. Acceptable for an integration test; the chunk size can grow up to just under 256 if it becomes slow.

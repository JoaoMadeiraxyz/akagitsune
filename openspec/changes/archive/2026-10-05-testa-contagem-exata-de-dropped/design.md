# Design

## Context

The bridge turns `broadcast::error::RecvError::Lagged(n)` into `{"type":"warning","dropped":n}` and pushes it into the same per-connection queue as relayed frames, so the warning sits exactly where the skipped frames would have been (`src/ws.rs:82` — `handle_socket`). `n` is the number of messages the receiver skipped. Every frame on the bus therefore reaches a lagging connection either as a frame or inside one `dropped`.

Measured on `main` with a throwaway test (not committed), five runs of A sending 128000 `{"seq":i}` frames plus `{"marker":"end"}` to a lagging B:

| run | warnings | frames received | sum of `dropped` | received + dropped |
|---|---|---|---|---|
| 0–4 | 1 | 6720 | 121281 | 128001 |

In all five runs, each `seq` after a warning was exactly `previous seq + 1 + dropped`.

## Goals / Non-Goals

**Goals:**

- Fail CI if `dropped` is off by any amount, or if a frame is lost without being counted.

**Non-Goals:**

- Asserting how many warnings occur or where; that does depend on scheduling.

## Decisions

- **Gap form:** track the `dropped` accumulated since the last relayed frame (several warnings in a row add up). For each relayed `{"seq":s}`, assert `s == expected_next + accumulated`, where `expected_next` is the previous `seq + 1` (or 0 before the first one), then reset the accumulator.
- **Balance form:** at the marker, assert `frames received (marker included) + sum of dropped == 128001`. This catches a loss right before the marker that the gap form cannot see.
- A new test rather than extending `delivery_resumes_after_a_warning`, so each scenario maps to one test and a failure names the broken guarantee.
- The test requires at least one warning, so a run where B never lagged fails instead of passing vacuously.

## Risks / Trade-offs

- If the bridge ever coalesces or splits warnings, the gap form still holds because it accumulates; only a wrong count fails it, which is the point.

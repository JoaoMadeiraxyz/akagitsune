# Design

## Context

`tests/gateway.rs` has four 128000-frame bursts. Three use `128_000` directly (`delivery_resumes_after_a_warning`, `dropped_count_matches_the_frames_skipped`, `slow_receiver_does_not_hold_back_others`). Only `slow_consumer_receives_a_warning_frame` derives it from `BROADCAST_CAPACITY`, and it is the only use of that constant in `tests/` and `examples/`.

## Goals / Non-Goals

**Goals:**

- The test's load is fixed by its scenario, not by the code under test.
- A change to `BROADCAST_CAPACITY` makes the test finish in bounded time: it passes while 128000 frames still overflow the bus, and fails through the existing 5 s receive timeout once they no longer do.

**Non-Goals:**

- Changing what the test asserts, or its name.
- Tying the other tests to the capacity in any way.

## Decisions

- Replace the computed `overflow` with the literal `128_000`, matching the other burst tests. A named constant shared by the four tests would also work, but the spec states the number per scenario, and each test reading as its scenario is worth more than removing three repeated literals.

## Risks / Trade-offs

- If the capacity is ever raised above roughly 128000, this test stops proving lag and fails with a receive timeout. That is the intended signal: the scenario would need rethinking, deliberately.

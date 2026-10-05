# Design

## Context

Measured on `main` with a throwaway test (not committed): sending a 65538-byte text frame made the client stream yield `Err(Protocol(ResetWithoutClosingHandshake))` and then end, and the other connection received nothing within 500 ms. A 65536-byte frame was relayed (`src/ws.rs:20` — `MAX_MESSAGE_SIZE`, applied at `src/ws.rs:28` — `websocket_handler`).

## Goals / Non-Goals

**Goals:**

- Fail CI if the limit moves or if an oversized frame starts being relayed or answered with a close frame.

**Non-Goals:**

- Changing the termination to a close frame with code 1009. If wanted, it is a separate change that MODIFIES this requirement.

## Decisions

- Payloads are JSON strings padded to the exact byte count (`"` + `x` * (n - 2) + `"`), so the boundary is exact and the frame is valid JSON on both sides of it.
- The oversized test asserts that the sender never receives a `Close` frame and that its stream ends within the existing 5 s helper timeout, rather than matching the tungstenite error variant, so it does not depend on the client library's error naming.
- The receiver side reuses `assert_silent`.

## Risks / Trade-offs

- An axum or tungstenite upgrade that starts sending a close frame on oversize will fail the test. That is intended: the spec says no close frame, so the spec has to be changed deliberately.

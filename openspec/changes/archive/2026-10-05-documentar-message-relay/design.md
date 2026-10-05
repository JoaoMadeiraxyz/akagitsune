# Design

## Context

Baseline change: the spec describes behavior that already exists. Sources were read from `main` and each requirement carries a `Fonte:` line in the `path:line` — `Symbol` format.

## Goals / Non-Goals

**Goals:**

- Record current wire behavior as a client observes it.
- Name the test that covers each requirement, or state that none does.

**Non-Goals:**

- Changing behavior, adding tests, or fixing gaps found while documenting. Gaps are listed in the PR and become their own changes.

## Decisions

- Requirements describe what a WebSocket client observes, not task structure, per `openspec/config.yaml`.
- Behavior without a test is documented only when it was confirmed by reading the code and, for the frame size limit, by a throwaway probe against a running gateway that was not committed.

## Risks / Trade-offs

- A requirement without a test can regress silently. Each one is called out in the PR as a coverage gap rather than hidden.

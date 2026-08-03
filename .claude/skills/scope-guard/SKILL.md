---
name: scope-guard
description: Decide whether a proposed change belongs in the realtime-gateway before implementing it. Use when adding or extending a feature, route, protocol field, message type, config option or dependency, and whenever a request mentions users, rooms, chat, notifications, history, presence, moderation, or anything about message content. Returns an in/out/borderline verdict with rationale.
---

# scope-guard

This project is a **generic** realtime WebSocket gateway. It manages connections
and relays messages. It never learns what a message means. It already drifted
once into being a chat server, so scope is enforced deliberately rather than
assumed.

Run this **before** writing code, not after.

## The test

> A feature belongs in this gateway only if it can be implemented **without
> knowing what the payload means**.

Apply it literally: if the implementation must read a field inside `data`,
validate its shape, or branch on its value, the answer is out of scope.

## Procedure

1. Read `docs/scope.md`. The in/out lists and the borderline table there are
   authoritative; this file is the procedure, not the source of truth.
2. State what the change would require the gateway to know. Be concrete — name
   the field or the decision the gateway would have to make.
3. Apply the test and pick a verdict.
4. Check the noun test as a cross-check: would this introduce a type, field,
   constant, error message or log line containing domain vocabulary
   (`username`, `room name`, `chat`, `player`, `notification`, `message body`)?
   If yes, that is a strong signal of out-of-scope even if step 3 looked clean.

## Verdicts

**In scope** — payload-agnostic, and keeps the gateway stateless, cheap and
small. Proceed. Point at the hot-path invariants in `docs/architecture.md` that
the implementation must respect, and require an integration test in
`tests/gateway.rs`.

**Out of scope** — say so plainly, name which rule it breaks, and describe where
it should live instead: in the application that consumes the gateway, speaking
its own protocol inside `data`. Do not implement a "small" version as a
compromise. A narrow exception is how the chat drift started.

Then look for the generic sibling — the *Find the generic sibling* table in
`docs/scope.md`. Most out-of-scope requests have a payload-agnostic version
underneath (`username` → opaque connection metadata; "send only to Alice" →
delivery by connection id). Offer it, note that it usually lands in borderline
territory, and let the user choose. A flat no with no alternative is how the
same request returns later in disguise.

Note that step 3 alone will not catch every domain leak. A `username` field at
connect never touches `data`, so it passes the literal test and is caught only
by step 4. When steps 3 and 4 disagree, step 4 wins.

**Borderline** — payload-agnostic but expands the project materially: adds
retained state, per-message bookkeeping, a second wire format, or a new failure
mode. `docs/scope.md` lists the known ones (replay buffer, acknowledgements,
persistence, predicate subscriptions, alternative encodings).

For borderline cases: **stop and ask the user.** Present the trade-off — what it
buys, what it costs structurally — and let them decide. Do not resolve it
silently in either direction. If they approve, write the entry in
`docs/decisions.md` (context, decision, consequence) before implementing.

## Output

Keep it short. Verdict, the one-sentence reason, and the next action. If out of
scope, include where the feature does belong — an unhelpful refusal is worse
than the drift.

Do not soften an out-of-scope verdict because the request was insistent. Report
the verdict, then follow the user's decision if they override it — and if they
do, record it in `docs/decisions.md`, because a deliberate scope change is a
decision, not an accident.

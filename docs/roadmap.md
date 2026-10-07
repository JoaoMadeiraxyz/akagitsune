# Roadmap

What is done, and what is planned next, in the order it is planned. Every item still has to pass the scope test in `docs/scope.md` before it is implemented. An item moves to *Done* with the date its implementation merged.

## Done

| # | Milestone | Reached | Notes |
|---|---|---|---|
| 1 | **Topic routing** | 2026-10-06 | A publish reaches only the subscribers of its topic. Implemented in [akagitsune#33](https://github.com/JoaoMadeiraxyz/akagitsune/pull/33), specs consolidated in [akagitsune#34](https://github.com/JoaoMadeiraxyz/akagitsune/pull/34). Decisions 12 to 15 in `docs/decisions.md`. |

## Planned, in order

| # | Milestone | Status | Notes |
|---|---|---|---|
| 2 | **Connect-time authentication** | Not started, no change opened | Admission control, in scope. `docs/decisions.md` entry 4 fixes where it goes: at connect time (headers, query string or a subprotocol), never as an in-band frame. |
| 3 | **Subscription authorization** | Not started, no change opened | Named in `docs/architecture.md` as the expected next step: today any connection can read any topic it can guess, and `subscribe` is the single place where a check goes. Open question for its proposal: how the gateway learns what a connection may subscribe to without learning any domain vocabulary. |
| 4 | **Publish pacing** (`delay_ms`) | Proposed in [akagitsune#35](https://github.com/JoaoMadeiraxyz/akagitsune/pull/35) | Starts only after 2 and 3 are done. Before it is kept, the proposal's verification tasks still apply: repeated alternating `scripts/bench.sh --quick` runs to show the lookahead is cheap, and a mutation check of the disconnect tests. Decision 17 in `docs/decisions.md` on the proposal branch. |
| 5 | **Per-connection rate limiting** | Not started, no change opened | Admission control, in scope. Listed in `README.md` under *Not implemented yet*. |

## Under consideration

Not planned yet. Each one needs a decision before it can be planned.

| Milestone | Notes |
|---|---|
| **Delivery acknowledgements** | Listed as a borderline case in `docs/scope.md`: payload-agnostic, but it forces per-message state and a retry path where today there is none. It needs an entry in `docs/decisions.md` before any proposal, and it has to be weighed against the rule that a connection holds nothing that outlives it. |

## Not scheduled

Known gaps with no position in the order yet (`README.md`, *Not implemented yet*). They are listed so they are not forgotten, not because they are planned.

- A backplane for running more than one instance (`docs/architecture.md`, *What would have to change*).

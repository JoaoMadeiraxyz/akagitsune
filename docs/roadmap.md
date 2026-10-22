# Roadmap

What is done, and what is planned next, in the order it is planned. Every item still has to pass the scope test in `docs/scope.md` before it is implemented. An item moves to *Done* with the date its implementation merged.

## Done

| # | Milestone | Reached | Notes |
|---|---|---|---|
| 1 | **Topic routing** | 2026-10-06 | A publish reaches only the subscribers of its topic. Implemented in [akagitsune#33](https://github.com/JoaoMadeiraxyz/akagitsune/pull/33), specs consolidated in [akagitsune#34](https://github.com/JoaoMadeiraxyz/akagitsune/pull/34). Decisions 12 to 15 in `docs/decisions.md`. |
| 2 | **Connect-time authentication** | 2026-10-22 | A connection is admitted only with a configured token, sent as a bearer header or a subprotocol, never as an in-band frame. Implemented in [akagitsune#39](https://github.com/JoaoMadeiraxyz/akagitsune/pull/39), proposal [akagitsune#37](https://github.com/JoaoMadeiraxyz/akagitsune/pull/37), specs consolidated in [akagitsune#40](https://github.com/JoaoMadeiraxyz/akagitsune/pull/40). Decision 19 in `docs/decisions.md`. |
| 3 | **Publish pacing** | 2026-10-22 | A text publish may carry `delay_ms` (up to 120 s); the connection's reader waits before fanning out, with no queue. Implemented in [akagitsune#41](https://github.com/JoaoMadeiraxyz/akagitsune/pull/41), proposal [akagitsune#35](https://github.com/JoaoMadeiraxyz/akagitsune/pull/35), specs consolidated in [akagitsune#42](https://github.com/JoaoMadeiraxyz/akagitsune/pull/42). Decision 17 in `docs/decisions.md`. |

## Planned, in order

| # | Milestone | Status | Notes |
|---|---|---|---|
| 4 | **Per-connection rate limiting** | Not started, no change opened | Admission control, in scope. Listed in `README.md` under *Not implemented yet*. |

## Under consideration

Not planned yet. Each one needs a decision before it can be planned.

| Milestone | Notes |
|---|---|
| **Subscription authorization** | Removed from the plan, see decision 18 in `docs/decisions.md`. The gateway is self-hosted and only the operator's own services are meant to reach it, so an admitted client is already trusted. It comes back only if an untrusted client, such as a browser, connects directly, which needs a credential issued per connection. |
| **Delivery acknowledgements** | Listed as a borderline case in `docs/scope.md`: payload-agnostic, but it forces per-message state and a retry path where today there is none. It needs an entry in `docs/decisions.md` before any proposal, and it has to be weighed against the rule that a connection holds nothing that outlives it. |

## Not scheduled

Known gaps with no position in the order yet (`README.md`, *Not implemented yet*). They are listed so they are not forgotten, not because they are planned.

- A backplane for running more than one instance (`docs/architecture.md`, *What would have to change*).

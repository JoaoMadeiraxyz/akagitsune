# Roadmap

What is done, and what is planned next, in the order it is planned. Every item still has to pass the scope test in `docs/scope.md` before it is implemented. An item moves to *Done* with the date its implementation merged.

## Done

| # | Milestone | Reached | Notes |
|---|---|---|---|
| 1 | **Topic routing** | 2026-10-06 | A publish reaches only the subscribers of its topic. Implemented in [akagitsune#33](https://github.com/JoaoMadeiraxyz/akagitsune/pull/33), specs consolidated in [akagitsune#34](https://github.com/JoaoMadeiraxyz/akagitsune/pull/34). Decisions 12 to 15 in `docs/decisions.md`. |
| 2 | **Connect-time authentication** | 2026-10-22 | A connection is admitted only with a configured token, sent as a bearer header or a subprotocol, never as an in-band frame. Implemented in [akagitsune#39](https://github.com/JoaoMadeiraxyz/akagitsune/pull/39), proposal [akagitsune#37](https://github.com/JoaoMadeiraxyz/akagitsune/pull/37), specs consolidated in [akagitsune#40](https://github.com/JoaoMadeiraxyz/akagitsune/pull/40). Decision 19 in `docs/decisions.md`. |
| 3 | **Publish pacing** | 2026-10-22 | A text publish may carry `delay_ms` (up to 120 s); the connection's reader waits before fanning out, with no queue. Implemented in [akagitsune#41](https://github.com/JoaoMadeiraxyz/akagitsune/pull/41), proposal [akagitsune#35](https://github.com/JoaoMadeiraxyz/akagitsune/pull/35), specs consolidated in [akagitsune#42](https://github.com/JoaoMadeiraxyz/akagitsune/pull/42). Decision 17 in `docs/decisions.md`. |

## MVP

The first production use: one instance, behind a proxy that terminates TLS, reached only by the operator's own services, with an application that tolerates at-most-once delivery (no replay after a disconnect or a lag drop). The items below are what that use still needs. Each one still passes the scope test and gets its own proposal.

| # | Milestone | Status | Notes |
|---|---|---|---|
| 4 | **Heartbeat and idle timeout** | Not started, no change opened | The gateway sends protocol pings and closes a connection that does not answer in time. Today a peer that vanishes without a FIN or RST, and only subscribes, is never dropped and keeps its memory. Transport concern, in scope. |
| 5 | **Connection cap** | Not started, no change opened | A configured maximum of open connections; an upgrade above it is refused. Admission control, in scope. Bounds memory against a client stuck in a reconnect loop. |
| 6 | **Graceful shutdown on SIGTERM** | Not started, no change opened | `src/main.rs` handles only Ctrl+C (SIGINT); `docker stop` and Kubernetes send SIGTERM. The change must also check whether open WebSockets are closed within a bound, or whether shutdown waits on them. Operability, in scope. |
| 7 | **Health check and minimal metrics** | Not started, no change opened | A health route for the orchestrator or load balancer, and at least the open-connection count and the dropped-message total. The exposition format is decided in the proposal. Operability, in scope. |
| 8 | **TLS-terminating proxy** | Not started, no change opened | The gateway does not terminate TLS (`README.md`), so clients reach it through a proxy that serves `wss://`, such as Caddy on the same host or the TLS of the hosting platform. Delivered as tested configuration and documentation, with the proxy's idle timeout above the heartbeat interval. No code change in the gateway. |
| 9 | **Open-file limit** | Not started, no change opened | Each connection is a file descriptor, and many Linux defaults allow 1024. The proposal decides between documenting the required `ulimit -n`, checking it at startup, or both. |
| 10 | **Packaging** | Not started, no change opened | A container image of the release binary, and an example deployment that runs it behind the proxy from 8. |

## After the MVP

| Milestone | Notes |
|---|---|
| **Per-connection rate limiting** | Admission control, in scope. Not needed while only the operator's own services publish: memory is already bounded, and lag warnings show a noisy publisher. It returns when publishers the operator does not control connect. The unit is messages per second first, because the inbox overflows by message count; bytes per second only if large payloads are common. Listed in `README.md` under *Not implemented yet*. |

## Under consideration

Not planned yet. Each one needs a decision before it can be planned.

| Milestone | Notes |
|---|---|
| **Subscription authorization** | Removed from the plan, see decision 18 in `docs/decisions.md`. The gateway is self-hosted and only the operator's own services are meant to reach it, so an admitted client is already trusted. It comes back only if an untrusted client, such as a browser, connects directly, which needs a credential issued per connection. |
| **Delivery acknowledgements** | Listed as a borderline case in `docs/scope.md`: payload-agnostic, but it forces per-message state and a retry path where today there is none. It needs an entry in `docs/decisions.md` before any proposal, and it has to be weighed against the rule that a connection holds nothing that outlives it. |

## Not scheduled

Known gaps with no position in the order yet (`README.md`, *Not implemented yet*). They are listed so they are not forgotten, not because they are planned.

- A backplane for running more than one instance (`docs/architecture.md`, *What would have to change*).

# Scope

## What this project is

A generic realtime WebSocket gateway. It accepts connections, assigns each one
an identity, and relays messages between them.

It is **transport**, not an application. Chat, notifications, multiplayer game
state, live dashboards and telemetry are all valid uses, and the gateway cannot
tell them apart — which is the point. The meaning of a message is defined by the
client and server that exchange it, never by the gateway.

## The scope test

> A feature belongs in this gateway only if it can be implemented **without
> knowing what the payload means**.

Apply it literally. If the implementation needs to look inside `data` — read a
field, validate a shape, branch on a value — the feature belongs in the
application consuming the gateway.

## In scope

Everything here is payload-agnostic:

- **Routing primitives** — topics, rooms, channels, direct delivery by
  connection id. Routing by an opaque key is fine; routing by message content is
  not.
- **Connection identity** — the UUID assigned at connect time.
- **Presence** — which connection ids are online, when they joined or left.
- **Admission control** — authentication at connect time, connection limits,
  rate limiting, message size limits.
- **Delivery semantics** — backpressure policy, lag signalling, ordering
  guarantees, acknowledgements, delivery retry.
- **Transport concerns** — binary passthrough, compression, heartbeats, close
  codes.
- **Operability** — metrics, structured logging, health checks, graceful
  shutdown.
- **Horizontal scale** — a backplane that relays between gateway instances.

## Out of scope

Everything here requires knowing what the payload means:

- **User concepts** — usernames, profiles, accounts, avatars, roles.
- **Content handling** — validation, transformation, filtering, moderation,
  translation, templating.
- **Application state** — chat history, unread counts, read receipts, typing
  indicators, game state, matchmaking.
- **Business logic of any kind**, including "just a small special case for
  messages of type X".
- **Storage.** The gateway holds no durable state.

If you want any of these, build them in the service that sits behind or beside
the gateway. That service speaks its own protocol inside `data`, and the gateway
carries it without opening it.

## Borderline cases

These pass the scope test but expand the project enough that they need an
explicit decision in `docs/decisions.md` before implementation — not a drive-by
commit.

| Case                                                                                                    | Why it is borderline                                                                                                                                            |
|---------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Replay buffer** — resend the last N messages to a late joiner                                         | Payload-agnostic, so it passes the test. But it turns a stateless relay into something that retains data, which changes the memory model and the failure modes. |
| **Acknowledgements / delivery guarantees**                                                              | Generic, but forces per-message state and a retry path where today there is none.                                                                               |
| **Persistence of any kind**, even a bounded queue                                                       | Same reasoning; the gateway is currently pure transport and stays cheap because of it.                                                                          |
| **Server-initiated routing rules** (filters, subscriptions with predicates)                             | Fine while the predicate is over metadata the gateway owns. The moment a predicate reads `data`, it is out.                                                     |
| **A second wire format** (protobuf, MessagePack)                                                        | Transport-level and legitimate, but doubles the protocol surface and the test matrix.                                                                           |
| **Opaque connection metadata** — a client-supplied label attached at connect and echoed in the envelope | Payload-agnostic and genuinely useful. But it is one rename away from being a username, and it is the single most likely path back into domain modelling.       |

The pattern: *payload-agnostic* is necessary, not sufficient. Also ask whether
the feature keeps the gateway stateless, cheap and small.

## Find the generic sibling

Most out-of-scope requests have an in-scope version underneath. Rejecting the
request without naming that version is how good ideas get lost and how the same
request comes back later disguised.

| Requested                            | Generic sibling                                                 |
|--------------------------------------|-----------------------------------------------------------------|
| `username` on the connection         | Opaque connection metadata, uninterpreted by the gateway        |
| "Send this only to Alice"            | Direct delivery addressed by connection id                      |
| "Only chat messages go to this room" | Routing by topic key, chosen by the client                      |
| "Reject malformed messages"          | Frame size limits and JSON well-formedness, nothing about shape |
| "Show who is in the room"            | Presence over connection ids                                    |

The generic sibling is not automatically approved — most land in *Borderline
cases* above and need a decision entry. But it is the right thing to put in
front of the user instead of a flat no.

The discipline is in the naming. If the field ends up called `username`, the
gateway has learned a domain concept no matter how opaque the implementation
claims to be. Name it for what the gateway knows, which is nothing.

## How this project drifted before

The first implementation was a chat server. It required a `{"type":"join",
"username":"..."}` handshake, parsed messages into `{"type":"message",
"content":"..."}`, and rejected anything that did not fit that shape. A client
wanting to ship game state or a notification payload could not use it.

Nothing in the repository said the gateway was supposed to be generic, so the
chat framing read as the specification rather than as a mistake. Every later
change — validation, error messages, tests — reinforced it.

Two lessons, both encoded in this repo now:

1. **The intent has to be written down.** Code alone always reads as intentional.
2. **Domain vocabulary is the early warning.** The moment a type, field or error
   message says `username`, `room name` or `chat`, the boundary has already been
   crossed. Watch the nouns.

## Proposing a change

1. Apply the scope test. If it fails, it belongs in the consuming application —
   say so and stop.
2. If it passes but appears in *Borderline cases*, or expands the project
   materially, write the decision in `docs/decisions.md` first: context,
   decision, consequence.
3. If it passes cleanly, implement it, add an integration test in
   `tests/gateway.rs`, and check it against `docs/architecture.md`'s hot-path
   invariants.

The `scope-guard` skill runs steps 1 and 2 for you.

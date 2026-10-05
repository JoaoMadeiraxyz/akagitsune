# Design

## Context

Measured on `main` with a throwaway test (not committed), sending text frames from A and printing the raw text B received:

| sent | `data` received |
|---|---|
| `{"b":1,  "a":[ 1,2 ],"u":"\u00e9"}` | `{"b":1,  "a":[ 1,2 ],"u":"\u00e9"}` |
| `  42  ` | `42` |
| `\n{"x" : 1.50}\t` | `{"x" : 1.50}` |

The envelope bytes were `{"type":"message","from":"<uuid>","data":<value>}`. The value is spliced via `&RawValue` (`src/ws.rs:98` — `handle_socket`; `src/protocol.rs:9` — `ServerMessage`), which keeps the value's bytes and excludes surrounding whitespace.

## Goals / Non-Goals

**Goals:**

- Fail CI if `data` stops being the sender's bytes.

**Non-Goals:**

- Fixing the envelope's own key order or format beyond what the test needs. The test does pin `{"type":"message","from":"<uuid>","data":` as the prefix, which is the current serde output; changing the envelope layout would then require updating the test deliberately.

## Decisions

- Assert on the raw text frame, never on a parsed `Value`.
- One payload carries every byte-level trait that a round trip would destroy: extra interior whitespace, non-alphabetical key order, a `\u` escape and a trailing-zero number (`1.50`).
- A second frame with surrounding whitespace (`"  42  "`) asserts the trimming, so the spec and the test agree on the one place where bytes are not kept.

## Risks / Trade-offs

- Pinning the envelope prefix couples the test to serde's field order. Accepted: the envelope is part of the wire protocol, so an accidental change there should fail too.

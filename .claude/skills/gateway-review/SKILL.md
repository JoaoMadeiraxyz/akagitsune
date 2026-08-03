---
name: gateway-review
description: Review pending changes to the realtime-gateway for scope, correctness, performance and test coverage before committing. Use after implementing or modifying anything in src/, tests/ or examples/, when asked to review the diff, or before creating a commit or pull request. Runs fmt, clippy and the test suite as part of the review.
---

# gateway-review

Project-specific review. Generic Rust review advice is not the point — these are
the things that go wrong in *this* codebase.

Start by reading the diff (`git diff`, plus `git diff --staged`). Review only
what changed, but check it against the invariants below.

## 1. Scope

The failure mode this project has actually hit.

- Did any domain vocabulary enter the code? Search the diff for `username`,
  `room`, `chat`, `player`, `notification`, `content`, `body`. A type, field,
  constant, error string or log line carrying one of these is a red flag.
- Is `data` read anywhere? The payload must be validated as `&RawValue` and
  embedded verbatim. Any `serde_json::Value`, typed struct, or field lookup on
  client payload is a violation.
- Does a new feature require the gateway to know what a message means? If in
  doubt, run the `scope-guard` skill.

## 2. Correctness

- **`.await` while holding a lock.** Currently there are no locks; if the diff
  introduces one, this becomes live. `std::sync` guards must not cross an await.
- **`unwrap`/`expect` on client-controlled input.** `to_text` uses `expect` only
  because `ServerMessage` is provably infallible to serialize. Anything derived
  from a frame must be handled.
- **Task lifecycle.** Every spawned task must be reachable by the `select!` and
  the aborts in `handle_socket`. A task that outlives the connection is a leak.
- **Broadcast errors handled.** `tx.send` returning `Err` and `recv` returning
  `Lagged`/`Closed` all need explicit arms.
- **Backpressure preserved.** Bounded channels stay bounded. An unbounded
  channel or a `Vec` that accumulates per connection is a memory bug waiting for
  a slow client.
- **Both frame types.** A change to text handling usually needs the binary path
  considered too, and vice versa.

## 3. Performance

Check against the invariants in `docs/architecture.md`:

- Payload never deserialized.
- Envelope serialized once by the publisher, never per receiver.
- Cloning a `BroadcastMessage` stays a refcount bump — no `to_vec`,
  `to_string`, `clone()` on the underlying bytes.
- No allocation added per receiver per message.
- No lock, no syscall, no logging with formatting cost added inside the fanout
  loop.

If the diff touches the reader, the bridge, or `BroadcastMessage`, run the
`perf-check` skill rather than guessing.

## 4. Tests

- Every protocol-visible behavior has an integration test in `tests/gateway.rs`.
  New frame type, new error condition, new field — all need one.
- Tests assert on the wire format, not on internals.
- No test relies on timing beyond the existing `assert_silent` pattern.

## 5. Hygiene

Run these, do not just recommend them:

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

- **No comments in the code.** Not doc comments, not inline. Rationale belongs in
  `docs/decisions.md` or the commit message.
- Docs updated if behavior changed: `README.md` for protocol,
  `docs/architecture.md` for invariants or limits, `docs/decisions.md` for a
  design choice worth not relitigating.

## Output

Report findings most severe first, each with the file, what breaks, and a
concrete fix. Say plainly if the diff is clean — do not manufacture findings.
State the actual result of the commands you ran; if something failed, show it.

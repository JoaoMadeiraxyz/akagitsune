# Proposal

## Why

The hard rule "never deserialize the payload" is only checked by semantic equality. `payload_is_relayed_verbatim_to_others` (`tests/gateway.rs:52`) parses what B receives into a `serde_json::Value` and compares values, so a change that deserialized `data` and re-serialized it, reordering keys, normalizing `1.50` to `1.5` or rewriting `\u00e9`, would still pass. The requirement also does not say what happens to whitespace around the value, which the gateway does drop.

## What Changes

- New integration test that compares the raw text B receives with the exact expected envelope bytes, using a payload whose bytes change under any parse and re-serialize round trip.
- The requirement states precisely what "verbatim" means today: the bytes of the JSON value are kept, and whitespace before and after the value is not.

No behavior change.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `message-relay`: "Text frames are relayed in an envelope" is made precise about bytes and surrounding whitespace, and gains test coverage.

## Impact

`tests/gateway.rs` (new test), `openspec/specs/message-relay/spec.md`. No change in `src/`.

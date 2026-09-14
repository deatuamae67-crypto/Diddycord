# Chapter 11 — bounded bulk message deletion

Discord can invalidate many cached messages in one Gateway dispatch with `MESSAGE_DELETE_BULK`. Ignoring that event leaves stale messages in the synchronous frontend cache even though single-message deletion is already supported.

## Selective parser

The Gateway now routes `MESSAGE_DELETE_BULK` through a dedicated parser that retains only:

- `channel_id`;
- the deleted message IDs.

Guild metadata and any unrelated payload fields are skipped. IDs remain borrowed from the Gateway frame during parsing and are converted to the existing single-delete frontend representation only when emitted.

## Defensive bound

The parser accepts at most **100 message IDs** from one bulk-delete dispatch. Its custom Serde sequence visitor caps the retained transient vector instead of allowing a payload-controlled allocation to grow with the incoming array.

A payload above that defensive limit is rejected rather than partially applied. Channel and message identifiers must also be decimal Discord snowflakes before any frontend events are emitted, so malformed path-like identifiers do not enter cached state.

## State convergence

Bulk deletion deliberately reuses the existing `FrontendEvent::MessageDelete` path. Each accepted ID becomes the same small deletion event already understood by `FrontendState`, which means there is only one cache-deletion semantic to maintain and no second message lifecycle implementation.

The fan-out is itself bounded by the 100-ID parser ceiling and uses the existing bounded Gateway broadcast. A slow frontend therefore still cannot create an unbounded queue or block the network heartbeat loop. If the broadcast receiver lags, the existing dropped-Gateway-event accounting exposes that loss to the frontend.

A future storage layer with much larger per-channel histories can replace the bounded fan-out with a batch cache operation without changing Discord parsing semantics. For the current intentionally small presentation histories, reusing the single-delete path keeps state logic compact and deterministic.

## Tests

The chapter tests verify:

- normal bulk payloads fan out to the existing single-delete event type in order;
- malformed non-snowflake identifiers are rejected before partial emission;
- payloads above the 100-ID defensive limit are rejected without emitting partial state changes.

# Chapter 9 — selected-channel history pager

Chapter 7 exposed bounded message-history REST reads. Chapter 9 adds the small piece of frontend state required to use those reads safely during channel selection and scrollback without letting the synchronous UI create duplicate or overlapping requests.

## Why a pager exists

A renderer should not need to remember raw REST request IDs, page sizes, the currently selected channel, or whether Discord already returned the beginning of a channel. `HistoryPager` keeps exactly that transient state and nothing more.

It does not own messages. `FrontendState` remains the single bounded presentation cache, and `RestDispatcher` remains the single asynchronous HTTP actor.

## Selection lifecycle

Selecting a different channel resets pending pagination state and the exhausted marker:

```rust
history.select_channel(channel_id);
history.try_load_latest(&rest)?;
```

Selecting the same channel again is a no-op. Clearing the selection removes all pager state without touching cached messages.

Only one history request may be pending for the current selection. Additional latest/older requests return `HistoryLoadStatus::Busy` instead of filling the bounded REST queue with duplicate work.

## Backward pagination

After an initial page has converged into `FrontendState`, an older page can be requested with:

```rust
history.try_load_older(&state, &rest)?;
```

The pager reads the oldest message currently exposed by `FrontendState::messages(channel_id)` and uses its Discord snowflake as the `before` anchor. The frontend therefore does not need to duplicate message-index bookkeeping.

If no message is currently cached, the result is `NoAnchor`; callers should request the latest page first.

## Completion matching

REST completions are matched by the generated request ID. History results must also carry the selected channel ID before they can complete the pending request.

A typical tick processes the event in this order:

```rust
state.apply_rest(event.as_ref());
let completion = history.observe_rest(event.as_ref());
```

Applying presentation state first means the freshly fetched page is already visible when the pager reports completion.

A page shorter than the requested page size marks the current selection as exhausted. A full page leaves backward pagination available. Matching failures release the pending slot so the caller can retry; unrelated REST events are ignored.

## Stale responses after channel switches

Changing selection deliberately forgets the old pending request. If that old HTTP operation later completes, `FrontendState` may still accept the bounded result for its own channel cache, but `HistoryPager` will ignore it because the request no longer matches the current selection.

This separation prevents a slow response from channel A from changing the pagination/exhaustion state of newly selected channel B.

## Bounds

- page size is clamped to Discord's 1–100 message range;
- there is at most one pending history request per pager;
- requests still flow through the bounded REST command queue;
- fetched messages still converge into the bounded `FrontendState` cache;
- the pager itself owns only one channel ID plus one pending request descriptor.

The pager intentionally does not implement persistent archives or an unbounded scrollback database. Those features, if ever added, should remain separate from the latency-sensitive presentation core.

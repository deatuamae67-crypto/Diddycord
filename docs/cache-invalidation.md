# Chapter 12 — topology-driven cache invalidation

The message cache is intentionally independent from the guild/channel/thread topology cache so rendering never needs to lock networking state. That separation creates one important consistency requirement: when Discord removes navigation topology, cached timelines for that topology must not survive indefinitely.

## Invalidation order

`FrontendState::apply()` now performs topology-driven timeline invalidation before forwarding the event to `TopologyState`. This ordering matters because deletion events remove the topology identifiers needed to find affected cached timelines.

The frontend cache reacts to:

- `GUILD_DELETE` with `unavailable=false`: retire every currently known channel and active thread timeline in the guild.
- `GUILD_DELETE` with `unavailable=true`: preserve topology and messages because Discord is signalling temporary unavailability rather than membership removal.
- `CHANNEL_DELETE`: retire the deleted channel timeline and the timelines of active child threads.
- archived `THREAD_UPDATE` and `THREAD_DELETE`: retire the thread timeline.
- `THREAD_LIST_SYNC`: retire active thread timelines that disappeared from the sync scope.
- a refreshed `GUILD_CREATE`: retire cached channels that disappeared from the replacement guild snapshot.

## Retired-channel tombstones

Deleting the in-memory timeline is not enough on its own. A REST history request submitted before a channel/thread disappears may complete after the Gateway deletion event. Without a guard, that late response could recreate stale presentation state.

`FrontendState` therefore keeps a small bounded FIFO of retired channel/thread IDs. Message creates, updates, and history merges targeting a retired ID are ignored. The tombstone budget is bounded to `max(max_channels, 8)`, so deletion history cannot grow without limit.

A topology event that makes an ID active again removes its tombstone. This is relevant for threads, which can transition from archived back to active using the same Discord snowflake. Fresh guild snapshots, channel creates, thread creates, active thread updates, and thread sync payloads perform that restoration.

## Memory and runtime properties

- no new dependency or global database
- no mutex on the render or Gateway hot paths
- timeline invalidation only scans bounded topology collections
- tombstones are bounded and use a compact `VecDeque<Box<str>>`
- direct-message timelines are unaffected because they are not retired by guild topology events
- existing message/timeline LRU limits remain authoritative

## Tests

The state tests cover:

- parent-channel deletion purging child thread timelines
- late REST history being unable to resurrect a retired channel
- temporary guild unavailability preserving messages while true guild deletion purges them
- scoped/full thread synchronization removing only stale thread timelines
- archived threads rejecting stale history until they become active again
- refreshed guild snapshots removing timelines for channels that no longer exist

# Chapter 15 — direct-message cache convergence

Chapter 14 made one-to-one bot DMs navigable, but the message timeline and DM navigation caches still had independent lifecycles. A `CHANNEL_DELETE` for a DM could remove navigation while leaving cached messages alive until ordinary LRU eviction. Chapter 15 closes that gap using the bounded retirement mechanism introduced for guild topology in Chapter 12.

## Deletion convergence

`FrontendState` now treats `DirectChannelDelete` as a destructive topology transition. Before ordinary event handling, it removes the corresponding message timeline and records the channel snowflake in the existing bounded retired-channel FIFO.

That tombstone blocks both late Gateway message creates/updates and REST history responses that were already in flight when the DM was deleted. A delayed network response therefore cannot recreate a conversation that Discord has already removed from navigation.

## Re-activation

A later `DirectChannelCreate` or `DirectChannelUpdate` for the same snowflake clears the retirement tombstone. New messages and history can then populate the bounded timeline normally. This keeps the stale-response guard reversible instead of turning it into a permanent deletion database.

## Runtime properties

- no new allocation structure: DMs reuse the existing bounded retirement FIFO;
- no new dependency or lock;
- direct-channel deletion is O(1) average for the timeline HashMap removal plus a scan of the small bounded tombstone FIFO;
- late history remains rejected through the same `apply_history` guard used by guild/thread retirement;
- direct topology remains a separate optional navigation controller, while `FrontendState` only consumes the lifecycle event needed for cache consistency.

## Tests

Coverage verifies that deleting a DM purges its timeline, late REST history and Gateway messages cannot resurrect it, and a later direct-channel create/update clears the tombstone so the same channel ID can become active again.

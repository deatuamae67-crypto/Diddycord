# Chapter 8 — bounded thread topology

Discord exposes active threads separately from ordinary guild channels. A channel-only navigation cache therefore cannot resolve message channel IDs that belong to forum/media posts or text-channel threads. Chapter 8 extends the existing bounded topology layer with active thread navigation while preserving the same low-memory design.

## Gateway coverage

The Gateway parser now handles:

- active `threads` embedded in `GUILD_CREATE`;
- `THREAD_CREATE`;
- `THREAD_UPDATE`;
- `THREAD_DELETE`;
- `THREAD_LIST_SYNC`.

Only fields needed by navigation are retained: thread ID, guild ID, parent channel ID, name, channel type, archived state and locked state. Thread members, message/member counts, permission data, rate-limit metadata and other channel fields are ignored.

A `GUILD_CREATE` continues to emit the ordinary bounded guild/channel snapshot and then emits a full-guild thread sync. This avoids widening the public guild snapshot structure and keeps thread lifecycle semantics in one event path.

## Bounded topology

`TopologyState` stores active threads beside channels inside each already-bounded guild entry. The default limit is 512 active threads per guild. `TopologyState::new(max_guilds, max_channels_per_guild)` uses the same per-guild bound for channels and threads, while `TopologyState::with_limits(...)` allows a distinct thread limit.

Reads remain allocation-free:

```rust
for thread in state.topology().threads(guild_id) {
    // render thread navigation
}

for thread in state.topology().threads_for_parent(guild_id, channel_id) {
    // render threads below one parent channel
}
```

No global thread-ID hash map is added. Thread lookup and update costs are linear in the configured per-guild thread bound, trading a small bounded scan for lower persistent memory.

## Sync semantics

`THREAD_LIST_SYNC` can cover either named parent channels or the entire guild. Diddycord mirrors those semantics:

- when `channel_ids` is present, cached active threads under exactly those parents are cleared before the returned active set is inserted;
- when `channel_ids` is omitted, the guild's complete active-thread cache is replaced;
- parent channels named in the sync but returning no threads are therefore correctly cleared.

Archived thread updates remove that thread from active navigation. A later create/update can reinsert it if it becomes active again.

Deleting a parent channel also drops cached active threads whose `parent_id` points to that channel. Deleting or truly leaving a guild removes its channels and threads together, while temporary guild unavailability preserves both caches.

## Memory and lag behavior

Thread events use the existing bounded Gateway broadcast. A slow frontend can lag and lose topology events, but it can never block heartbeats or grow an unbounded queue. A subsequent guild snapshot or thread-list sync can converge active-thread topology again.

When the configured thread limit is reached, additional thread entries are dropped and included in `TopologyState::dropped_items()` accounting rather than forcing memory growth.

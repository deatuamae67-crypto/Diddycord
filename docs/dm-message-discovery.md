# Inbound DM discovery from MESSAGE_CREATE

Diddycord can now learn a one-to-one direct-message channel from an inbound Gateway `MESSAGE_CREATE`, even when Discord did not emit a fresh `CHANNEL_CREATE` after the client started.

## Why this exists

`DirectTopologyState` originally learned DM navigation only from `CHANNEL_CREATE` and `CHANNEL_UPDATE`. A bot can receive a direct message in a channel that is not currently present in that bounded navigation cache, leaving the message timeline visible to the core but the conversation absent from the graphical DM list.

The Gateway connection now performs a second selective borrowed parse of `MESSAGE_CREATE` before emitting the ordinary timeline event. If the payload has no `guild_id`, the current bot identity from the Chapter 13 latest-value identity watch is available, and the message author is a different valid Discord snowflake, the author is treated as the DM recipient.

The emitted `DirectChannelUpdate` contains only the same bounded navigation metadata already used by Chapter 14: channel ID, recipient ID, username, global name and avatar hash. `DirectTopologyState` therefore reuses its existing bounded LRU upsert path; no new unbounded collection or global message/user database is introduced.

## Ordering

For an accepted inbound DM, the direct-channel discovery event is emitted before the normal `FrontendEvent::Message`. Consumers draining the shared bounded broadcast consequently learn the conversation before touching or rendering its timeline.

This ordering also lets `FrontendState` clear a matching retired-DM tombstone before applying the fresh message, reusing Chapter 15's cache-convergence semantics when Discord legitimately makes the channel active again.

## Self-echo protection

Outbound Discord messages are normally echoed back through Gateway `MESSAGE_CREATE`. Diddycord compares the payload author ID with the authenticated bot ID captured from `READY`. A self-authored echo does **not** emit DM discovery, so it cannot replace a known recipient with the bot's own identity.

If the current bot identity is not yet known, discovery is skipped rather than guessing. The ordinary message parser still runs independently.

## Bounds and validation

- Guild messages (`guild_id` present) are ignored by the DM discovery path.
- Channel and author IDs must be decimal Discord snowflakes.
- An empty author username is rejected for recipient discovery.
- The parser borrows Gateway strings during decoding and allocates only when an accepted discovery event is emitted.
- Existing bounded broadcast capacity and `DirectTopologyState` LRU limits remain unchanged.

Authentication remains bot/application-token only. This feature does not implement user-token or self-bot behavior.

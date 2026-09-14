# Chapter 14 — bounded direct-message topology

Guild topology and direct-message navigation have different shapes. Discord DM channel payloads do not contain a `guild_id`, and a one-to-one DM is identified primarily by the channel snowflake plus recipient metadata. Chapter 14 adds a small dedicated path for that navigation state without turning the core into a global user database.

## Selective channel parsing

`CHANNEL_CREATE` and `CHANNEL_UPDATE` payloads are inspected by both the existing guild-channel parser and the new DM parser. The DM parser accepts only channel type `1` with no `guild_id` and retains:

- channel ID
- first recipient ID
- first recipient username
- optional global display name
- optional avatar hash

The recipients array is parsed with a custom Serde visitor that retains only its first entry and consumes the rest as `IgnoredAny`, preventing recipient-array size from controlling persistent allocations. Guild channels are ignored by this parser.

`CHANNEL_DELETE` with no `guild_id` becomes a direct-channel deletion event. Snowflake IDs are validated before frontend events are emitted.

## Bounded navigation state

`DirectTopologyState` is a standalone bounded controller because DM navigation does not naturally belong inside a guild entry. Its default capacity is 64 channels and callers can choose another positive limit.

The cache:

- upserts `DirectChannelCreate` / `DirectChannelUpdate` in place;
- deletes `DirectChannelDelete` immediately;
- uses vector-backed storage rather than a global hash index;
- evicts the least-recently-used DM at capacity;
- touches known DMs when message create/update/delete events reference their channel;
- counts LRU drops and Gateway broadcast lag separately.

`FrontendDirectChannel::display_name()` prefers a global name and falls back to the username.

## Frontend transport

A frontend can create a second inexpensive subscription from `NetworkBackbone::subscribe()` and drain it through `DirectTopologyState::drain()`. Broadcast messages contain `Arc<FrontendEvent>`, so the extra subscriber shares event allocations rather than copying complete Gateway payloads.

The direct drain is budgeted and non-blocking. It reports processed events, direct/topology events, lag, and closure. This keeps the renderer in control of per-frame work even during Gateway bursts.

## Scope

This chapter targets one-to-one bot DMs (Discord channel type `1`). Group-DM semantics are deliberately not modeled because they are not part of the normal bot/application navigation model. The feature does not add user-token authentication or self-bot behavior.

Message history remains in the existing bounded `FrontendState`; this chapter supplies navigation metadata. A later convergence layer can use direct-channel deletion events to retire message timelines with the same stale-response guarantees already used for guild topology.

# Diddycord architecture — Chapter 6 guild/channel topology

Chapter 6 adds the minimum server/channel topology required for navigation while deliberately refusing to turn Gateway guild payloads into a full Discord object cache.

## Selective Gateway surface

The Gateway dispatcher now recognizes:

- `GUILD_CREATE`
- `GUILD_UPDATE`
- `GUILD_DELETE`
- `CHANNEL_CREATE`
- `CHANNEL_UPDATE`
- `CHANNEL_DELETE`

`GUILD_CREATE` can be one of the largest routine Gateway payloads. The parser keeps only the guild identity/name/availability flag plus channel topology. Member lists, roles, emojis, presences, voice states, permission overwrite contents, activities and other metadata are skipped by Serde while scanning the payload.

For each guild channel, the retained fields are only:

- channel ID;
- optional name;
- Discord channel type;
- position;
- optional parent/category ID.

This provides enough information for a frontend to render a server/channel navigator without paying the memory cost of a general Discord cache.

## Bounded topology state

`TopologyState` uses a small `Vec` of guild records and a `Vec` of channels per guild instead of global hash indexes. The default limits are 256 guilds and 512 channels per guild. Both are configurable through `FrontendState::with_topology_limits()`.

Linear lookup is deliberate: navigation collections are small compared with message/event throughput, and the vector representation avoids one hash-table allocation plus bucket overhead per topology index. For the legacy target, predictable memory and cache locality are preferred over theoretical O(1) lookup.

Guild capacity uses least-recently-used eviction. A channel beyond the configured per-guild limit is dropped and counted by `TopologyState::dropped_items()` rather than allowing memory to grow without bound.

## Availability semantics

Discord can emit `GUILD_DELETE` with `unavailable = true` during a temporary guild outage. Diddycord preserves the cached guild/channel topology and marks that guild unavailable in this case. A normal `GUILD_DELETE` removes the guild.

A subsequent full `GUILD_CREATE` refreshes the guild and replaces its bounded channel snapshot. An unavailable snapshot with no channels does not destroy a previously useful cached channel list.

## Frontend access

Topology events travel through the same bounded Gateway broadcast and the same budgeted `FrontendState::drain()` call as message events.

```rust
let report = state.drain(&mut gateway_events, 64);

for guild in state.topology().guilds() {
    println!("{}: {}", guild.id, guild.name);
    for channel in state.topology().channels(guild.id) {
        println!("  {:?}", channel.name.as_deref());
    }
}
```

No network operation, lock, or allocation is required when iterating the cached topology from the synchronous frontend.

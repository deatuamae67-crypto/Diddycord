# Cache invalidation test matrix

This note records the consistency cases covered by Chapter 12 so later frontend/JNI work can preserve the same semantics.

| Gateway/topology transition | Message cache result | Late history result |
| --- | --- | --- |
| `GUILD_DELETE` with `unavailable=true` | preserve | accepted |
| `GUILD_DELETE` with `unavailable=false` | retire known guild channel/thread timelines | rejected for retired IDs |
| `CHANNEL_DELETE` | retire channel and active child-thread timelines | rejected |
| archived `THREAD_UPDATE` | retire thread timeline | rejected until active again |
| active `THREAD_UPDATE` / `THREAD_CREATE` | keep/restore thread | accepted |
| `THREAD_DELETE` | retire thread timeline | rejected |
| scoped `THREAD_LIST_SYNC` | retire only missing threads inside the named parent scope | rejected for retired thread IDs |
| full `THREAD_LIST_SYNC` | retire missing active guild threads | rejected for retired thread IDs |
| refreshed available `GUILD_CREATE` | retire channels absent from the replacement snapshot | rejected for retired channel IDs |

All retirement tracking remains bounded; it is a stale-response guard, not a durable deletion database.

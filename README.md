# Diddycord

Diddycord is an experimental ultra-low-overhead Rust Discord client core. The design target is one codebase that remains responsive on modern desktop hardware while still being practical on legacy systems such as a Core 2 Duo / Windows 7 machine and Android 6-era ARM hardware.

The project is intentionally split into small, bounded layers so networking never waits for a synchronous frontend and memory usage remains predictable.

## Current implementation

### Chapter 1 — asynchronous Gateway backbone

- Tokio multi-thread runtime with a 1–4 worker cap, bounded blocking pool, reduced worker stack size, explicit scheduler polling intervals, and cooperative yielding during Gateway bursts.
- Secure WebSocket connection to Discord Gateway **v9** using `tokio-tungstenite` + `rustls`.
- Borrowed outer Gateway JSON parsing with `serde_json::value::RawValue` instead of a generic JSON DOM.
- HELLO, IDENTIFY, RESUME, HEARTBEAT, HEARTBEAT ACK, RECONNECT, INVALID SESSION, close-code classification, session resume, and reconnect backoff.
- Tight WebSocket buffers, explicit timeouts, and bounded frontend transport.

### Chapter 2 — synchronous frontend state

- Frontend-owned `FrontendState`; no mutex between rendering and Gateway I/O.
- Budgeted non-blocking event draining with `try_recv()`.
- Bounded per-channel message histories and bounded channel count.
- Least-recently-used channel eviction and explicit broadcast-lag accounting.

### Chapter 3 — cached message lifecycle

- Selective `MESSAGE_CREATE`, `MESSAGE_UPDATE`, and `MESSAGE_DELETE` parsing.
- Message IDs retained so cached messages can be edited and removed.
- Edits/deletes operate only inside the already bounded channel history; no global message database or message-id hash index is required.

### Chapter 4 — outbound REST dispatcher

- Bounded asynchronous command actor for text message send/edit/delete operations.
- Non-blocking synchronous `RestHandle::try_*` submission API.
- rustls/WebPKI HTTP transport with redirects disabled and Authorization marked sensitive.
- Conservative Discord 429 handling plus `X-RateLimit-*` pre-emptive delays.
- Bounded response-body reads and selective JSON parsing.
- Ambiguous transport failures are not automatically retried, avoiding accidental duplicate message POSTs.

### Chapter 5 — REST/Gateway state convergence

- Successful REST send/edit/delete results can be drained directly into `FrontendState` without blocking the renderer.
- Message creation is an upsert by Discord message ID inside the bounded channel timeline.
- A REST send result and its later Gateway `MESSAGE_CREATE` echo therefore converge to one cached message instead of producing duplicates.
- REST lag and Gateway lag are accounted separately.
- Failed REST operations do not mutate presentation state, avoiding rollback bookkeeping.

### Chapter 6 — guild/channel topology

- Selective `GUILD_CREATE`, `GUILD_UPDATE`, `GUILD_DELETE`, `CHANNEL_CREATE`, `CHANNEL_UPDATE`, and `CHANNEL_DELETE` handling.
- Large guild payloads retain only guild identity/name/availability and channel ID/name/type/position/parent topology; members, roles, presences, emojis and permission metadata are skipped.
- `TopologyState` uses bounded vector-backed guild/channel storage instead of global hash indexes.
- Default limits are 256 guilds and 512 channels per guild, with configurable limits and drop accounting.
- Temporary guild unavailability preserves cached topology; a true guild delete removes it.

### Chapter 7 — bounded message-history bootstrap

- `RestHandle::try_fetch_messages()` loads the newest 1–100 messages for a selected channel without blocking the frontend.
- `RestHandle::try_fetch_messages_before()` provides explicit backward pagination using a validated Discord message snowflake.
- History responses use a separate hard 2 MiB body ceiling and selective borrowed parsing; embeds, attachments, reactions and unrelated metadata are not retained.
- Fetched messages merge into the existing bounded channel timeline in Discord snowflake order rather than creating a second history store.
- Already cached live/Gateway messages win over history snapshots, preventing a late REST response from overwriting a newer edit.
- Cache capacity still keeps the newest configured messages, so repeated backward pagination cannot grow memory without bound.

### Chapter 8 — bounded active-thread topology

- Selective active-thread bootstrap from `GUILD_CREATE` plus `THREAD_CREATE`, `THREAD_UPDATE`, `THREAD_DELETE`, and `THREAD_LIST_SYNC`.
- Thread cache retains only navigation fields: ID, guild, parent channel, name, type, archived state, and locked state.
- Active threads live inside each bounded guild entry with a configurable per-guild cap; there is no global thread-ID hash database.
- Scoped `THREAD_LIST_SYNC` replaces only the named parent-channel subsets, while full sync replaces the complete guild thread set.
- Archived threads disappear from active navigation, and deleting a parent channel also removes its active child threads.

### Chapter 9 — selected-channel history pager

- `HistoryPager` turns the low-level Chapter 7 history endpoints into a tiny synchronous frontend controller.
- Selecting a channel resets stale pagination state and guarantees at most one history request is in flight for that selection.
- `try_load_latest()` submits the initial recent page; `try_load_older()` automatically anchors backward pagination to the oldest message currently cached for that channel.
- Matching REST completions release the pending slot and short pages mark the channel history as exhausted.
- Unrelated or stale REST completions are ignored by request ID, so switching channels cannot corrupt the new selection's pagination state.
- Page size is clamped to Discord's 1–100 range and all submissions still use the existing bounded REST queue.

### Chapter 10 — cancellable network lifecycle

- `NetworkBackbone::control()` exposes a cloneable, low-overhead control handle before the Gateway task is moved into the runtime.
- `NetworkControl::shutdown()` cancels pending connect/read/reconnect waits and transitions the Gateway loop to `Stopped` without blocking a frontend or mobile lifecycle callback.
- `NetworkStatus` reports `Idle`, `Connecting`, `Identifying`, `Resuming`, `Ready`, `Reconnecting`, `Stopped`, and fatal close states through a latest-value Tokio `watch` channel.
- Slow frontends cannot build a lifecycle-event backlog: status observation is level-triggered and only the newest state is retained.
- Recoverable disconnects preserve existing resume/reidentify and bounded backoff behavior while publishing retry state and attempt count.
- A shutdown requested before `run()` performs no network I/O, which makes Android activity/service teardown deterministic and testable.

### Chapter 11 — bounded bulk message deletion

- Gateway `MESSAGE_DELETE_BULK` events now converge with the same cached deletion semantics as ordinary `MESSAGE_DELETE` events.
- A custom Serde sequence visitor bounds each bulk payload to at most 100 retained message IDs instead of allowing payload-controlled transient allocation growth.
- Channel and message identifiers are validated as decimal Discord snowflakes before any frontend deletion event is emitted.
- Malformed or over-limit payloads are rejected atomically rather than partially mutating the presentation cache.
- Accepted IDs fan out through the existing bounded Gateway broadcast, so the network loop still cannot be blocked by a slow frontend or create an unbounded queue.

### Chapter 12 — topology-driven cache invalidation

- `FrontendState` now retires message timelines when their guild, channel, or thread disappears from the bounded navigation topology.
- True `GUILD_DELETE` events purge known guild channel/thread timelines, while temporary `unavailable=true` events preserve both topology and messages.
- Deleting a parent channel also purges active child-thread timelines; archived/deleted threads and stale `THREAD_LIST_SYNC` entries are removed immediately.
- Refreshed `GUILD_CREATE` snapshots retire channels that disappeared from the replacement topology instead of waiting for message-cache LRU eviction.
- A bounded FIFO of retired channel/thread snowflakes prevents late REST history responses or stale message events from resurrecting deleted timelines.
- Active thread/create/sync events clear matching tombstones, allowing Discord threads to be unarchived under the same snowflake without losing subsequent messages.

### Chapter 13 — current bot identity

- `READY` now selectively captures the authenticated bot's ID, username, global display name, discriminator, avatar hash, and bot flag while ignoring unrelated account/application metadata.
- `USER_UPDATE` refreshes that identity without touching the high-volume frontend event stream.
- Identity uses `watch<Option<Arc<CurrentUser>>>`: there is no growing queue, and multiple frontend consumers share the same allocated strings.
- `NetworkControl::self_user()` provides an immediate snapshot while `subscribe_self_user()` supports reactive desktop/mobile UI updates.
- `CurrentUser::display_name()` prefers Discord's global name and falls back to the username.
- The value survives reconnect/resume transitions until Discord supplies a newer identity, and the feature remains strictly within bot/application authentication.

## Authentication

The current core uses a Discord **bot/application token**. It intentionally does not implement user-token/self-bot authentication.

For the desktop harness:

```text
DISCORD_BOT_TOKEN=<bot token>
DISCORD_INTENTS=37377
```

`DISCORD_INTENTS` is optional. The default enables `GUILDS`, `GUILD_MESSAGES`, `DIRECT_MESSAGES`, and `MESSAGE_CONTENT`. Enable the Message Content privileged intent in the Discord Developer Portal when required for the bot.

Do not commit `.env` files or tokens. `.env.example` exists only as a variable-name reference.

## Build

The project pins Rust **1.77.2** because ordinary Rust Windows targets raised their Windows baseline after that toolchain generation. This keeps the project buildable for the Windows 7 target requirement while CI checks the same MSRV on modern runners.

```bash
cargo build --release
```

The release profile is deliberately runtime/footprint oriented:

```toml
[profile.release]
opt-level = 3
lto = true
codegen-units = 1
panic = "abort"
strip = "symbols"
incremental = false
```

## Android 6 / API 23

The core itself does not depend on a desktop GUI API. CI cross-checks `aarch64-linux-android` at API 23. The eventual Android application should expose the library through a small JNI/NDK bridge and inject credentials through the application layer rather than environment variables.

The Galaxy S6 family exists in both 64-bit and 32-bit userspace variants depending on firmware/device configuration, so final packaging should verify the target handset before dropping `armeabi-v7a` support.

## Frontend usage

Gateway receive/lifecycle path:

```rust
let backbone = NetworkBackbone::new(config, 256);
let control = backbone.control();
let mut network_status = control.subscribe_status();
let self_user = control.subscribe_self_user();
let mut gateway_events = backbone.subscribe();
let mut state = FrontendState::new(64, 128);

runtime.spawn(async move {
    let _ = backbone.run().await;
});

// Once per frame/tick:
let gateway_report = state.drain(&mut gateway_events, 64);
let current_status = *network_status.borrow();
if let Some(user) = self_user.borrow().as_ref() {
    let display_name = user.display_name();
    let avatar_hash = user.avatar_hash.as_deref();
}

for guild in state.topology().guilds() {
    for channel in state.topology().channels(guild.id) {
        let _ = channel.name.as_deref();
    }
    for thread in state.topology().threads(guild.id) {
        let _ = thread.parent_id.as_deref();
    }
}

// During application/service shutdown:
control.shutdown();
```

REST action/history path:

```rust
let (rest, worker) = RestDispatcher::new(&token, 32, 64)?;
runtime.spawn(worker.run());
let mut rest_events = rest.subscribe();
let mut history = HistoryPager::new(50);

let send_request = rest.try_send_message(channel_id, "hello")?;
history.select_channel(channel_id);
let initial_history = history.try_load_latest(&rest)?;

// Once per frame/tick, process REST results without blocking.
while let Ok(event) = rest_events.try_recv() {
    state.apply_rest(event.as_ref());
    let completion = history.observe_rest(event.as_ref());
}

// When the user scrolls to the top of the cached timeline:
let older_history = history.try_load_older(&state, &rest)?;
```

History fetching remains caller-driven: selecting or scrolling a channel can request a page, while idle channels consume no REST bandwidth or history memory beyond the configured presentation cache.

Both transports are bounded: a slow frontend cannot block Gateway heartbeats or create an unbounded outbound queue.

## Repository layout

```text
src/main.rs       desktop Gateway harness
src/lib.rs        public library surface
src/gateway/      Discord Gateway transport, selective parsers, identity, heartbeat, reconnect and lifecycle control
src/history.rs    selected-channel history paging state machine
src/rest.rs       bounded Discord REST actor, outbound actions and message-history reads
src/runtime.rs    low-overhead Tokio runtime configuration
src/state.rs      bounded synchronous presentation cache and topology-driven invalidation
src/topology.rs   bounded guild/channel/thread navigation state
docs/             architecture notes by chapter
```

## Security boundary

Never hard-code or commit Discord credentials. The Gateway and REST layers treat the token as configuration only and do not log it. REST redirects are disabled to avoid forwarding Authorization to another host.

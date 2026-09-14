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

Gateway receive path:

```rust
let backbone = NetworkBackbone::new(config, 256);
let mut gateway_events = backbone.subscribe();
let mut state = FrontendState::new(64, 128);

// Once per frame/tick:
let gateway_report = state.drain(&mut gateway_events, 64);

for guild in state.topology().guilds() {
    for channel in state.topology().channels(guild.id) {
        let _ = channel.name.as_deref();
    }
}
```

REST action/history path:

```rust
let (rest, worker) = RestDispatcher::new(&token, 32, 64)?;
runtime.spawn(worker.run());
let mut rest_events = rest.subscribe();

let send_request = rest.try_send_message(channel_id, "hello")?;
let history_request = rest.try_fetch_messages(channel_id, 50)?;

// For explicit backward pagination, use the oldest cached message ID.
let older_request = rest.try_fetch_messages_before(channel_id, oldest_message_id, 50)?;

// Also once per frame/tick. Successful results converge with Gateway state.
let rest_report = state.drain_rest(&mut rest_events, 32);
```

History fetching is deliberately caller-driven: selecting or scrolling a channel can request a page, while idle channels consume no REST bandwidth or history memory beyond the configured presentation cache.

Both transports are bounded: a slow frontend cannot block Gateway heartbeats or create an unbounded outbound queue.

## Repository layout

```text
src/main.rs       desktop Gateway harness
src/lib.rs        public library surface
src/gateway/      Discord Gateway transport, parser, heartbeat and reconnect logic
src/rest.rs       bounded Discord REST actor, outbound actions and message-history reads
src/runtime.rs    low-overhead Tokio runtime configuration
src/state.rs      bounded synchronous presentation cache and REST/Gateway/history convergence
src/topology.rs   bounded guild/channel navigation state
docs/             architecture notes by chapter
```

## Security boundary

Never hard-code or commit Discord credentials. The Gateway and REST layers treat the token as configuration only and do not log it. REST redirects are disabled to avoid forwarding Authorization to another host.

# Diddycord

Diddycord is an experimental ultra-low-overhead Rust Discord Gateway client core. The design target is one codebase that remains responsive on modern desktop hardware while still being practical on legacy systems such as a Core 2 Duo / Windows 7 machine and Android 6-era ARM hardware.

The repository currently contains **Chapter 1: the asynchronous networking backbone**.

## Chapter 1 status

Implemented:

- Tokio multi-thread runtime with a 1–4 worker cap, bounded blocking pool, reduced worker stack size, explicit scheduler polling intervals, and cooperative yielding during Gateway bursts.
- Secure WebSocket connection to Discord Gateway **v9** using `tokio-tungstenite` + `rustls`.
- Native-root and WebPKI-root TLS paths for desktop and Android-oriented builds.
- Borrowed outer Gateway JSON parsing with `serde_json::value::RawValue` so large event payloads are not materialized into generic JSON trees.
- Selective `MESSAGE_CREATE` extraction of only `channel_id`, `author.username`, and `content`.
- Bounded `tokio::sync::broadcast` bridge so a slow synchronous UI cannot backpressure networking or heartbeats.
- HELLO, IDENTIFY, RESUME, HEARTBEAT, HEARTBEAT ACK, RECONNECT, INVALID SESSION, close-code classification, session resume, and reconnect backoff.
- First-heartbeat jitter, 1–5 second invalid-session delay, and a minimum 5 second IDENTIFY retry interval.
- Tight WebSocket read/write buffers and a hard inbound frame/message ceiling.
- Parser/recovery unit tests.

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

The release profile is deliberately size/runtime oriented:

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

## Frontend bridge

Create a receiver before running the backbone:

```rust
let backbone = NetworkBackbone::new(config, 256);
let mut events = backbone.subscribe();
```

A synchronous render/UI loop can call `events.try_recv()` once per frame/tick. `broadcast` is bounded: lagging consumers lose old entries instead of blocking the network executor.

## Repository layout

```text
src/main.rs       desktop harness
src/lib.rs        public library surface
src/gateway/      Discord Gateway transport, parser, heartbeat and reconnect logic
src/runtime.rs    low-overhead Tokio runtime configuration
docs/architecture.md
```

## Security boundary

Never hard-code or commit Discord credentials. The networking core treats the token as configuration only and does not log it.

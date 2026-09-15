# Diddycord

Diddycord is an experimental, low-overhead **graphical Discord bot client** written in Rust. The project keeps the Discord networking core independent from presentation so the same bounded Gateway/REST architecture can drive native desktop and Android frontends without moving network work onto the render thread.

The compatibility target is deliberately broad: modern desktops, legacy Windows 7/Core 2 Duo-class systems, and Android 6/API 23-era ARM64 hardware.

> Diddycord accepts Discord **bot/application tokens only**. User-token/self-bot authentication is intentionally unsupported.

## Current applications

| Platform | Frontend | Output |
| --- | --- | --- |
| Windows x86_64 | Native eframe/egui GUI | `diddycord.exe` |
| Linux x86_64 | Native eframe/egui GUI | `diddycord` |
| macOS | Native eframe/egui GUI | source build |
| Android 6+/API 23, ARM64 | NativeActivity eframe/egui GUI | installable `.apk` |
| Desktop headless | CLI/network harness | `diddycord-headless` |

The graphical clients currently provide bot-token login, connection state/current bot identity, server navigation, one-to-one DM navigation, text-channel and active-thread timelines, recent-history bootstrap, live Gateway updates, message sending and explicit disconnect.

## Architecture

Diddycord is intentionally split into bounded layers:

- a Tokio multi-thread runtime with capped worker counts and cooperative scheduling;
- Discord Gateway v9 over `tokio-tungstenite` + `rustls`;
- selective borrowed Serde parsing instead of retaining generic JSON trees;
- heartbeat, reconnect and session-resume handling;
- bounded Gateway/REST broadcasts drained synchronously by the frontend;
- bounded guild/channel/thread/DM topology and message caches;
- a bounded REST actor for send/edit/delete/history operations;
- latest-value `watch` channels for network status and current bot identity;
- native eframe/egui presentation on desktop and Android.

Slow rendering therefore cannot block heartbeats or create an unbounded event queue. The desktop and Android applications reuse the same protocol, cache and REST code paths.

## Implemented chapters

| Chapter | Capability |
| ---: | --- |
| 1 | asynchronous Gateway backbone |
| 2 | bounded synchronous frontend state |
| 3 | cached message create/update/delete lifecycle |
| 4 | bounded outbound REST dispatcher |
| 5 | REST/Gateway state convergence |
| 6 | bounded guild/channel topology |
| 7 | bounded message-history bootstrap |
| 8 | bounded active-thread topology |
| 9 | selected-channel history pager |
| 10 | cancellable network lifecycle/status watch |
| 11 | bounded `MESSAGE_DELETE_BULK` handling |
| 12 | topology-driven message-cache invalidation |
| 13 | current authenticated bot identity |
| 14 | bounded one-to-one direct-message topology |
| 15 | direct-message cache convergence/invalidation |
| 16 | native Windows/Linux/macOS graphical client |
| 17 | Android 6+ NativeActivity graphical APK |

Detailed implementation notes live in `docs/`.

## Authentication

The GUI asks for a Discord bot token directly. The headless harness can use environment variables:

```text
DISCORD_BOT_TOKEN=<bot token>
DISCORD_INTENTS=37377
```

`DISCORD_INTENTS` is optional. The default enables `GUILDS`, `GUILD_MESSAGES`, `DIRECT_MESSAGES`, and `MESSAGE_CONTENT`. Enable the Message Content privileged intent in the Discord Developer Portal when the bot/application requires it.

Never commit tokens or `.env` files. `.env.example` contains variable names only.

## Desktop build

Diddycord pins Rust **1.77.2** to preserve the Windows 7 baseline while still supporting the GUI dependency set.

```bash
rustup override set 1.77.2
cargo build --release --bin diddycord
```

Run the graphical desktop app with:

```bash
cargo run --release --bin diddycord
```

The old headless harness remains available:

```bash
DISCORD_BOT_TOKEN=... cargo run --release --bin diddycord-headless
```

The release profile is optimized for runtime performance/footprint:

```toml
[profile.release]
opt-level = 3
lto = true
codegen-units = 1
panic = "abort"
strip = "symbols"
incremental = false
```

## Android 6+ graphical APK

Android uses the same eframe/egui 0.27.2 UI stack through `winit` 0.29.15 and `android-activity` 0.5.2 NativeActivity. The application package is `io.github.diddycord.client`.

Current Android packaging targets:

- minimum SDK: API 23 / Android 6;
- target SDK: API 35;
- ABI: `aarch64-linux-android`;
- permission: `android.permission.INTERNET`;
- cleartext traffic disabled;
- NDK: 27.2.12479018;
- packaging tool: `cargo-apk` 0.10.0.

The mobile UI uses a touch-oriented single-pane flow: **servers/DMs → channels → timeline/composer**, with larger interactive controls and explicit back navigation.

To build locally after installing the Android SDK/NDK, Rust Android target and `cargo-apk`:

```bash
rustup target add aarch64-linux-android --toolchain 1.77.2
cargo apk build --release --lib
```

`cargo-apk` requires signing for release-profile APKs. CI creates an ephemeral signing certificate so every pull request can prove that a real installable APK packages successfully. For update-safe official releases, configure a persistent keystore through the repository secrets `DIDDYCORD_ANDROID_KEYSTORE_B64` and `DIDDYCORD_ANDROID_KEYSTORE_PASSWORD`. The release workflow automatically uses them when present and records when an ephemeral fallback was used.

See `docs/android-gui.md` for the lifecycle, packaging and signing design.

## Frontend model

A frontend does not own the network runtime. It receives bounded events and applies them to synchronous presentation state:

```rust
let backbone = NetworkBackbone::new(config, 256);
let control = backbone.control();
let mut gateway_events = backbone.subscribe();
let mut direct_events = backbone.subscribe();
let mut state = FrontendState::new(64, 128);
let mut direct = DirectTopologyState::default();

runtime.spawn(async move {
    let _ = backbone.run().await;
});

// Once per frame/tick:
let gateway_report = state.drain(&mut gateway_events, 64);
let direct_report = direct.drain(&mut direct_events, 64);

// During application/activity shutdown:
control.shutdown();
```

REST work follows the same bounded model:

```rust
let (rest, worker) = RestDispatcher::new(&token, 32, 64)?;
runtime.spawn(worker.run());
let mut rest_events = rest.subscribe();
let mut history = HistoryPager::new(50);

let send_request = rest.try_send_message(channel_id, "hello")?;
history.select_channel(channel_id);
let initial_history = history.try_load_latest(&rest)?;

while let Ok(event) = rest_events.try_recv() {
    state.apply_rest(event.as_ref());
    let completion = history.observe_rest(event.as_ref());
}
```

## Repository layout

```text
src/main.rs                    graphical desktop entry point / non-desktop fallback harness
src/bin/diddycord-headless.rs  explicit headless desktop client
src/lib.rs                     public core surface + Android NativeActivity entry point
src/gui_app.rs                 shared desktop/Android graphical presentation layer
src/gateway/                   Gateway transport and selective event parsers
src/history.rs                 selected-channel history paging state machine
src/rest.rs                    bounded Discord REST actor
src/runtime.rs                 Tokio runtime configuration
src/state.rs                   bounded presentation cache/invalidation
src/topology.rs                bounded guild/channel/thread navigation state
docs/                          architecture and frontend notes
```

## Compatibility and security boundary

The desktop GUI uses exact `eframe = 0.27.2` with pinned transitive dependencies to stay inside the Rust 1.77.2/Windows 7 compatibility envelope. Android uses the same eframe generation and keeps API 23 as the minimum supported API.

The Gateway and REST layers treat the Discord token as configuration only and do not log it. REST redirects are disabled so Authorization cannot be forwarded to another host. Android cleartext traffic is disabled. User-token/self-bot behavior is outside the project boundary.

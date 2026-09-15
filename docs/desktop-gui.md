# Desktop graphical client

The default `diddycord` binary is now a native graphical application on Windows, Linux and macOS. The previous environment-variable-only entry point remains available as `diddycord-headless`.

## Scope of this first graphical layer

The UI is intentionally a thin synchronous presentation layer over the existing bounded core. It does not move Gateway or REST work onto the render thread.

- Bot-token login screen with password-style token entry.
- Gateway connection state and current bot identity in the top bar.
- Server navigation from `TopologyState`.
- One-to-one DM navigation from `DirectTopologyState` when Discord has supplied DM channel metadata.
- Text-channel and active-thread selection.
- Bounded cached message timeline rendering from `FrontendState`.
- Automatic recent-history request when an empty text channel is selected.
- Message sending through the existing bounded `RestDispatcher`.
- Explicit disconnect that cancels the Gateway lifecycle.

The GUI drains Gateway and REST broadcasts with fixed per-frame budgets, so a slow renderer cannot create an unbounded event queue. The network and REST dispatcher run on the project's existing bounded Tokio runtime in a dedicated worker thread.

## Compatibility choice

Desktop rendering uses exact `eframe = 0.27.2` with the Glow backend. That release declares Rust 1.72 as its MSRV and is based on `winit 0.29`; Diddycord remains pinned to Rust 1.77.2. Broad transitive GUI ranges that have since moved to newer Rust versions are pinned to older compatible releases.

Linux enables the X11 backend. Windows uses the native winit backend, whose supported baseline includes Windows 7. The GUI dependency is desktop-only for now, so the Android API 23 core build is not regressed while a proper Android NativeActivity/APK frontend is implemented separately.

## Running

Desktop graphical client:

```text
cargo run --release
```

Legacy/headless client:

```text
DISCORD_BOT_TOKEN=... cargo run --release --bin diddycord-headless
```

Diddycord continues to accept **bot/application tokens only**. User-token/self-bot authentication is not implemented.

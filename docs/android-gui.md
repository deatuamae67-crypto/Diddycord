# Android graphical application

Diddycord's Android frontend uses the same bounded Rust Gateway, REST, topology, identity and presentation-state layers as the desktop graphical client. Android is not a separate protocol implementation and does not use a WebView.

## Runtime and rendering

- Minimum supported Android version: Android 6 / API 23.
- Current packaged ABI: `aarch64-linux-android`.
- UI: `egui`/`eframe` 0.27.2 with the Glow renderer.
- Window/lifecycle integration: `winit` 0.29.15 + `android-activity` 0.5.2 NativeActivity.
- Packaging: `cargo-apk` 0.10.0.
- Rust application MSRV remains 1.77.2.
- Network I/O remains on the existing Tokio worker; the Android render/event-loop thread only drains bounded event channels and renders presentation state.

The crate exports the NativeActivity `android_main(AndroidApp)` entry point from `src/lib.rs`. Because eframe 0.27 predates the later `NativeOptions::android_app` field, Diddycord supplies the `AndroidApp` to winit through eframe's `event_loop_builder` hook.

## Mobile interaction model

The desktop three-column layout becomes a touch-oriented single-pane flow on Android:

1. server/direct-message selection;
2. channel or DM selection;
3. message timeline and composer.

Back controls move between those panes without discarding the bounded frontend caches. Interactive controls are enlarged on Android, and the message composer is stacked vertically to remain usable with the software keyboard and narrow displays.

The same functionality is retained: bot-token login, connection status, current bot identity, guild/channel/thread navigation, one-to-one DM navigation, recent-history bootstrap, live message updates and message sending.

## Permissions and security

The generated manifest requests only `android.permission.INTERNET` for current functionality. Discord authentication remains bot/application-token only. User-token/self-bot authentication is intentionally unsupported.

The application package is `io.github.diddycord.client`. Cleartext network traffic is disabled, so Discord Gateway and REST traffic continue to use TLS.

## Building an APK

Install Android SDK platform 35, Android build-tools 35.0.0, NDK 27.2.12479018, the Rust `aarch64-linux-android` target, and `cargo-apk` 0.10.0. Then build the library target:

```bash
cargo apk build --release --lib
```

`cargo-apk` requires a signing certificate for non-dev profiles. CI generates an ephemeral certificate only to prove that the optimized APK is genuinely packageable and installable. Official update-safe distribution should configure these repository secrets:

- `DIDDYCORD_ANDROID_KEYSTORE_B64`: base64-encoded persistent release keystore;
- `DIDDYCORD_ANDROID_KEYSTORE_PASSWORD`: keystore password.

The release workflow automatically uses those secrets when present. Without them it can still produce an installable experimental APK, but the workflow records that an ephemeral certificate was used; a future APK with a different certificate then requires uninstalling the previous build before installation.

## CI artifact

The Android CI job cross-compiles the complete graphical library for API 23, builds an optimized APK, signs it, computes SHA-256, and uploads both files as the `diddycord-android-api23-aarch64` artifact. This is materially stronger than the earlier core-only cross-check because packaging itself is now exercised on every pull request.

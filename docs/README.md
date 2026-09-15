# Architecture notes

The `docs/` directory contains implementation notes for Diddycord's bounded cross-platform core and graphical frontends.

- `desktop-gui.md` documents the native Windows/Linux/macOS graphical client introduced in Chapter 16.
- `android-gui.md` documents the Android 6+/API 23 NativeActivity graphical APK, its mobile navigation model, packaging and signing behavior.
- `dm-message-discovery.md` documents inbound one-to-one DM discovery from Gateway `MESSAGE_CREATE`, including self-echo protection and event ordering.
- `gateway-latency.md` documents the latest-value Gateway heartbeat RTT watch, reconnect clearing semantics and resource bounds.
- `gui-gateway-latency.md` documents how desktop and Android surfaces render the latest Gateway RTT without retaining history.
- `cache-invalidation.md`, `cache-invalidation-testing.md`, and `cache-invalidation-notes.md` cover Chapter 12 topology-driven message-cache invalidation.
- The remaining chapter files document the corresponding bounded Gateway, REST, topology, identity, DM and history components.

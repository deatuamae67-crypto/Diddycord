# Graphical Gateway latency

Chapter 20 surfaces the Chapter 19 heartbeat round-trip measurement in both graphical frontends.

## Desktop

The desktop top bar shows `Gateway <n> ms` next to the current connection state once the active Gateway connection has produced a heartbeat ACK sample. The value is obtained directly from `NetworkControl::latency()` and is rendered in whole milliseconds.

## Android

The Android/mobile status row uses the same latest-value latency source and formatting. It stays compact and touch-oriented; no graph, history buffer or background sampling task is introduced.

## Reconnect behavior

The latency label disappears whenever the core reports `None`. Chapter 19 clears the value during new connection attempts, disconnects, reconnects and shutdown, so the GUI never intentionally displays a stale RTT from a previous Gateway connection.

## Resource behavior

Rendering reads the existing Tokio watch value during the normal egui repaint. There is no additional network request, timer, queue, retained latency history or Android-specific telemetry path.

# Diddycord architecture — Chapter 1

## Execution model

Diddycord separates the network executor from the eventual synchronous frontend. The Tokio runtime uses between one and four worker threads depending on available parallelism. On legacy single-core or dual-core systems this prevents the networking layer from consuming an unbounded thread pool, while modern machines still retain enough concurrency for TLS, WebSocket I/O, timers, and parsing.

The Gateway task is intentionally single-flow: one socket, one heartbeat state machine, one session state, and one bounded outbound event bus. No task is spawned per message.

## Gateway receive path

```text
TLS/TCP -> WebSocket frame -> borrowed GatewayEnvelope -> event dispatch
                                                    |
                                                    +-> RawValue `d`
                                                          |
                                                          +-> parse only selected event type
                                                                |
                                                                +-> copy only UI-required strings
```

`GatewayEnvelope` borrows the event name and the raw `d` JSON value directly from the WebSocket text buffer. Unknown dispatch types never build a `serde_json::Value` tree. For `MESSAGE_CREATE`, Serde scans the object but stores only the three fields currently required by the frontend contract; unknown metadata is skipped during deserialization.

## Backpressure policy

The frontend channel uses `tokio::sync::broadcast` with a fixed capacity. Gateway I/O never awaits frontend consumption. If a consumer falls behind, Tokio reports lag on that receiver and old entries are discarded. This is deliberate: losing stale presentation events is preferable to delaying heartbeat ACK processing and causing a Gateway disconnect.

## Heartbeat and recovery

After HELLO, the initial heartbeat is jittered across the interval. Each scheduled heartbeat requires an ACK before the next scheduled heartbeat; missing the ACK forces reconnection. Discord RECONNECT requests prefer RESUME when session state is complete. Invalid sequence/session timeout close codes force a fresh IDENTIFY. Non-recoverable authentication, intent, shard, and protocol errors terminate the core instead of reconnecting forever.

IDENTIFY retries never occur faster than five seconds. RESUME retries use a shorter capped exponential backoff. INVALID SESSION waits a randomized one-to-five-second interval before reconnecting.

## Memory policy

- No generic JSON DOM for dispatch payloads.
- No task-per-message allocation pattern.
- Fixed frontend ring capacity.
- Small WebSocket eager buffers.
- `Box<str>` for owned message strings rather than spare-capacity `String`s.
- `Arc<FrontendEvent>` lets `broadcast` fan out without cloning message bodies per receiver.
- No WebRTC framework and no full Discord framework dependency.

## Platform boundary

`src/lib.rs` is the reusable core. `src/main.rs` is only a desktop harness that reads environment variables. Android integration should call the library from JNI/NDK and keep platform lifecycle, token storage, notifications, and rendering outside the Gateway module.

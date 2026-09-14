# Chapter 10 — cancellable network lifecycle

Long-lived Gateway networking needs an explicit lifecycle boundary on desktop and especially on mobile, where the application can be suspended or destroyed while a TCP/TLS/WebSocket operation is pending. Chapter 10 adds a tiny control plane without putting locks in the Gateway hot path.

## Network control

`NetworkBackbone::control()` returns a cloneable `NetworkControl` before the backbone is moved into its async task:

```rust
let backbone = NetworkBackbone::new(config, 256);
let control = backbone.control();

runtime.spawn(async move {
    let _ = backbone.run().await;
});

// Application shutdown / Android lifecycle callback:
control.shutdown();
```

Shutdown is backed by a Tokio `watch` channel. The request is idempotent and does not allocate per notification. If shutdown is requested before `run()`, Diddycord returns without opening a network connection. If it arrives while a connect/authentication/read future or reconnect delay is pending, the competing future is dropped and `run()` returns `Ok(())`.

This deliberately prioritizes deterministic task cancellation over waiting indefinitely for a WebSocket close handshake. Dropping the socket tears down the transport immediately and avoids holding mobile resources after the application has requested shutdown.

## Status watch

The same control handle exposes a latest-value status stream:

```rust
let mut status_rx = control.subscribe_status();
let current = control.status();
```

`NetworkStatus` is a compact `Copy` enum:

- `Idle`
- `Connecting { resume }`
- `Identifying`
- `Resuming`
- `Ready`
- `Reconnecting { attempt, resume }`
- `Stopped`
- `Fatal { code }`

A `watch` channel is used instead of a queue because connection state is level-triggered: a frontend generally needs the newest state, not an unbounded history of every transient transition. A slow UI therefore cannot create lifecycle backpressure.

## Reconnect semantics

Recoverable socket failures still use the existing bounded reconnect/backoff policy. Before each retry, the status changes to `Reconnecting`, including whether the next authentication attempt will try to resume the previous Discord session. The subsequent connection attempt publishes `Connecting`, then `Identifying` or `Resuming`, and finally `Ready` after `READY` or `RESUMED` is accepted.

A non-recoverable Discord close code publishes `Fatal { code }` before `run()` returns an error. User-requested cancellation publishes `Stopped` and returns success.

## Cost model

The lifecycle control adds two small Tokio watch channels to `NetworkBackbone`:

- one boolean shutdown signal;
- one latest-value `NetworkStatus` signal.

There is no mutex on message dispatch, no per-frame allocation, and no status-event backlog. `tokio::select!` is only used at the outer connection/reconnect boundaries, so the existing heartbeat and selective JSON loop remain unchanged.

## Testing

The unit tests verify that a newly created controller starts in `Idle`, can be cloned, and that requesting shutdown before `run()` transitions directly to `Stopped` without any network I/O. Cross-platform CI continues to compile the same code on Rust 1.77.2 for Linux, Windows and Android API 23/aarch64.

# Diddycord architecture — Chapter 2 state boundary

Chapter 2 adds the first synchronous frontend state layer without moving rendering or UI work onto Tokio.

## Ownership model

The network executor continues to own sockets, heartbeat timing, parsing, and reconnection. It publishes normalized `Arc<FrontendEvent>` values through the bounded broadcast channel introduced in Chapter 1. A synchronous frontend owns `FrontendState` and drains the receiver with `try_recv()` through `FrontendState::drain`.

No mutex is placed between rendering and networking. The frontend cache is mutated only by the synchronous owner that drains it.

```text
Discord Gateway
      |
      v
Tokio networking + selective parser
      |
      v
bounded broadcast<Arc<FrontendEvent>>
      |
      v
FrontendState::drain(receiver, per-frame budget)
      |
      v
synchronous renderer / platform UI
```

## Bounded memory

Message history is bounded twice:

- a configurable maximum number of cached channels;
- a configurable maximum number of retained messages per channel.

When a new channel arrives at the channel limit, the least recently used cached channel is discarded. Within a channel, the oldest message is removed before appending a new message at capacity.

The cache stores the existing `Arc<FrontendEvent>` produced by the network boundary instead of copying message bodies again. `channel_id`, `author_username`, and `content` therefore remain single owned allocations for each normalized message event.

## Frame budget

`FrontendState::drain` accepts a maximum number of events to apply per synchronous frame or tick. It never waits for additional events. If the Tokio broadcast receiver reports lag, the skipped event count is accumulated in `DrainReport` and in the state-level diagnostic counter.

This keeps burst traffic from monopolizing a legacy single-core or dual-core UI thread. A frontend can choose a small budget on slow hardware and a larger one on modern systems without changing network behavior.

## Current scope

This state layer intentionally caches only the normalized Chapter 1 event surface. Guild/channel metadata, message edits/deletes, presence, typing, attachments, and richer user records should be added as independently bounded structures when their corresponding frontend features are implemented. The network parser should continue to deserialize only the fields required by those structures.

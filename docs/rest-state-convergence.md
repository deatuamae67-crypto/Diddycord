# Diddycord architecture — Chapter 5 REST/Gateway state convergence

Chapter 5 connects the outbound REST actor to the bounded synchronous presentation cache without turning the cache into a second Discord database.

## Problem

A successful message send has two observable paths:

1. the REST response returns the newly created Discord message;
2. the Gateway later emits `MESSAGE_CREATE` for that same message.

If both paths are appended blindly, the UI briefly or permanently shows a duplicate. Waiting only for the Gateway avoids duplicates but makes the client feel unnecessarily latent on slow hardware or networks.

## Convergence rule

Cached message creation is now an **upsert by `(channel_id, message_id)`** inside the already bounded channel timeline. A REST success can therefore populate the cache immediately and the later Gateway echo replaces that same cached slot instead of appending another entry.

The search remains newest-first and O(H), where H is the configured per-channel history bound. No global message-ID hash table is introduced. This preserves the project's fixed-memory design and is appropriate while H remains intentionally small.

```text
REST POST success ---------+
                           +--> bounded channel timeline --> UI
Gateway MESSAGE_CREATE ----+
                 same id => replace, not append
```

## REST event application

`FrontendState::apply_rest()` handles successful outbound results:

- `MessageSent`: inserts/upserts the returned message when the response includes an author username;
- `MessageEdited`: upserts the complete returned message, or applies a partial content update when the defensive REST parser did not receive author data;
- `MessageDeleted`: removes the matching cached message;
- `Failed`: does not mutate presentation state.

`FrontendState::drain_rest()` mirrors the Gateway `drain()` API. It uses `try_recv()`, accepts a per-frame/tick budget, never waits for I/O, and accounts REST broadcast lag separately from Gateway lag.

## Failure semantics

The cache only mutates after a successful REST result. Submission failures, HTTP failures, timeouts, and rejected Discord requests remain visible through `RestEvent::Failed` but do not speculatively modify state. This avoids rollback bookkeeping and keeps the first implementation deterministic.

A transport failure after a POST is intentionally not auto-retried by the REST actor because the server may already have accepted the message. The eventual Gateway event can still populate the state if Discord accepted it.

## Memory and CPU policy

No new unbounded collection is added. REST and Gateway receivers remain bounded broadcasts, outbound commands remain a bounded `mpsc`, channel count remains LRU-bounded, and each channel history remains length-bounded. Duplicate detection costs at most one scan over the small cached history and allocates no index entry.

This keeps the synchronous state layer usable on the Core 2 Duo / Windows 7 and Android 6 targets while allowing a modern frontend to drain both transports every frame.

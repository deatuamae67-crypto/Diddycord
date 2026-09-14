# Diddycord architecture — Chapter 4 outbound REST

Chapter 4 adds the outbound Discord HTTP path needed for a usable client surface while keeping it isolated from Gateway heartbeats and from the synchronous renderer.

## Actor model

`RestDispatcher` owns one asynchronous HTTP client and one bounded `tokio::sync::mpsc` command queue. `RestHandle` is cheap to clone and exposes non-blocking `try_*` methods for the synchronous frontend.

```text
frontend -> try_send / try_edit / try_delete
              |
              v
        bounded mpsc queue
              |
              v
       RestDispatcher task
              |
              v
      Discord REST API v9
              |
              v
    bounded broadcast results -> frontend
```

A full queue rejects new work immediately instead of blocking a render loop or growing memory without bound.

## Supported operations

The first outbound surface intentionally stays narrow:

- create a text message;
- edit a text message;
- delete a text message.

Channel and message IDs are validated as decimal Discord snowflakes before they are inserted into request paths. Text-only sends reject empty content and enforce the 2000-character limit before allocating a command.

## TLS and HTTP

The REST path uses `reqwest` with default features disabled and the WebPKI/rustls backend only. Native TLS, cookies, compression, HTTP/2, SOCKS, system proxy discovery, multipart support, and blocking APIs are not enabled.

The client keeps at most one idle connection to `discord.com`, uses explicit connect/request timeouts, marks the Authorization header as sensitive, and refuses redirects so a credential cannot be forwarded to another host.

## Rate limiting

The dispatcher serializes mutating requests. This is deliberately conservative for an interactive client and has two useful properties on old hardware: deterministic memory use and no per-route task fan-out.

It observes `Retry-After`, `X-RateLimit-Remaining`, and `X-RateLimit-Reset-After`. HTTP 429 responses are delayed and retried inside the dispatcher up to a bounded retry budget. A bucket exhaustion delay is conservatively applied to the whole single-worker queue. This can under-utilize independent Discord rate-limit buckets, but it is safe and avoids a route/bucket hash table until profiling or actual throughput requirements justify one.

Ordinary network timeouts and ambiguous transport failures are **not** retried automatically. In particular, retrying a POST after an uncertain network failure could duplicate a message. The result event instead tells the caller whether a failure is potentially retryable.

## Defensive response handling

REST response bodies are streamed into a bounded buffer with a 256 KiB ceiling. Success responses are parsed selectively into only `id`, `channel_id`, `author.username`, and `content`; generic JSON DOMs are not built. Discord error responses retain only the human-readable `message` and `retry_after` fields when present.

## Platform boundary

The dispatcher is runtime-agnostic beyond Tokio. Desktop can spawn `RestDispatcher::run()` on the same optimized runtime as the Gateway. Android can keep the exact same Rust worker behind a JNI layer. The synchronous UI only holds `RestHandle` plus a broadcast receiver and therefore does not need to lock the network executor.

# Chapter 7 — bounded message-history bootstrap

A live Gateway stream is not enough to render a useful channel immediately after startup: it only supplies new events that arrive after the session is connected. Chapter 7 adds explicit recent-history reads through the existing bounded REST actor and merges those results into the same presentation cache used by Gateway events.

## Goals

The history path must preserve the core constraints used by the rest of Diddycord:

- no blocking work on the synchronous frontend thread;
- no unbounded queue or message database;
- no generic JSON DOM for Discord payloads;
- no second cache that must later be reconciled with Gateway state;
- no REST request unless the application explicitly asks for a page;
- preserve newer live state when a slower history response arrives afterward.

## REST API

The cloneable `RestHandle` exposes two non-blocking submission calls:

```rust
rest.try_fetch_messages(channel_id, 50)?;
rest.try_fetch_messages_before(channel_id, oldest_message_id, 50)?;
```

Both calls validate channel/message IDs as decimal Discord snowflakes before URL construction. The requested page size is restricted to Discord's 1–100 message range.

Commands enter the same bounded `mpsc` queue as send/edit/delete operations. They therefore share one asynchronous dispatcher and its conservative rate-limit handling rather than spawning per-request tasks.

## Response bounds

Ordinary action responses retain the existing 256 KiB ceiling. History pages have a separate hard 2 MiB ceiling because even a small number of Discord messages can carry large attachment/embed metadata in the wire payload even though Diddycord does not retain that metadata.

The entire history page is still strictly bounded by both:

1. at most 100 messages requested/accepted; and
2. at most 2 MiB of response body.

If either defensive limit is violated, the operation emits `RestEvent::Failed` and does not mutate presentation state.

## Selective parsing

History JSON is deserialized into a bounded `Vec<ApiMessage<'_>>` whose strings borrow from the response buffer. Only the following fields are retained into owned `RestMessage` values:

- message ID;
- channel ID;
- author username when present;
- text content.

Embeds, attachments, reactions, mentions, flags, member data and other message metadata are skipped by Serde. Every returned message is checked to ensure its channel matches the requested channel and its identifiers are decimal snowflakes.

## Cache merge semantics

Discord's history endpoint returns recent messages newest-first, while `FrontendState` exposes channel timelines oldest-to-newest. History results therefore cannot simply be appended.

`FrontendState::apply_rest()` handles `RestEvent::MessagesFetched` with a bounded in-place merge:

- existing cached IDs are never overwritten by history;
- missing history messages are inserted according to decimal snowflake order;
- the channel's configured message-capacity remains authoritative;
- when capacity is exceeded, the oldest cached message is removed.

Preserving existing IDs is intentional. A Gateway edit/delete/create or a successful outbound REST result may have updated the cache while a history GET was in flight. Treating cached live state as newer prevents a stale history page from rolling presentation state backward.

The merge does not add a global message-ID hash index. Duplicate detection and insertion are O(H) per fetched message, where H is the already bounded per-channel presentation history. For the intended small cache sizes this trades a small bounded amount of CPU for lower persistent memory and simpler synchronization.

## Pagination policy

Diddycord does not automatically crawl channel history. The application decides when to request another page, normally when a channel is selected or the user scrolls to the oldest cached item.

A typical frontend flow is:

1. select a channel;
2. call `try_fetch_messages(channel_id, N)`;
3. drain REST results during the normal frame/tick budget;
4. if more history is needed, read the oldest cached message ID and call `try_fetch_messages_before(...)`.

Because the presentation cache remains bounded, requesting older pages cannot cause unbounded resident history. If a frontend wants durable archival storage, that should be a separate opt-in subsystem rather than part of the latency-sensitive core.

## Failure and rate-limit behavior

History requests use the same dispatcher behavior as action requests:

- HTTP 429 responses are delayed and retried within the bounded retry budget;
- `X-RateLimit-Remaining: 0` schedules a pre-emptive delay;
- transport failures are surfaced as failures rather than hidden behind unbounded retries;
- redirects remain disabled so the Authorization header cannot be forwarded to another host.

A failed history fetch leaves `FrontendState` unchanged.

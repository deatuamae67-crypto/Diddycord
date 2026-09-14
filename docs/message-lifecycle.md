# Diddycord architecture — Chapter 3 message lifecycle

Chapter 3 extends the normalized Gateway boundary from message creation to the complete cached text-message lifecycle required by a responsive frontend.

## Selective Gateway parsing

`MESSAGE_CREATE`, `MESSAGE_UPDATE`, and `MESSAGE_DELETE` are recognized directly from the borrowed Gateway envelope. The parser still does not construct a generic JSON DOM.

For creation, Diddycord retains only:

- message `id`;
- `channel_id`;
- `author.username`;
- `content`.

For partial updates, it retains only the message identity plus `content` and `author.username` when those fields are actually present. Delete events retain only `id` and `channel_id`.

Attachments, embeds, reactions, flags, mentions, member records, guild metadata, and other fields remain skipped by Serde unless a later frontend feature explicitly requires them.

## Cache mutation

`FrontendState` continues to be owned by the synchronous frontend. Message creation appends an immutable shared event to the bounded channel timeline. A message update searches only the already bounded timeline for its channel and replaces the matching cached entry. A delete removes the matching cached entry in place.

Updates for messages that have already fallen outside the bounded history are ignored. This is intentional: the cache is a presentation cache, not a complete Discord database.

The common create path retains the Chapter 2 allocation behavior. Edits may allocate replacement strings because message edits are much less frequent than message creation and keeping mutable shared message objects would require additional synchronization or interior mutability.

## Complexity

For a channel history bound `H`:

- create: amortized O(1);
- update: O(H), searched newest-first;
- delete: O(H).

`H` is deliberately small and configurable, keeping worst-case work deterministic on legacy CPUs. A global message-id index would reduce update/delete lookup time but would add one hash-table entry per cached message, more pointer chasing, and more memory. It is therefore deferred until profiling proves it beneficial.

## Consistency under lag

The frontend transport is still lossy under consumer lag by design. If a create event is skipped but a later update/delete arrives, the mutation simply finds no cached message and exits. The network executor is never blocked trying to repair presentation state.

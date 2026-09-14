# Chapter 13 — current bot identity

The frontend needs a small amount of account identity to render an account header, avatar, and display name. That state changes rarely and has latest-value semantics, so it does not belong in the high-volume `FrontendEvent` broadcast.

## Selective READY parsing

The Gateway `READY` payload already carries the authenticated bot/application user. Diddycord parses that payload a second time with a tiny borrowed struct and retains only:

- user snowflake ID
- username
- optional global display name
- optional discriminator
- optional avatar hash
- bot flag

Guild lists, application metadata, locale, flags, email, MFA state, and unrelated User fields are ignored. The retained strings are copied once into the public `CurrentUser` value only after the payload passes basic ID/name validation.

## Latest-value transport

`NetworkBackbone` owns a `watch::Sender<Option<Arc<CurrentUser>>>`. `NetworkControl` and the backbone can provide receivers through `subscribe_self_user()`.

This gives the desired behavior for desktop and Android frontends:

- identity starts as `None` before the first valid `READY`;
- `READY` publishes the current identity;
- `USER_UPDATE` replaces the current value;
- reconnects and `RESUMED` keep the last known identity until Discord provides a replacement;
- slow consumers cannot build an identity-event backlog because Tokio `watch` retains only the newest value;
- `Arc` lets multiple frontend consumers share the same allocated strings.

`NetworkControl::self_user()` provides an immediate cloned `Arc` snapshot when event-driven observation is unnecessary.

## Display name

`CurrentUser::display_name()` returns `global_name` when Discord supplies one and falls back to `username`. The core does not synthesize legacy tags or perform avatar URL formatting; those remain presentation concerns.

## Security boundary

This feature exposes only the identity of the bot/application authenticated by Diddycord. It does not add user-account token authentication, self-bot behavior, credential discovery, or private account fields.

## Tests

Unit coverage verifies selective `READY` parsing, nullable `USER_UPDATE` profile fields, malformed snowflake rejection, initial `None` state, and propagation through an already-created `NetworkControl`.

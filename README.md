# Diddycord

Diddycord is an experimental ultra-low-overhead Rust Discord Gateway client core. The design target is one codebase that remains responsive on modern desktop hardware while still being practical on legacy systems such as a Core 2 Duo / Windows 7 machine and Android 6-era ARM hardware.

The repository currently contains **Chapter 1: the asynchronous networking backbone**.

## Chapter 1 status

Implemented:

- Tokio multi-thread runtime with a 1â€“4 worker cap, bounded blocking pool, reduced worker stack size, explicit scheduler polling intervals, and cooperative yielding during Gateway bursts.
- Secure WebSocket connection to Discord Gateway **v9** using `tokio-tungstenite` + `rustls`.
- Native-root and WebPKI-root TLS paths for desktop and Android-oriented builds.
- Borrowed outer Gateway JSON parsing with `serde_json::value::RawValue` so large event payloads are not materialized into generic JSON trees.
- Selective `MESSAGE_CREATE` extraction of only `channel_id`, `author.username`, and `content`.
- Bounded `tokio::sync::broadcast` bridge so a slow synchronous UI cannot backpressure networking or heartbeats.
- HELLO, IDENTIFY, RESUME, HEARTBEAT, HEARTBEAT ACK, RECONNECT, INVALID SESSION, close-code classification, session resume, and reconnect backoff.
- First-heartbeat jitter, 1â€“5 second invalid-session delay, and a minimum 5 second IDENTIFY retry interval.
- Tight WebSocket read/write buffers and a hard inbound frame/message ceiling.
- Parser/recovery unit tests.

## Authentication

The current core uses a Discord **bot/application token**. It intentionally does not implement user-token/self-bot authentication.

For the desktop harness:

```text
DISCORD_BOT_TOKEN=<bot token>
DISCORD_INTENTS=37377
```

`DISCORD_INTENTS` is optional. The default enables `GUILDS`, `GUILD_MESSAGES`, `DIRECT_MESSAGES`, and `MESSAGE_CONTENT`. Enable the Message Content privileged intent in the Discord Developer Portal when required for the bot.

Do not commit `.env` files or tokens. `.env.example` exists only as a variable-name reference.

## Build

The project pins Rust **1.77.2** because ordinary Rust Windows targets raised their Windows baseline after that toolchain generation. ThhÈÙY\ÈH›Ú™XİZ[X›H›ÜˆHÚ[™İÜÈÈ\™Ù]™\]Z\™[Y[Ú[HÒHÚXÚÜÈHØ[YHTÔ•ˆÛˆ[Ù\›ˆ[›™\œË‚‚˜˜\Ú˜Ø\™ÛÈZ[K\™[X\ÙB˜‚•H™[X\ÙH›Ùš[H\È[X™\˜][HÚ^™KÜ[[YHÜšY[Y‚‚˜Û[–Ü›Ùš[Kœ™[X\ÙWB›Ü[]™[HÂ›ÈHYB˜ÛÙYÙ[‹][š]ÈHBœ[šXÈH˜X›Ü‚œİš\HœŞ[X›ÛÈ‚š[˜Ü™[Y[[H˜[ÙB˜‚ˆÈÈ[™›ÚYˆÈTHŒÂ‚•HÛÜ™H]Ù[ˆÙ\È›İ\[™ÛˆH\ÚİÜÕRHTKˆÒHÜ›ÜÜËXÚXÚÜÈX\˜Ú[[^X[™›ÚY]THŒËˆH]™[X[[™›ÚY\XØ][ÛˆÚİ[^ÜÙHHXœ˜\H›İYÚHÛX[“’KÓ‘ÈœšYÙH[™[š™XİÜ™Y[X[È›İYÚH\XØ][Ûˆ^Y\ˆ˜]\ˆ[ˆ[š\›Û›Y[˜\šXX›\Ë‚‚•HØ[^HÍˆ˜[Z[H^\İÈ[ˆ›İXš][™Ì‹Xš]\Ù\œÜXÙH˜\šX[È\[™[™ÈÛˆš\›]Ø\™KÙ]šXÙHÛÛ™šYİ\˜][Û‹ÛÈš[˜[XÚØYÚ[™ÈÚİ[™\šYHH\™Ù][™Ù]™Y›Ü™H›Ü[™È\›YXXšK]ØXİ\Ü‚‚ˆÈÈœ›Û[™œšYÙB‚Ü™X]HH™XÙZ]™\ˆ™Y›Ü™H[›š[™ÈH˜XÚØ›Û™N‚‚˜\İ›]˜XÚØ›Û™HH™]ÛÜšĞ˜XÚØ›Û™N›™]ÊÛÛ™šYËMŠNÂ›]]]]™[ÈH˜XÚØ›Û™KœİXœØÜšX™J
NÂ˜‚HŞ[˜Ú›Û›İ\È™[™\‹ÕRHÛÜØ[ˆØ[]™[ËWÜ™XİŠ
XÛ˜ÙH\ˆœ˜[YKİXÚËˆœ›ØYØ\İ\È›İ[™YˆYÙÚ[™ÈÛÛœİ[Y\œÈÜÙHÛ[šY\È[œİXYÙˆ›ØÚÚ[™ÈH™]ÛÜšÈ^Xİ]Ü‹‚‚ˆÈÈ™\ÜÚ]ÜH^[İ]‚˜^œÜ˜ËÛXZ[‹œœÈ\ÚİÜ\›™\ÜÂœÜ˜ËÛX‹œœÈX›XÈXœ˜\Hİ\™˜XÙBœÜ˜ËÙØ]]Ø^KœœÈ\ØÛÜ™Ø]]Ø^H˜[œÜÜ\œÙ\‹X\™X][™™XÛÛ›™XİÙÚXÂœÜ˜ËÜ[[YKœœÈİË[İ™\šXYÚÚ[È[[YHÛÛ™šYİ\˜][Û‚™ØÜËØ\˜Ú]Xİ\™K›Y˜‚ˆÈÈÙXİ\š]H›İ[™\B‚“™]™\ˆ\™XÛÙHÜˆÛÛ[Z]\ØÛÜ™Ü™Y[X[ËˆH™]ÛÜšÚ[™ÈÛÜ™H™X]ÈHÚÙ[ˆ\ÈÛÛ™šYİ\˜][ÛˆÛ›H[™Ù\È›İÙÈ]‚
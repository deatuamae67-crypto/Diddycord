# Gateway heartbeat latency

Chapter 19 exposes a bounded, latest-value view of Discord Gateway heartbeat round-trip time (RTT).

## Measurement

Diddycord timestamps each Gateway heartbeat immediately before it is written to the WebSocket. When Discord replies with opcode `11` (`Heartbeat ACK`), the elapsed monotonic time is published as the current Gateway latency.

Both scheduled heartbeats and server-requested opcode `1` heartbeats are measured. An unsolicited ACK with no tracked heartbeat does not fabricate a sample.

## Public API

`NetworkControl::latency()` returns `Option<Duration>` with the most recent ACK RTT for the current connection. `None` means that the active connection has not produced a valid sample yet, or that there is no active connection.

`NetworkControl::subscribe_latency()` and `NetworkBackbone::subscribe_latency()` expose Tokio `watch` receivers for consumers that want change notifications instead of polling. The channel stores only the latest value; there is no latency history or unbounded queue.

## Reconnect semantics

Latency is deliberately cleared to `None` when a connection attempt starts and immediately when an established Gateway connection exits. This prevents a graphical frontend or embedder from displaying an old RTT while Diddycord is reconnecting, stopped, or handling a fatal close.

A successfully resumed connection starts with no sample and publishes a fresh value after its next heartbeat ACK.

## Failure behavior

The existing missed-heartbeat-ACK recovery remains authoritative. If a heartbeat is still awaiting an ACK when the next scheduled heartbeat deadline is reached, the connection exits into the existing resume/reidentify path. No artificial timeout latency value is published.

## Resource bounds

The feature adds one `watch` slot and one optional monotonic timestamp per network backbone/connection. It does not allocate per heartbeat, retain historical samples, add a task, or alter Gateway intents. Authentication remains bot/application-token only.

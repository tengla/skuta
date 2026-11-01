# Bugs

## Oversized messages fail silently

Found 2026-11-02 by sending increasing body sizes to a real server with `kubemq-js`, using default config (`Broker.MaxPayload` and `Grpc.BodyLimit` at 100 MiB).

| Type | Largest body that worked | Smallest that failed | Result when too big |
|---|---|---|---|
| Queue | 33,554,176 bytes (about 32 MiB) | 33,554,432 bytes (32 MiB) | Single send fails silently; a batch send reports `message too large` per message |
| Events store | 33,554,176 bytes (about 32 MiB) | 33,554,432 bytes (32 MiB) | Throws `message too large` |
| Events | 63 MiB | 64 MiB | Send succeeds, message is dropped |

The 32 MiB limit on stored messages is expected: it's JetStream's file store limit (`rlBadThresh` in `nats-server`), and `Broker.MaxPayload` doesn't change it. The bugs are in how failures are reported.

### Events of 64 MiB or more are dropped

`sendEvent` succeeds but the subscriber never gets the message. The server logs `nats: slow consumer, messages dropped ... for subscription on "_EVENTS_.<channel>"`.

Likely cause, not confirmed by a fix: the 64 MiB default pending limit on NATS subscriptions (`DefaultSubPendingBytesLimit` in `nats.go`). `pkg/client` doesn't raise it for events; `pkg/jstore/subscription.go:178` does for its own inbox.

Commands and queries also use core NATS but weren't tested.

### Oversized single queue sends look successful

`sendQueueMessage` doesn't throw. It returns a `messageId` with `sentAt` set to `1970-01-01T00:00:00.000Z`, and the message is never stored.

Probably the SDK ignoring the server's error, since `sendQueueMessagesBatch` reports it correctly. The raw response to the single send wasn't inspected.

### README overstates the events limit

`README.md:98` says `broker.maxPayload` (100 MiB) applies to events, commands and queries. For events, delivery stops working at 64 MiB.

### Client stops working after oversized events (cause unknown)

After oversized events of 64 to 70 MiB in one client session, later subscribes for 80 and 90 MiB failed with `8 RESOURCE_EXHAUSTED: Bandwidth exhausted or memory limit exceeded`. A later 31 MiB queue send on that client timed out after 60 s.

The same 31 MiB send worked on a fresh client and server, so this needs its own reproduction. It isn't clear whether the client or the server is at fault.

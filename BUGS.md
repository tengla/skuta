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

## Cluster: requests lost around channel changes

Found 2026-11-12 by running `test/bench` against a three-node cluster on k3s (three pods on one Raspberry Pi, reached through a Service, so the benchmark's connections land on different nodes). A single node isn't affected.

### Sends hung for 60 seconds (fixed, uncommitted)

The first run failed most scenarios with several senders on a stored channel: `queue/send/1KB/x8` (5 errors), `queue/batch/1KB/100-per-call/x4`, `queue/pipeline/1KB/4-senders-4-receivers` and `events_store/send/1KB/x8` all reported `context deadline exceeded`. No pod restarted and the servers logged no error.

A publish that arrives while a replicated stream is being created or deleted can go unanswered: no reply and no "no responders" either, so the sender waited out `publishTimeout`. When it happened locally the stream existed and had a leader, and a publish sent a few seconds later was stored. Why the first one is dropped wasn't established.

Fix in `pkg/jstore/jstore.go`: with more than one replica, a publish carries a message ID (`Nats-Msg-Id`) and is sent again when no reply comes, first after 250 ms; the stream's duplicate window keeps one copy. Single-node publishing is unchanged. `TestCluster_SendsAroundChannelCreateAndDelete` in `services/broker/cluster_test.go` covers it and passed six runs in a row. On the Pi, all nine benchmark scenarios then completed without errors.

Not verified: `pkg/client`, `interfaces/grpc`, `interfaces/rest` and `services/array` haven't run against the fix.

### Pipeline receives stall for about 10 seconds (open, cause unknown)

After the fix, `queue/pipeline/1KB/4-senders-4-receivers` has a p99 of 10.3 s on three nodes, against 1.1 s on one node, and 106 msg/s against 279. Nothing is lost and nothing fails.

Suspected, not investigated: a subscriber's request (creating the consumer, or a pull) is dropped the way publishes were and only recovered by a timeout. `ensureConsumerRetrying` in `pkg/jstore/subscription.go` retries after `apiTimeout`, which is 10 s.

## Known limitation: port 8080 has no TLS

The `security` section applies to gRPC and REST only. The dashboard, `/api` and `/mcp` on `Api.Port` are plain HTTP by design: requiring client certificates there would lock out browsers, Kubernetes probes and most MCP clients. Bearer tokens and the dashboard's session cookie are only protected by whatever carries the traffic.

Reach the dashboard with `kubectl port-forward`, which tunnels through the Kubernetes API server, or put a proxy that terminates TLS in front of it. Don't expose port 8080 directly.

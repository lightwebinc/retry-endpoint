# bitcoin-retry-endpoint — Architecture

## Overview

`bitcoin-retry-endpoint` sits alongside `bitcoin-shard-listener` on the multicast
fabric. It joins all shard groups, caches every BRC-124 frame it receives, and
serves unicast NACK requests from listeners that detect sequence gaps. On a cache
hit it retransmits the frame via multicast egress and/or directly to the requesting
listener via unicast, then sends an ACK response. On a miss it sends a MISS
response so the listener can escalate immediately to the next endpoint.

Dynamic endpoint discovery is provided by the ADVERT beacon: the endpoint
periodically multicasts a 56-byte advertisement so listeners can maintain a
priority-sorted registry without static configuration (BRC-126).

```
BSV senders
   │
   ▼
bitcoin-shard-proxy  ──UDP multicast──▶ FF05::<shard>:9001
                                              │
              ┌───────────────────────────────┤
              │                               │
              ▼                               ▼
bitcoin-shard-listener              bitcoin-retry-endpoint
(gap detected → NACK)  ──UDP──▶  [nack-addr]:9300
              │                        │ lookup cache
              │                        ├─ HIT  → retransmit (multicast and/or unicast) + ACK
              │                        └─ MISS → MISS response → escalate
              ◀── ACK/MISS ────────────┘
```

## Ingress (multicast receive)

A single goroutine opens a UDP socket with `SO_REUSEPORT` on the configured
listen port, joins all `NumGroups` shard groups, and writes each received frame to
the cache with the configured TTL.

**Why one worker:** Linux delivers multicast datagrams to **every** socket in a
`SO_REUSEPORT` group — there is no load balancing for multicast. Running multiple
workers would store each frame N times and drive N-fold cache churn. A single
worker avoids this entirely. (`SO_REUSEPORT` load-balancing applies to unicast
UDP only.)

## Cache

Two backends are supported:

| Backend  | Storage                                    | Dedup                 | Notes                                         |
| -------- | ------------------------------------------ | --------------------- | --------------------------------------------- |
| `memory` | In-process freecache (60 s TTL by default) | None                  | Single-node; cache lost on restart            |
| `redis`  | External Redis (`bre:frame:<key>`)         | Cross-instance SET NX | Shared across all endpoints; survives restart |

Cache keys use a dual-index scheme:

- `0x01 ∥ CurSeq` (8 B) → raw frame bytes (primary)
- `0x00 ∥ PrevSeq` (8 B) → CurSeq pointer (8 B) (secondary)

The secondary index lets the NACK server resolve a lookup by `PrevSeq` (forward
gap fill) even when the listener only knows the previous frame's `CurSeq`.

## NACK server

`NACK_WORKERS` goroutines share a single `net.PacketConn` bound to
`[nackBindAddr]:nack-port`. Each worker:

1. Reads one 24-byte NACK datagram (BRC-126 wire format).
2. Applies four-tier rate limiting (per-IP, per-chain, per-sequence pre-lookup;
   per-group post-lookup). Pre-lookup drops are silent. The group tier skips
   the retransmit but still sends ACK so the listener does not escalate.
3. Looks up the frame in the cache by `LookupType` + `LookupSeq`.
4. On **hit**: dispatches `retransmit.Send`, then sends a 16-byte ACK (unless
   `-suppress-ack`).
5. On **miss**: sends a 16-byte MISS (unless `-suppress-miss`). The listener
   escalates to the next endpoint immediately.

### NACK wire format (BRC-126) — 24 bytes

| Offset | Size | Field      | Value / notes                                          |
| ------ | ---- | ---------- | ------------------------------------------------------ |
| 0      | 4    | Magic      | 0xE3E1F3E8                                             |
| 4      | 2    | ProtoVer   | 0x02BF                                                 |
| 6      | 1    | MsgType    | 0x10 (NACK)                                            |
| 7      | 1    | LookupType | 0x00 = by PrevSeq; 0x01 = by CurSeq                    |
| 8      | 8    | LookupSeq  | uint64 BE; the XXH64 hash to look up                   |
| 16     | 8    | ChainID    | uint64 BE; initial CurSeq of the chain; 0 = orphan gap |

### ACK/MISS wire format — 16 bytes

| Offset | Size | Field    | Value / notes                                             |
| ------ | ---- | -------- | --------------------------------------------------------- |
| 0      | 4    | Magic    | 0xE3E1F3E8                                                |
| 4      | 2    | ProtoVer | 0x02BF                                                    |
| 6      | 1    | MsgType  | 0x12 = ACK; 0x11 = MISS                                   |
| 7      | 1    | Flags    | 0x01 on ACK; 0x00 on MISS                                 |
| 8      | 8    | CurSeq   | uint64 BE; CurSeq of the resolved frame (ACK) or 0 (MISS) |

### NACK bind address

On multi-homed Linux hosts (management NIC + fabric NIC), the kernel may select a
SLAAC-derived address as the source of outgoing ACK/MISS responses if the NACK
socket is bound to `[::]`. Listeners using connected sockets or nftables rules
keyed on the advertised NACKAddr will silently drop such responses.

The server binds to `[nackBindAddr]:nack-port` where `nackBindAddr` is resolved at
startup: the explicit `-nack-addr` flag if set, otherwise the first non-link-local
global-unicast IPv6 address on the egress interface. This ensures ACK/MISS
responses are always sourced from the address advertised in the ADVERT beacon.

## Retransmit

The endpoint supports two retransmit modes, which can be enabled independently or
together via the ADVERT beacon flags:

| Mode      | Beacon flag               | Config flag                                | Behaviour                                                          |
| --------- | ------------------------- | ------------------------------------------ | ------------------------------------------------------------------ |
| Multicast | `FlagMulticastRetransmit` | `-beacon-flags-multicast` (default `true`) | Frame sent to `FF05::<shard>:egress-port` on each egress interface |
| Unicast   | `FlagUnicastRetransmit`   | `-beacon-flags-unicast` (default `false`)  | Frame sent directly back to the NACK sender's address              |

### Multicast retransmit

`retransmit.Retransmitter` holds one egress UDP socket per configured egress
interface (set via `-egress-iface`). On a cache hit it:

1. Decodes the cached frame to extract the TxID.
2. Derives the shard group address from the TxID via `shard.Engine`.
3. Sends the raw frame bytes verbatim to `FF05::<shard>:egress-port` on each
   egress interface.

Listeners that receive the retransmitted multicast frame call
`nack.Tracker.Observe`; if the incoming `CurSeq` matches a pending gap entry the
gap is auto-closed inline, before the next sweeper tick.

### Unicast retransmit

When unicast retransmit is enabled, the NACK server sends the raw frame directly
back to the listener that issued the NACK, using the source address from the
incoming datagram. This guarantees delivery to the specific listener without
relying on multicast fabric propagation, but does not benefit other listeners
that may have the same gap.

Both modes can fire for the same NACK when both beacon flags are set.

### Cross-instance deduplication

When the Redis backend is in use, a `SET NX` with the `CurSeq` key and a
`dedup-window` TTL (default 60 s) prevents two endpoints from both
retransmitting the same frame. The first endpoint to acquire the key wins;
others skip the send.

## Beacon discovery (BRC-126)

The beacon sender runs as a separate goroutine and fires every `beacon-interval`
(default 60 s). It sends a 56-byte ADVERT datagram to the configured beacon
multicast group:

| `-beacon-scope` | Group           | Purpose                            |
| --------------- | --------------- | ---------------------------------- |
| `site`          | `FF05::FF:FFFD` | Intra-site listener discovery      |
| `global`        | `FF0E::FF:FFFD` | Inter-AS discovery via MP-BGP MVPN |
| `both`          | both            | Mixed deployments                  |

The ADVERT carries the endpoint's NACKAddr, NACKPort, Tier, Preference, Flags,
and a stable InstanceID (CRC32c hash of the hostname). Listeners upsert endpoints
into a `discovery.Registry` sorted by **(Tier ASC, Preference DESC)**; entries not
refreshed within `3 × beacon-interval` are evicted automatically.

**Interface binding:** The beacon socket sets `IPV6_MULTICAST_IF` explicitly after
`net.DialUDP` to force datagrams out the fabric NIC (`MC_IFACE`). Without this the
kernel may route `FF05::` via the management interface (lower-metric default route).

## Rate limiting

Four tiers applied in order. Pre-lookup tiers drop silently (no response sent).
The post-lookup group tier skips the retransmit but still sends ACK — the
listener must not escalate when the frame is available.

| #   | Level                 | Algorithm      | Position    | Config flags                              |
| --- | --------------------- | -------------- | ----------- | ----------------------------------------- |
| 1   | Per source IP         | Token bucket   | Pre-lookup  | `-rl-ip-rate`, `-rl-ip-burst`             |
| 2   | Per (srcIP, ChainID)  | Sliding window | Pre-lookup  | `-rl-chain-rate`, `-rl-chain-window`      |
| 3   | Per `LookupSeq`       | Sliding window | Pre-lookup  | `-rl-sequence-max`, `-rl-sequence-window` |
| 4   | Per (srcIP, groupIdx) | Token bucket   | Post-lookup | `-rl-group-rate`, `-rl-group-burst`       |

`ChainID=0` (orphan/unattributed gap) bypasses tier 2 to avoid bucketing all
unattributed gaps from the same source together.

## Graceful shutdown

On `SIGINT` or `SIGTERM`:

1. If `-drain-timeout` is non-zero, `rec.SetDraining()` flips `/readyz` to `503`
   and the process sleeps for that duration while the ingress and NACK workers
   continue serving.
2. The root `context.Context` is cancelled, unblocking `ingress.Run` and
   `server.Run`. The `done` channel is closed, signalling the metrics server.
3. `main` waits for all goroutines via `sync.WaitGroup`, then flushes the OTLP
   exporter before returning.

## Package structure

```
bitcoin-retry-endpoint/
  main.go          entry point; wires config → cache → ingress → server → beacon
  config/          runtime configuration (flags + env vars + validation)
  ingress/         single-worker multicast receive loop; writes to cache
  cache/           Cache interface + memory (freecache) and redis backends
  server/          UDP NACK receive pool; rate-limit → cache lookup → retransmit
  retransmit/      multicast + unicast retransmit egress; cross-instance dedup
  beacon/          ADVERT beacon sender; IPV6_MULTICAST_IF binding
  ratelimit/       four-tier rate limiter (IP, chain, sequence, group)
  metrics/         OTel + Prometheus instrumentation (bre_ prefix)
```

Protocol primitives are provided by
[`github.com/lightwebinc/bitcoin-shard-common`](https://github.com/lightwebinc/bitcoin-shard-common):

```
bitcoin-shard-common/
  frame/    v1/BRC-124 wire format: Decode, Encode, constants, errors
  shard/    txid → group index → IPv6 multicast address derivation
  seqhash/  XXH64 hash chain for PrevSeq/CurSeq stamping
```

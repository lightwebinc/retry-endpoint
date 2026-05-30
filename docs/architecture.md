# retry-endpoint — Architecture

## Overview

`retry-endpoint` sits alongside `shard-listener` on the multicast
fabric. It joins all shard groups plus `GroupBlockBroadcast` (BRC-131 / BRC-134) and optionally
`GroupSubtreeAnnounce` (BRC-132), caches every frame it receives, and serves
unicast NACK requests from listeners that detect sequence gaps.

Foundational concepts (shard hierarchy, frame versions, NACK semantics) live in
[multicast-skills/architecture.md](../../../multicast-skills/architecture.md) and
[multicast-skills/protocol.md](../../../multicast-skills/protocol.md); BRC wire formats in
[bsv-multicast/docs/](../../../bsv-multicast/docs/). On a cache hit it
retransmits the frame via multicast egress and/or directly to the requesting listener
via unicast, then sends an ACK response. On a miss it sends a MISS response so the
listener can escalate immediately to the next endpoint.

Dynamic endpoint discovery is provided by the ADVERT beacon: the endpoint
periodically multicasts a 56-byte advertisement so listeners can maintain a
priority-sorted registry without static configuration (BRC-126).

```
BSV senders
   │
   ▼
shard-proxy  ──UDP multicast──▶ FF05::<shard>:9001
                                              │
              ┌───────────────────────────────┤
              │                               │
              ▼                               ▼
shard-listener              retry-endpoint
(gap detected → NACK)  ──UDP──▶  [nack-addr]:9300
              │                        │ lookup cache
              │                        ├─ HIT  → retransmit (multicast and/or unicast) + ACK
              │                        └─ MISS → MISS response → escalate
              ◀── ACK/MISS ────────────┘
```

## SSM (RFC 4607) mode

When `-source-mode=ssm` the retry-endpoint operates as both an SSM
emitter and an SSM consumer:

- **Beacon emit** binds `-bind-source` via `net.DialUDP(laddr=...)`
  so listeners can pre-declare this retry-endpoint in their
  `ssm-bootstrap-beacon` list. Each replica MUST use a distinct
  `bindSource` (anycast / ECMP-shared sources break PIM-SSM RPF).
- **Data-plane ingress** uses the shared `shard-common/netjoin.Join`
  helper, which branches `IPV6_JOIN_GROUP` vs
  `MCAST_JOIN_SOURCE_GROUP` by the per-group source list. Source lists
  come from per-control-group bootstrap (`-ssm-bootstrap-manifest`,
  `-ssm-bootstrap-beacon`, `-ssm-bootstrap-subtree-announce`) resolved
  via `shard-common/bootstrap.Resolver` (DNS names or IPv6 literals;
  fail-closed startup; last-good retention on refresh failures).
- **Addressing** uses `FF35::B:idx` (site SSM) or `FF3E::B:idx`
  (global SSM); ASM at global scope is rejected per RFC 8815.

See the [SSM Support Plan](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/SourceSpecificMulticast/ssm-support-plan.md)
for fabric prerequisites (PIM-SSM, MLDv2, raised `mld_max_msf`).

## Ingress (multicast receive)

A single goroutine opens a UDP socket with `SO_REUSEPORT` on the configured
listen port, joins all `NumGroups` shard groups, and writes each received frame to
the cache with the configured TTL.

In addition to the shard groups, the ingress worker always joins `GroupBlockBroadcast`
(`FF0X::B:FFFE`) to cache BRC-131 block control frames and BRC-134 anchor transaction
frames (FrameVerV6). When `-subtree-data-enabled=true`, it also joins
`GroupSubtreeAnnounce` (`FF0X::B:FFFB`) to cache BRC-132 subtree data frames. The cache
key is frame-version-agnostic: `HashKey (8B) ∥ SeqNum (8B)` → raw frame bytes regardless of
frame type, so BRC-131, BRC-132, and BRC-134 frames are served on NACK request with the same
lookup path as BRC-124/BRC-128 frames.

See [bsv-multicast/docs/brc-134-anchor-transactions.md](../../../bsv-multicast/docs/brc-134-anchor-transactions.md)
for the anchor frame wire format.

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

Cache keys use a single 16-byte key:

- `HashKey (8B) ∥ SeqNum (8B)` → raw frame bytes

`HashKey` is a stable per-flow identifier (`XXH64(senderIPv6 ∥ groupIdx ∥ subtreeID)`)
stamped by the proxy. `SeqNum` is a monotonic per-flow counter. Together they
uniquely identify every frame within a flow. No secondary index is needed.

## NACK server

`NACK_WORKERS` goroutines share a single `net.PacketConn` bound to
`[nackBindAddr]:nack-port`. Each worker:

1. Reads one 64-byte NACK datagram (BRC-126 wire format).
2. Applies four-tier rate limiting (per-IP, per-HashKey, per-SeqNum pre-lookup;
   per-group post-lookup). Pre-lookup drops are silent. The group tier skips
   the retransmit but still sends ACK so the listener does not escalate.
3. Looks up the frame in the cache by `HashKey ∥ StartSeq` (16-byte key).
4. On **hit**: dispatches `retransmit.Send`, then sends a 16-byte ACK (unless
   `-suppress-ack`).
5. On **miss**: sends a 16-byte MISS (unless `-suppress-miss`). The listener
   escalates to the next endpoint immediately.

### NACK wire format (BRC-126) — 64 bytes

| Offset | Size | Field     | Value / notes                                                 |
| ------ | ---- | --------- | ------------------------------------------------------------- |
| 0      | 4    | Magic     | 0xE3E1F3E8                                                    |
| 4      | 2    | ProtoVer  | 0x02BF                                                        |
| 6      | 1    | MsgType   | 0x10 (NACK)                                                   |
| 7      | 1    | Flags     | Reserved; must be 0x00                                        |
| 8      | 8    | HashKey   | uint64 BE; stable per-flow XXH64 identifier                   |
| 16     | 8    | StartSeq  | uint64 BE; first missing SeqNum (inclusive)                    |
| 24     | 8    | EndSeq    | uint64 BE; last missing SeqNum (inclusive; == StartSeq for 1)  |
| 32     | 32   | SubtreeID | 32-byte batch identifier; zeros = unset                       |

### ACK/MISS wire format — 16 bytes

| Offset | Size | Field    | Value / notes                                             |
| ------ | ---- | -------- | --------------------------------------------------------- |
| 0      | 4    | Magic    | 0xE3E1F3E8                                                |
| 4      | 2    | ProtoVer | 0x02BF                                                    |
| 6      | 1    | MsgType  | 0x12 = ACK; 0x11 = MISS                                    |
| 7      | 1    | Flags    | 0x01 on ACK (multicast); 0x02 (unicast); 0x00 on MISS      |
| 8      | 8    | SeqNum   | uint64 BE; SeqNum of the resolved frame (ACK) or 0 (MISS)  |

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

1. Inspects the cached frame's version byte to determine the egress group:
   - V2 (BRC-124/BRC-128): derives the shard group from the TxID via `shard.Engine`
   - V4 (BRC-131): retransmits to `GroupBlockBroadcast` (`FF0X::B:FFFE`)
   - V5 (BRC-132): retransmits to `GroupSubtreeAnnounce` (`FF0X::B:FFFB`)
   - V6 (BRC-134 anchor): retransmits to `GroupBlockBroadcast` (`FF0X::B:FFFE`)
2. Sends the raw frame bytes verbatim to the derived group address on each
   egress interface.

Listeners that receive the retransmitted multicast frame call
`nack.Tracker.Observe`; if the incoming `SeqNum` matches a pending gap entry the
gap is auto-closed inline, before the next sweeper tick.

### Unicast retransmit

When unicast retransmit is enabled, the NACK server sends the raw frame directly
back to the listener that issued the NACK, using the source address from the
incoming datagram. This guarantees delivery to the specific listener without
relying on multicast fabric propagation, but does not benefit other listeners
that may have the same gap.

Both modes can fire for the same NACK when both beacon flags are set.

### Cross-instance deduplication

When the Redis backend is in use, a `SET NX` with the `HashKey∥SeqNum` key and a
`dedup-window` TTL (default 60 s) prevents two endpoints from both
retransmitting the same frame. The first endpoint to acquire the key wins;
others skip the send.

## Beacon discovery (BRC-126)

The beacon sender runs as a separate goroutine and fires every `beacon-interval`
(default 60 s). It sends a 56-byte ADVERT datagram to the configured beacon
multicast group:

| `-beacon-scope` | Group           | Purpose                            |
| --------------- | --------------- | ---------------------------------- |
| `site`          | `FF05::B:FFFD` | Intra-site listener discovery      |
| `global`        | `FF0E::B:FFFD` | Inter-AS discovery via MP-BGP MVPN |
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
| 2   | Per (srcIP, HashKey)  | Sliding window | Pre-lookup  | `-rl-chain-rate`, `-rl-chain-window`      |
| 3   | Per SeqNum            | Sliding window | Pre-lookup  | `-rl-sequence-max`, `-rl-sequence-window` |
| 4   | Per (srcIP, groupIdx) | Token bucket   | Post-lookup | `-rl-group-rate`, `-rl-group-burst`       |

`HashKey=0` (unstamped frame) bypasses tier 2 to avoid bucketing all
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
retry-endpoint/
  main.go          entry point; wires config → cache → ingress → server → beacon
  config/          runtime configuration (flags + env vars + validation)
  ingress/         single-worker multicast receive loop; writes to cache
  cache/           Cache interface + memory (freecache) and redis backends
  server/          UDP NACK receive pool; rate-limit → cache lookup → retransmit
  retransmit/      multicast + unicast retransmit egress; cross-instance dedup
  beacon/          ADVERT beacon sender; IPV6_MULTICAST_IF binding
  ratelimit/       four-tier rate limiter (IP, HashKey, SeqNum, group)
  metrics/         OTel + Prometheus instrumentation (bre_ prefix)
```

Protocol primitives are provided by
[`github.com/lightwebinc/shard-common`](https://github.com/lightwebinc/shard-common):

```
shard-common/
  frame/    BRC-12/BRC-124/BRC-128/BRC-131/BRC-132/BRC-134 wire format: Decode, Encode, constants
  shard/    txid → group index → IPv6 multicast address derivation;
            control group constants and GroupAddr
  seqhash/  XXH64 flow hash for HashKey computation
```

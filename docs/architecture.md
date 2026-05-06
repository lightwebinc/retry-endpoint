# bitcoin-retry-endpoint — Architecture

## Overview

`bitcoin-retry-endpoint` sits alongside `bitcoin-shard-listener` on the multicast
fabric. It joins all shard groups, caches every BRC-124 frame it receives, and
serves unicast NACK requests from listeners that detect sequence gaps. On a cache
hit it retransmits the frame via multicast egress and sends an ACK response; on a
miss it sends a MISS response so the listener can escalate immediately to the next
endpoint.

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
              │                        ├─ HIT  → retransmit multicast + ACK
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

| Backend | Storage | Dedup | Notes |
|---------|---------|-------|-------|
| `memory` | In-process freecache (60 s TTL by default) | None | Single-node; cache lost on restart |
| `redis` | External Redis (`bre:frame:<key>`) | Cross-instance SET NX | Shared across all endpoints; survives restart |

Cache keys use a dual-index scheme:
- `0x01 ∥ CurSeq` (8 B) → raw frame bytes (primary)
- `0x00 ∥ PrevSeq` (8 B) → CurSeq pointer (8 B) (secondary)

The secondary index lets the NACK server resolve a lookup by `PrevSeq` (forward
gap fill) even when the listener only knows the previous frame's `CurSeq`.

## NACK server

`NACK_WORKERS` goroutines share a single `net.PacketConn` bound to
`[nackBindAddr]:nack-port`. Each worker:

1. Reads one 24-byte NACK datagram (BRC-126 wire format).
2. Applies two-level rate limiting (per-IP token bucket, per-LookupSeq sliding
   window). Drops exceeding the limit are silent.
3. Looks up the frame in the cache by `LookupType` + `LookupSeq`.
4. On **hit**: dispatches `retransmit.Send`, then sends a 16-byte ACK (unless
   `-suppress-ack`).
5. On **miss**: sends a 16-byte MISS (unless `-suppress-miss`). The listener
   escalates to the next endpoint immediately.

### NACK wire format (BRC-126) — 24 bytes

```text
Offset  Size  Field        Value / notes
------  ----  -----        -------------
     0     1  LookupType   0x00 = by PrevSeq; 0x01 = by CurSeq
     1     7  Reserved     0x00
     8     8  LookupSeq    uint64 BE; the XXH64 hash to look up
    16     8  Reserved     0x00
```

### ACK/MISS wire format — 16 bytes

```text
Offset  Size  Field        Value / notes
------  ----  -----        -------------
     0     1  MsgType      0x10 = ACK; 0x11 = MISS
     1     7  Reserved     0x00
     8     8  CurSeq       uint64 BE; CurSeq of the resolved frame (ACK) or echo of LookupSeq (MISS)
```

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

`retransmit.Sender` holds one egress UDP socket per configured egress interface
(set via `-egress-iface`). On a cache hit it:

1. Decodes the cached frame to extract the TxID.
2. Derives the shard group address from the TxID via `shard.Engine`.
3. Sends the raw frame bytes verbatim to `FF05::<shard>:egress-port` on each
   egress interface.

**Cross-instance deduplication:** When the Redis backend is in use, a `SET NX`
with the `CurSeq` key and a `dedup-window` TTL (default 60 s) prevents two
endpoints from both retransmitting the same frame. The first endpoint to acquire
the key wins; others skip the send.

Listeners that receive the retransmitted multicast frame call
`nack.Tracker.Observe`; if the incoming `CurSeq` matches a pending gap entry the
gap is auto-closed inline, before the next sweeper tick.

## Beacon discovery (BRC-126)

The beacon sender runs as a separate goroutine and fires every `beacon-interval`
(default 60 s). It sends a 56-byte ADVERT datagram to the configured beacon
multicast group:

| `-beacon-scope` | Group | Purpose |
|-----------------|-------|---------|
| `site` | `FF05::FF:FFFD` | Intra-site listener discovery |
| `global` | `FF0E::FF:FFFD` | Inter-AS discovery via MP-BGP MVPN |
| `both` | both | Mixed deployments |

The ADVERT carries the endpoint's NACKAddr, NACKPort, Tier, Preference, Flags,
and a stable InstanceID (FNV-1a hash of the hostname). Listeners upsert endpoints
into a `discovery.Registry` sorted by **(Tier ASC, Preference DESC)**; entries not
refreshed within `3 × beacon-interval` are evicted automatically.

**Interface binding:** The beacon socket sets `IPV6_MULTICAST_IF` explicitly after
`net.DialUDP` to force datagrams out the fabric NIC (`MC_IFACE`). Without this the
kernel may route `FF05::` via the management interface (lower-metric default route).

## Rate limiting

Two levels, applied in order before any cache lookup:

| Level | Algorithm | Config flags |
|-------|-----------|-------------|
| Per source IP | Token bucket | `-rl-ip-rate`, `-rl-ip-burst` |
| Per `LookupSeq` | Sliding window counter | `-rl-sequence-max`, `-rl-sequence-window` |

All drops are silent (no response sent). `SenderID` was removed from the BRC-124
NACK wire format and is no longer a rate-limiting key.

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
  retransmit/      multicast retransmit egress; cross-instance dedup
  beacon/          ADVERT beacon sender; IPV6_MULTICAST_IF binding
  ratelimit/       per-IP token bucket + per-LookupSeq sliding window
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

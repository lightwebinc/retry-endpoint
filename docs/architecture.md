# retry-endpoint — Architecture

## Overview

`retry-endpoint` sits alongside `shard-listener` on the multicast
fabric. It joins all shard groups plus `GroupBlockBroadcast` (BRC-131 / BRC-134) and optionally
`GroupSubtreeDataAnnounce` (BRC-132), caches every frame it receives, and serves
unicast NACK requests from listeners that detect sequence gaps.

BRC wire formats live in
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
              │                        │ rate-limit tiers (IP / seq / chain / group)
              │                        │ lookup cache
              │                        ├─ HIT      → retransmit (multicast and/or unicast) + ACK
              │                        │            (ACK Flags reflect what was dispatched)
              │                        ├─ MISS     → MISS response → escalate
              │                        └─ THROTTLED → seq/chain/group throttle (opt-in
              │                                       -rl-throttle-response) → listener holds, no escalate
              ◀── ACK/MISS/THROTTLED ──┘
```

The seq/chain/group rate-limit tiers reject honest congestion; with
`-rl-throttle-response` they answer with a 16-byte THROTTLED hint so the listener
holds the gap and retries the same endpoint instead of escalating (without the
flag they stay silent and the listener falls back to timeout + backoff). The
per-IP flood tier never answers (reflection guard). See
[Rate Limiting](configuration.md#rate-limiting).

## Cascaded / downstream-domain retransmission

The NACK protocol is domain-agnostic: the same cache + ADVERT + retransmit
machinery that recovers the proxy→listener hop recovers a
listener→downstream-consumer hop. This lets consumers downstream of a
`shard-listener` request retransmission from a retry-endpoint deployed in
**their own** multicast domain, with no protocol changes.

The pattern relies on the listener's
[multicast egress / domain bridging](https://github.com/lightwebinc/shard-listener/blob/main/docs/configuration.md).
When `-mc-egress-enabled=true` the listener re-emits each filtered frame
**verbatim** (full 92-byte frame; requires `-strip-header=false`) into a
downstream multicast address space, keeping the proxy-stamped `HashKey` /
`SeqNum` / `TxID` intact. Because flow identity survives the bridge:

- downstream consumers detect gaps on the **same** `(HashKey, SeqNum)` flow;
- a downstream retry-endpoint caches the bridged frames by `HashKey ∥ SeqNum`
  and re-derives the shard group from `TxID` deterministically.

Once the upstream listener has completed its own NACK recovery, the verbatim
bridge gives the downstream domain a complete stream; the downstream
retry-endpoint only repairs losses on the listener→downstream hop.

```
upstream fabric                         downstream domain
─────────────                           ─────────────────
shard-proxy ─▶ FF0X::Gᵤ:idx
                  │
          ┌───────┴────────┐
          ▼                ▼
   retry-endpoint     shard-listener ──mc-egress──▶ FF0X::G_d:idx
   (upstream)           (bridge)                       │
                                            ┌──────────┴──────────┐
                                            ▼                     ▼
                                     retry-endpoint        downstream consumer
                                     (downstream)  ◀─NACK──  (listener)
                                            └────ACK+retransmit──▶
```

All retry-endpoint group addresses (ingress join, ADVERT beacon, retransmit
egress) derive from `-scope` + `-mc-group-id`, so a downstream deployment is
configured purely by aligning those with the bridge's egress space:

| Upstream listener (bridge)       | Downstream retry-endpoint           | Downstream consumer  |
| -------------------------------- | ----------------------------------- | -------------------- |
| `-mc-egress-enabled=true`        | —                                   | —                    |
| `-strip-header=false` (default)  | —                                   | —                    |
| `-mc-egress-scope` = X           | `-scope` = X                        | `-scope` = X         |
| `-mc-egress-group-id` = G_d      | `-mc-group-id` = G_d                | `-mc-group-id` = G_d |
| `-mc-egress-port` = P            | `-listen-port`=P, `-egress-port`=P  | `-listen-port` = P   |
| `-shard-bits` = B                | `-shard-bits` = B                   | `-shard-bits` = B    |

Isolate the two domains — translate the group-id (`G_d ≠ Gᵤ`) or confine the
bridge with `-mc-egress-hoplimit` / a distinct `-mc-egress-iface` — so
downstream beacons and retransmits do not leak back upstream.

**Scope:** this covers the BRC-124/BRC-128 transaction hot path, which is the
path the listener's multicast egress re-emits. Control-plane frames
(BRC-131/132/134) take the listener's unicast egress and are not bridged onto
the downstream multicast domain, so they are not recoverable downstream by
this pattern.

### Failure mode: frames the listener never emitted

A downstream retry-endpoint can only serve what it cached, and it caches the
**same** multicast emission the consumers receive. If the listener never put a
frame on the downstream wire — an `mc-egress` `sendto` failure
(`bsl_mc_egress_errors_total`; the frame is dropped, not retried), an egress
interface flap, or in-fabric loss with no successful copy reaching the segment
— then the downstream retry-endpoint missed it identically. A downstream NACK
then returns MISS from **every** downstream endpoint and the gap is
unrecoverable within the downstream domain alone.

This is the one hole the verbatim bridge does not close: completeness of the
upstream stream (after the listener's own NACK recovery) guarantees the listener
*had* the frame, not that it *emitted* it. Mitigations, in preference order:

1. **NACK proxying (self-healing).** Enable `-proxy-enabled` with
   `-upstream-retry-endpoints` so the downstream endpoint recovers cache misses
   from an upstream endpoint and re-serves the downstream domain — no
   per-consumer config. See [NACK proxying](#nack-proxying-cross-domain-recovery)
   below.
2. **Redundant bridge listeners.** Run ≥ 2 listeners bridging the same
   upstream→downstream over independent egress paths. A single egress failure no
   longer creates a domain-wide hole — another bridge emits the frame, and both
   the downstream consumers and the downstream retry-endpoint receive it. Pair
   with the listener's egress dedup (shared `-deployment-id` + Redis) so
   consumers do not see duplicates. Keeps recovery entirely inside the
   downstream domain and also covers control-plane frames (which proxying does
   not).
3. **Upstream fallback tier.** Give downstream consumers a higher-`Tier`
   registry entry (a static `-retry-endpoints` seed) pointing at an *upstream*
   retry-endpoint running `-beacon-flags-unicast=true`. On downstream-MISS
   escalation the consumer reaches back across the domain boundary; the upstream
   endpoint — which received the frame directly from the proxy, independent of
   the failed bridge — unicast-retransmits the verbatim frame. Requires IP
   reachability and that the frame is still within the upstream cache TTL.

### NACK proxying (cross-domain recovery)

When `-proxy-enabled` is set, a local cache miss triggers an **asynchronous**
recovery from an upstream retry-endpoint (the frame's true origin cache),
re-caching the frame and serving the whole downstream domain. This closes the
"frames the listener never emitted" hole without per-consumer fallback config.

```text
downstream consumer        downstream retry-EP            upstream retry-EP
        │ (1) NACK ───────────────▶│ cache miss                       │
        │◀──────── MISS ───────────│ (2) NACK (FlagProxied) ─────────▶│ cache hit
        │                          │◀──── (3) unicast frame ──────────│
        │◀═ (4) multicast retransmit into downstream domain ══════════│
        │   gap auto-fills (nack.Tracker.Observe)
```

- **Async cache-warm:** the server returns MISS immediately and enqueues a
  bounded recovery job (`-proxy-workers`, `-proxy-queue`); no NACK worker is held.
- **Unicast return channel:** the downstream endpoint is not joined to the
  upstream shard groups, so the recovered frame must come back by unicast. A NACK
  carrying `FlagProxied` is always served a unicast copy regardless of the
  upstream's retransmit mode (so the upstream does **not** strictly need
  `-beacon-flags-unicast`, though enabling it is harmless).
- **One-hop bound:** `FlagProxied` (NACK Flags bit `0x01`) stops an upstream
  endpoint from re-proxying. Such requests are served from local cache only.
- **Discovery is static:** upstreams are listed in `-upstream-retry-endpoints`
  (a separated downstream domain generally cannot receive upstream beacons).
- **Sibling dedup:** with a shared cache backend (`redis`/`aerospike`) an
  in-flight SETNX claim (`bre:proxy:` prefix, `-proxy-dedup-window` TTL) ensures
  only one downstream endpoint recovers a given gap; with `memory` the claim is
  per-process and siblings may each proxy (logged at startup).
- **Scope:** BRC-124/128 tx frames (the bridged hot path). Control-plane frames
  are not bridged onto the downstream domain, so use redundant bridges for them.
- **Metrics:** `bre_proxy_requests_total`, `bre_proxy_recovered_total`,
  `bre_proxy_failed_total{reason}`, `bre_proxy_inflight_dedup_total`,
  `bre_proxy_queue_dropped_total`. The endpoint also advertises ADVERT
  `HasParent` (`0x0002`) while proxying is enabled.

**Operational note:** all proxied NACKs share the downstream endpoint's source
IP, so the upstream's per-IP rate limit (`-rl-ip-rate`) may throttle a large
recovery burst — raise it or exempt proxy source IPs upstream.

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

See the [SSM Support Plan](https://github.com/lightwebinc/bsv-multicast/blob/main/DESIGN.md#source-specific-multicast-ssm)
for fabric prerequisites (PIM-SSM, MLDv2, raised `mld_max_msf`).

## Ingress (multicast receive)

A single goroutine opens a UDP socket with `SO_REUSEADDR` on the configured
listen port, joins all `NumGroups` shard groups, and writes each received frame to
the cache with the configured TTL. `SO_REUSEADDR` (not `SO_REUSEPORT`) is the
cross-EUID co-bind path: it lets this socket co-exist with a co-resident
shard-listener running under a **different** user on a collapsed node — both
receive the same multicast group.

In addition to the shard groups, the ingress worker always joins `GroupBlockBroadcast`
(`FF0X::B:FFFE`) to cache BRC-131 block control frames and BRC-134 anchor transaction
frames (FrameVerV6). When `-subtree-data-enabled=true`, it also joins
`GroupSubtreeDataAnnounce` (`FF0X::B:FFFB`) to cache BRC-132 subtree data frames. The cache
key is frame-version-agnostic: `HashKey (8B) ∥ SeqNum (8B)` → raw frame bytes regardless of
frame type, so BRC-131, BRC-132, and BRC-134 frames are served on NACK request with the same
lookup path as BRC-124/BRC-128 frames.

See [bsv-multicast/docs/brc-134-anchor-transactions.md](../../../bsv-multicast/docs/brc-134-anchor-transactions.md)
for the anchor frame wire format.

**Why one worker:** Linux delivers multicast datagrams to **every** socket bound
to the group — there is no load balancing for multicast. Running multiple
workers would store each frame N times and drive N-fold cache churn. A single
worker avoids this entirely. (Declaring `SO_REUSEPORT` would instead force a
same-EUID check and defeat the `SO_REUSEADDR` cross-user share; its
load-balancing applies to unicast UDP only.)

## Cache

The frame store is the modular [`shard-common/cache`](https://github.com/lightwebinc/shard-common/blob/main/docs/cache-backend.md)
backend, selected via `-cache-backend`. Three backends are supported:

| Backend     | Storage                                              | Dedup                  | Notes                                         |
| ----------- | ---------------------------------------------------- | ---------------------- | --------------------------------------------- |
| `memory`    | In-process striped map (64 shards, 60 s TTL default) | None                   | Single-node; cache lost on restart            |
| `redis`     | Any Redis-protocol server (`bre:frame:<key>`)        | Cross-instance `SET NX`| Shared across all endpoints; survives restart |
| `aerospike` | Aerospike (`CREATE_ONLY` write)                      | Cross-instance         | Largest fleets; auto-sharded hybrid RAM/SSD   |

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
   per-group post-lookup). Throttled requests are answered with THROTTLED when
   `-rl-throttle-response` is set (sequence/chain/group tiers; the IP flood
   tier never answers) and dropped silently otherwise.
3. Looks up the frame in the cache by `HashKey ∥ StartSeq` (16-byte key).
4. On **hit**: dispatches `retransmit.Send`, then sends a 16-byte ACK (unless
   `-suppress-ack`) whose Flags record what was dispatched (`0x01` multicast,
   `0x02` unicast).
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
   - V5 (BRC-132): retransmits to `GroupSubtreeDataAnnounce` (`FF0X::B:FFFB`)
   - V6 (BRC-134 anchor): retransmits to `GroupBlockBroadcast` (`FF0X::B:FFFE`)
2. Sends the raw frame bytes verbatim to the derived group address on each
   egress interface.

Listeners that receive the retransmitted multicast frame call
`nack.Tracker.Observe`; if the incoming `SeqNum` matches a pending gap entry the
gap is auto-closed inline, before the next sweeper tick.

### Unicast retransmit

When unicast retransmit is enabled, the NACK server sends the raw frame directly
back to the requester via `retransmit.RetransmitUnicast` on a dedicated UDP
socket source-bound to the advertised NACK address (so the requester's registry
filter matches). It uses the source address from the incoming datagram. This
guarantees delivery to the specific requester without relying on multicast
fabric propagation, but does not benefit other listeners with the same gap.
Unlike the multicast path it applies **no** cross-instance dedup — each
requester needs its own copy.

A NACK carrying `FlagProxied` (a cross-domain proxy request — see
[NACK proxying](#nack-proxying-cross-domain-recovery)) is **always** served a
unicast copy, regardless of `-beacon-flags-unicast`, because the requesting
endpoint receives the frame only through this return channel.

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

Four tiers applied in order. A throttled request is dropped silently unless
`-rl-throttle-response` is set, in which case the honest-congestion tiers
(2–4) answer with a THROTTLED hint so the listener holds instead of
escalating; the per-IP flood tier (1) always stays silent. The group tier
never sends ACK on throttle — an ACK would cancel the listener gap with no
retransmit dispatched.

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

## Logging & Tracing

The endpoint uses the shared `shard-common/logging` package: `run` calls
`logging.Init` once, installing a process-wide `slog` default carrying the
`service.{name,instance.id,version}` identity triple (shared with the OTLP
metrics resource attributes). `-log-format json` emits one JSON object per line
on stdout; `-log-level` is runtime-togglable via `POST /loglevel` and SIGHUP. A
one-shot `host.inventory` event and a `bre_host_info` gauge are emitted at
startup. Tracing is opt-in (`-trace-sampling > 0` + `-otlp-endpoint`) and
covers the NACK → retransmit control-plane flow, never the cache hot path. See
the [Unified Logging Plan](https://github.com/lightwebinc/shard-common/blob/main/docs/logging.md).

## Package structure

```
retry-endpoint/
  main.go          entry point; wires config → cache → ingress → server → beacon
  config/          runtime configuration (flags + env vars + validation)
  ingress/         single-worker multicast receive loop; writes to cache
  cache/           Store adapter over the shard-common cache.Backend (memory / redis / aerospike)
  server/          UDP NACK receive pool; rate-limit → cache lookup → retransmit
  retransmit/      multicast + unicast retransmit egress; cross-instance dedup
  proxy/           cross-domain NACK proxying: recover misses from upstream, re-cache
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

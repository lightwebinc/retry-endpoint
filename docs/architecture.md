# retry-endpoint: Architecture

## Overview

`retry-endpoint` sits alongside `shard-listener` on the multicast
fabric. It joins all shard groups plus `GroupBlockBroadcast` (BRC-131 / BRC-134) and optionally
`GroupSubtreeDataAnnounce` (BRC-132), caches every frame it receives, and serves
unicast NACK requests from listeners that detect sequence gaps.

BRC wire formats live in
[bsv-multicast/docs/](https://github.com/lightwebinc/bsv-multicast/tree/main/docs). On a cache hit it
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
shard-proxy  ──UDP multicast──▶ FF05::B:<shard>:9001
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
**verbatim** (full frame with the 92-byte header intact; requires
`-strip-header=false`) into a
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

The retry-endpoint's ingress-join and retransmit-egress groups derive from
`-scope` + `-mc-group-id`, and its ADVERT beacon group from `-beacon-scope` +
`-mc-group-id`, so a downstream deployment is configured purely by aligning
those with the bridge's egress space:

set the downstream retry-endpoint's `-scope`, `-mc-group-id`, `-listen-port`,
`-egress-port` and `-shard-bits` to the bridge listener's `-mc-egress-scope`,
`-mc-egress-group-id`, `-mc-egress-port` and `-shard-bits`, and its
`-beacon-scope` to the downstream consumers' `-beacon-scope` (the bridge also
needs `-strip-header=false`). The listener-side flags are documented in
[shard-listener: Multicast Egress](https://github.com/lightwebinc/shard-listener/blob/main/docs/configuration.md#multicast-egress-domain-bridging).

Isolate the two domains, by translating the group-id (`G_d ≠ Gᵤ`) or confining
the bridge with `-mc-egress-hoplimit` / a distinct `-mc-egress-iface`, so
downstream beacons and retransmits do not leak back upstream.

**Scope:** this covers the BRC-124/BRC-128 transaction hot path, which is the
path the listener's multicast egress re-emits. Control-plane frames
(BRC-131/132/134) take the listener's unicast egress and are not bridged onto
the downstream multicast domain, so they are not recoverable downstream by
this pattern.

### Failure mode: frames the listener never emitted

A downstream retry-endpoint can only serve what it cached, and it caches the
**same** multicast emission the consumers receive. If the listener never put a
frame on the downstream wire (an `mc-egress` `sendto` failure,
`bsl_mc_egress_errors_total`, where the frame is dropped, not retried; an egress
interface flap; or in-fabric loss with no successful copy reaching the segment),
then the downstream retry-endpoint missed it identically. A downstream NACK
then returns MISS from **every** downstream endpoint and the gap is
unrecoverable within the downstream domain alone.

This is the one hole the verbatim bridge does not close: completeness of the
upstream stream (after the listener's own NACK recovery) guarantees the listener
*had* the frame, not that it *emitted* it. Mitigations, in preference order:

1. **NACK proxying (self-healing).** Enable `-proxy-enabled` with
   `-upstream-retry-endpoints` so the downstream endpoint recovers cache misses
   from an upstream endpoint and re-serves the downstream domain, with no
   per-consumer config. See [NACK proxying](#nack-proxying-cross-domain-recovery)
   below.
2. **Redundant bridge listeners.** Run ≥ 2 listeners bridging the same
   upstream→downstream over independent egress paths. A single egress failure then
   creates no domain-wide hole: another bridge emits the frame, and both
   the downstream consumers and the downstream retry-endpoint receive it. Pair
   with the listener's egress dedup (shared `-deployment-id` + Redis) so
   consumers do not see duplicates. Keeps recovery entirely inside the
   downstream domain and also covers control-plane frames (which proxying does
   not).
3. **Upstream fallback tier.** Give downstream consumers a higher-`Tier`
   registry entry (a static `-retry-endpoints` seed) pointing at an *upstream*
   retry-endpoint running `-beacon-flags-unicast=true`. On downstream-MISS
   escalation the consumer reaches back across the domain boundary; the upstream
   endpoint, which received the frame directly from the proxy, independent of
   the failed bridge, unicast-retransmits the verbatim frame. Requires IP
   reachability and that the frame is still within the upstream cache TTL.

### NACK proxying (cross-domain recovery)

When `-proxy-enabled` is set, a local cache miss triggers an **asynchronous**
recovery from an upstream retry-endpoint (the frame's true origin cache),
re-caching the frame and retransmitting it into the downstream domain. This
closes the "frames the listener never emitted" hole without per-consumer
fallback config.

```text
downstream consumer        downstream retry-EP            upstream retry-EP
        │ (1) NACK ───────────────▶│ cache miss                       │
        │◀──────── MISS ───────────│ (2) NACK (FlagProxied) ─────────▶│ cache hit
        │                          │◀──── (3) unicast frame ──────────│
        │◀═══ (4) retransmit ══════│ re-cache
        │   multicast: gap auto-fills (nack.Tracker.Observe)
```

- **Async cache-warm:** the server returns MISS immediately and enqueues a
  bounded recovery job (`-proxy-workers`, `-proxy-queue`); no NACK worker is held.
- **Delivery follows the retransmit modes:** the recovered frame is multicast
  into the downstream domain (`-beacon-flags-multicast`), sent unicast to the
  listener whose NACK missed (`-beacon-flags-unicast`), or both. With neither
  mode on it is only re-cached, and the requester's next NACK hits.
- **Unicast return channel:** the downstream endpoint is not joined to the
  upstream shard groups, so the recovered frame must come back by unicast. A NACK
  carrying `FlagProxied` is always served a unicast copy regardless of the
  upstream's retransmit mode (so the upstream does **not** strictly need
  `-beacon-flags-unicast`, though enabling it is harmless).
- **One-hop bound:** `FlagProxied` (NACK Flags bit `0x01`) stops an upstream
  endpoint from re-proxying. Such requests are served from local cache only.
- **Discovery is static:** upstreams are listed in `-upstream-retry-endpoints`
  (a separated downstream domain generally cannot receive upstream beacons).
- **Sibling dedup:** with a shared dedup backend (`-cache-backend`
  `redis`/`aerospike`, or `-redis-addr` alongside `memory`) an in-flight SETNX
  claim (`bre:proxy:` prefix, `-proxy-dedup-window` TTL) ensures only one
  downstream endpoint recovers a given gap (one per requester when unicast
  retransmit is on); without one no claim is made and siblings may each proxy
  (logged at startup). A claim the backend cannot answer fails open like the
  retransmit claim: the gap is recovered anyway, and the error is counted in
  `bre_dedup_errors_total{claim="proxy"}` and logged at most once per 30 s.
- **Scope:** BRC-124/128 tx frames (the bridged hot path). Control-plane frames
  are not bridged onto the downstream domain, so use redundant bridges for them.
- **Metrics:** `bre_proxy_requests_total`, `bre_proxy_recovered_total`,
  `bre_proxy_failed_total{reason}`, `bre_proxy_inflight_dedup_total`,
  `bre_proxy_queue_dropped_total`, `bre_dedup_errors_total{claim="proxy"}`.
  The endpoint also advertises ADVERT
  `HasParent` (`0x0002`) while proxying is enabled.

**Operational note:** all proxied NACKs share the downstream endpoint's source
IP, so the upstream's per-IP rate limit (`-rl-ip-rate`) may throttle a large
recovery burst; raise it or exempt proxy source IPs upstream.

## SSM (RFC 4607) mode

When `-source-mode=ssm` the retry-endpoint operates as both an SSM
emitter and an SSM consumer:

- **Beacon emit** binds `-bind-source` via `net.DialUDP(laddr=...)`
  so listeners can pre-declare this retry-endpoint in their
  `ssm-bootstrap-beacon` list. Each replica MUST use a distinct
  `bindSource` (anycast / ECMP-shared sources break PIM-SSM RPF).
- **Data-plane ingress** uses the shared `shard-common/netjoin.Join`
  helper, which branches `IPV6_JOIN_GROUP` vs
  `MCAST_JOIN_SOURCE_GROUP` by the per-group source list. The shard
  groups, the BEEF plane band and the block-broadcast group take their
  list from `-ssm-publishers-static`: block-broadcast frames leave from
  the emitting proxy (BRC-133), a publisher source (BRC-129). The
  subtree-data group takes its list from
  `-ssm-bootstrap-subtree-announce`. Lists are resolved via
  `shard-common/bootstrap.Resolver` (DNS names or IPv6 literals;
  fail-closed startup) and the joins use the set resolved at startup. A
  group with no list is joined any-source, which PIM-SSM does not route;
  with no publisher left to join (`-ssm-publishers-static` empty, or
  listing only this node's own source) the endpoint logs a warning at
  startup.
- **Addressing** uses `FF35::B:idx` (site SSM) or `FF3E::B:idx`
  (global SSM); `-scope` must be `site` or `global` under SSM.

See the [Source-Specific Multicast design](https://github.com/lightwebinc/bsv-multicast/blob/main/DESIGN.md#source-specific-multicast-ssm)
for fabric prerequisites (PIM-SSM, MLDv2, raised `mld_max_msf`).

## Ingress (multicast receive)

Each ingress worker (`-ingress-workers`, default 1) opens a UDP socket with
`SO_REUSEADDR` on the configured listen port, joins its share of the groups
(every Nth group, so the shard groups spread evenly), and writes each received
frame to the cache with the configured TTL. On Linux the socket turns
`IPV6_MULTICAST_ALL` off, so it receives only the groups it joined and no frame
is cached twice; elsewhere multicast reaches every socket bound to the port, so
the worker count stays at 1. Every ingress socket asks for the receive buffer
set by `-ingress-recv-buf-bytes` (64 MiB default), sized to hold a proxy's
whole-object burst. Each socket (the tee's too) is drained by one goroutine
that receives in batches (`recvmmsg` on Linux) into a ring of reusable buffers
and queues each datagram (`-ingress-queue-depth`) to one of
`-ingress-procs` processing goroutines, chosen by the frame's cache key, so
every write of one key keeps its arrival order. With `-ingress-gro` the
multicast sockets take coalesced runs (`UDP_GRO`): a run is one queued slot,
split into its datagrams by the processing goroutine its first datagram
selects. The order then holds within a run, not for every write of one key: a
run's later datagrams are stored by the first one's goroutine, so a second
copy of one of them (a duplicate, or the tee's) can be stored by its own key's
goroutine before or after. Both write the same bytes under the same key, so
the cache holds the frame either way. A queue that stays full for
100 ms drops the datagram as `bre_frames_dropped_total{reason="queue_full"}`;
a run dropped that way counts each of its datagrams. Group memberships are re-asserted every 30 s
for the life of the socket: a kernel membership can be lost underneath a running
socket when the forwarding state it depends on changes (a multicast RPF move
after a routing re-convergence, a forwarder restart, an interface rebuild) with
nothing reported to the process; `recvfrom` simply stops producing frames for
that source while the process looks healthy, so the periodic re-join is what
keeps a per-source counter from silently freezing. Only the first join is fatal
on error; re-assert refusals (the common "already a member" case) are logged at
debug. `SO_REUSEADDR` (not `SO_REUSEPORT`) is the
cross-EUID co-bind path: it lets this socket co-exist with a co-resident
shard-listener running under a **different** user on a collapsed node; both
receive the same multicast group.

**Why workers partition the groups:** Linux delivers a multicast datagram to
**every** socket joined to the group; there is no load balancing for multicast.
Two workers joined to the same group would store each frame twice and double
the cache churn, so each group belongs to exactly one worker, and
`IPV6_MULTICAST_ALL` is turned off so a wildcard-bound socket does not also
receive the groups its siblings (or a co-resident listener) joined. (Declaring
`SO_REUSEPORT` would instead force a same-EUID check and defeat the
`SO_REUSEADDR` cross-user share; its load-balancing applies to unicast UDP only.)

In addition to the shard groups, the ingress workers always join `GroupBlockBroadcast`
(`FF0X::B:FFFE`) to cache BRC-131 block control frames and BRC-134 anchor transaction
frames (FrameVerV6). When `-subtree-data-enabled=true`, they also join
`GroupSubtreeDataAnnounce` (`FF0X::B:FFFB`) to cache BRC-132 subtree data frames. When
`-beef-enabled=true`, they additionally join the BRC-148 BEEF plane band to cache V9 object
frames. BRC-130 fragments (FrameVerV3) are cached individually (each fragment carries its
own HashKey/SeqNum at the standard offsets), with TTLs per `OrigFrameVer`. The cache
key is frame-version-agnostic: `HashKey (8B) ∥ SeqNum (8B)` → raw frame bytes regardless of
frame type, so BRC-131, BRC-132, and BRC-134 frames are served on NACK request with the same
lookup path as BRC-124/BRC-128 frames.

See [bsv-multicast/docs/brc-134-anchor-transactions.md](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/brc-134-anchor-transactions.md)
for the anchor frame wire format.

### BRC-142 bundle (FrameVer 0x08)

A BRC-142 coalescing bundle (FrameVer `0x08`) packs several small transactions
into one datagram. The retry-endpoint caches and retransmits bundles
**opaquely**: `processBundleFrame` stores the whole datagram under the same
`HashKey ∥ SeqNum` flow key as a BRC-124 frame (using the tx TTL, `-cache-ttl-tx`)
and re-emits it verbatim on NACK. The **members are never parsed** on the hot
path (only the 66-byte header is read): the bundle is a single opaque cache
entry keyed by its flow, so recovery,
dedup, and rate limiting all operate on the whole bundle, not its members.

The one bundle-specific step is the retransmit group: a bundle has no single
TxID, so its shard group index is read from the header at **offset 56:58**
(`binary.BigEndian.Uint16(raw[56:58])`) rather than derived from a TxID. The
same offset feeds the tier-4 group rate limiter for bundles.

### Tee ingest (loopback frame mirror)

`-tee-listen` opens a second, optional ingest: an **exclusively bound** IPv6
loopback UDP socket fed by co-resident processes mirroring frames into the
cache. Two datagram forms are accepted, branched on the leading 4 bytes:

- **Raw frames** (fabric magic `0xE3E1F3E8`), the proxy's `-retry-tee` form:
  frames this node originates, mirrored verbatim. The per-source cache counter
  is labeled with the loopback datagram source, exactly as the wildcard data
  socket would have labeled it.
- **`teewire` envelopes** (magic `TEE1`), the listener's `-retry-tee` form:
  frames this node *received* from the fabric, prefixed with a 24-byte header
  carrying the original datagram source (see `shard-common/teewire`). The
  envelope is unwrapped and the payload processed through the normal
  `processFrame` path with that original source, so per-source attribution
  (the `RetryCacheSourceStarved` alerting contract) is identical to native
  multicast receive. Malformed envelopes are dropped and counted
  (`bre_frames_dropped_total{reason="tee_decap_error"}`).

The tee socket is **loopback-only, fail-closed at config validation**: the
envelope asserts a source address, so accepting it off-node would allow remote
attribution forgery. It deliberately does NOT target the shared wildcard data
port: with the listener co-bound there, kernel unicast demux delivers each
datagram to only ONE of the bound sockets, unpredictably: a tee aimed at
`:9001` can be swallowed by the listener (or loop a listener's own mirror back
into itself). The dedicated port makes delivery deterministic and single-owner.

`-tee-ring` is a third ingest: a shared-memory ring (`shard-common/teering`, a
file on `/dev/shm`) into which a co-resident producer writes its mirrored
datagrams, with no loopback socket on the path. The endpoint attaches to the
file (it waits for a missing file, follows an in-place reset and re-attaches
when the file is replaced), copies each datagram out, and processes it as a
raw frame labeled `::1`, on processing goroutines of its own sized like a
socket's. A `teewire` envelope read from the ring is dropped
(`bre_frames_dropped_total{reason="ring_encap"}`): the file is written by
whoever can open it, so it must not assert a frame source. With the in-memory
cache a datagram is copied once, out of the ring into memory the cache keeps.
See [`-tee-ring`](configuration.md#-tee-ring--tee_ring-default-empty--disabled).

With both the multicast join and a tee active, a frame received natively may be
stored twice, an idempotent overwrite of identical bytes at the same
`HashKey ∥ SeqNum` key (the TTL refreshes). With `-mc-join-enabled=false` the
endpoint runs **tee-only**: no join, no wildcard bind, and the cache's coverage
is exactly what the co-resident proxy (own-origin) and listener (remote-origin)
mirror in. The death of a tee feed, socket or ring, is process-fatal, like the
multicast worker's: a silently dead tee is a cache that answers MISS forever
while looking healthy.

## Cache

The frame store is the modular [`shard-common/cache`](https://github.com/lightwebinc/shard-common/blob/main/docs/cache-backend.md)
backend, selected via `-cache-backend`. Three backends are supported:

| Backend     | Storage                                              | Dedup                  | Notes                                         |
| ----------- | ---------------------------------------------------- | ---------------------- | --------------------------------------------- |
| `memory`    | In-process striped map (64 shards, 60 s TTL default) | None                   | Single-node; cache lost on restart            |
| `redis`     | Any Redis-protocol server (`bre:frame:<key>`)        | Cross-instance `SET NX`| Shared across all endpoints; survives restart |
| `aerospike` | Aerospike (`CREATE_ONLY` write)                      | Cross-instance         | Largest fleets; auto-sharded hybrid RAM/SSD   |

With `-cache-arena-bytes` the `memory` backend keeps the frames in one block
allocated at start instead of allocating each one (see
[configuration.md](configuration.md#-cache-arena-bytes--cache_arena_bytes-default-0)).

Cache keys use a single 16-byte key:

- `HashKey (8B) ∥ SeqNum (8B)` → raw frame bytes

`HashKey` is a stable per-flow identifier (`XXH64(senderIPv6 ∥ groupIdx ∥ subtreeID)`)
stamped by the proxy. `SeqNum` is a monotonic per-flow counter. Together they
uniquely identify every frame within a flow. No secondary index is needed.

## NACK server

One goroutine reads 64-byte NACK datagrams from a single `net.PacketConn`
bound to `[nackBindAddr]:nack-port` and queues each to one of `NACK_WORKERS`
worker goroutines, which share the socket for their responses. For each
datagram a worker:

1. Validates the 64-byte NACK (BRC-126 wire format).
2. Applies four-tier rate limiting (per-IP, per-`(HashKey, SeqNum)`, per-chain pre-lookup;
   per-group post-lookup). Throttled requests are answered with THROTTLED when
   `-rl-throttle-response` is set (sequence/chain/group tiers; the IP flood
   tier never answers) and dropped silently otherwise.
3. Looks up the frame in the cache by `HashKey ∥ StartSeq` (16-byte key).
4. On **hit**: dispatches `retransmit.Send`, then sends a 16-byte ACK (unless
   `-suppress-ack`) whose Flags record what was dispatched (`0x01` multicast,
   `0x02` unicast). ACK means "retransmit dispatched": when a send was attempted
   and nothing went on the wire (the multicast copy was dedup-suppressed because
   a sibling already claimed the frame, or the unicast write failed), the
   corresponding flag stays clear, and with no flag set at all **no ACK is
   sent**, so the requester escalates instead of closing a gap nothing repaired.
   (An endpoint configured with neither send mode still answers a bare ACK: that
   is the deliberate "answer without repairing" posture.)
5. On **miss**: sends a 16-byte MISS (unless `-suppress-miss`). The listener
   escalates to the next endpoint immediately.

### NACK wire format (BRC-126), 64 bytes

The canonical field layout lives in
[BRC-126 Retransmission Protocol](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/brc-126-retransmission-protocol.md).
The server consumes `HashKey`, `StartSeq`/`EndSeq`, `SubtreeID`, and the Flags
byte: bit 0 (`0x01`) = Proxied (see
[NACK proxying](#nack-proxying-cross-domain-recovery)); bits 1–7 reserved `0x00`.
A range request (`StartSeq != EndSeq`, reserved by BRC-126) is dropped.

### ACK/MISS/THROTTLED responses, 16 bytes

The layouts live in
[BRC-126 Retransmission Protocol](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/brc-126-retransmission-protocol.md).
The server sets an ACK Flags bit only for a retransmit path that dispatched
(`0x01` multicast, `0x02` unicast; step 4 above), answers MISS with SeqNum
`0`, and puts the THROTTLED backoff bucket in the Flags low nibble (`2`
sequence tier, `3` chain, `4` group) with the requested StartSeq echoed.

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

| Mode      | Beacon flag               | Config flag                                | Behavior                                                           |
| --------- | ------------------------- | ------------------------------------------ | ------------------------------------------------------------------ |
| Multicast | `FlagMulticastRetransmit` | `-beacon-flags-multicast` (default `true`) | Frame sent to `FF05::B:<shard>:egress-port` on each egress interface |
| Unicast   | `FlagUnicastRetransmit`   | `-beacon-flags-unicast` (default `false`)  | Frame sent directly back to the NACK sender's address              |

### Multicast retransmit

`retransmit.Retransmitter` holds one egress UDP socket per configured egress
interface (set via `-egress-iface`). On a cache hit it:

1. Inspects the cached frame's version byte to determine the egress group:
   - V2 (BRC-124/BRC-128): derives the shard group from the TxID via `shard.Engine`
   - V4 (BRC-131): retransmits to `GroupBlockBroadcast` (`FF0X::B:FFFE`)
   - V5 (BRC-132): retransmits to `GroupSubtreeDataAnnounce` (`FF0X::B:FFFB`)
   - V6 (BRC-134 anchor): retransmits to `GroupBlockBroadcast` (`FF0X::B:FFFE`)
   - V8 (BRC-142 bundle): reads the shard group index from the header at offset
     56:58 (a bundle has no TxID); member count is never parsed
   - V9 (BRC-148 BEEF): derives the domain-tagged group from the TopicID at
     offset 56:88 at the BEEF plane width (plane engine required; never from
     offset 8; the ContentID is not a shard key)
   - V3 (BRC-130 fragment): routed per `OrigFrameVer`, the same group as the
     original frame class
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
The counterweight is that the multicast path's group-wide delivery is also
CHARGED as delivery to every member (including those that lost nothing), and its
arrival is not confirmed on the requester's NACK socket: a multicast-only repair
is acknowledged with a multicast-flagged ACK (`0x01`), after which the listener
can only wait for the copy on the data plane, a failed round if that copy is
lost on the same path (BRC-126 also lets a listener close the gap on such an
ACK, on trust). Unicast is therefore the recommended
posture (the flag defaults are `-beacon-flags-multicast=true` and
`-beacon-flags-unicast=false`).
Unlike the multicast path it applies **no** cross-instance dedup; each
requester needs its own copy.

A NACK carrying `FlagProxied` (a cross-domain proxy request; see
[NACK proxying](#nack-proxying-cross-domain-recovery)) is **always** served a
unicast copy, regardless of `-beacon-flags-unicast`, because the requesting
endpoint receives the frame only through this return channel.

Both modes can fire for the same NACK when both beacon flags are set.

### Cross-instance deduplication

When a shared dedup backend is in use (`-cache-backend redis` or `aerospike`,
or `memory` with `-redis-addr` set for dedup only), a `SET NX` with the
`HashKey∥SeqNum` key and a `dedup-window` TTL (default 60 s) prevents two
endpoints from both retransmitting the same frame. The first endpoint to
acquire the key wins; others skip the send (`ErrDedupSuppressed`, counted in
`bre_retransmit_dedup_total`) and leave the multicast flag clear in their ACK,
or send no ACK at all if nothing else was dispatched, so the requester never
closes a gap on the strength of a sibling's copy (see the NACK server hit path
above).

A claim the backend cannot answer (down, timed out, refused) **fails open**:
the frame is sent without a claim, as if no backend were configured, which is
also how a `memory` endpoint runs when its `-redis-addr` dedup backend is
unreachable at startup. A duplicate copy is the cost; failing closed would
leave every gap unrepaired for as long as the backend is out. Each such error
is counted in `bre_dedup_errors_total{claim="retransmit"}` and logged at most
once per 30 s, with the number of errors not logged since the last line.

## Beacon discovery (BRC-126)

The beacon sender runs as a separate goroutine and fires every `beacon-interval`
(default 60 s). It sends a 56-byte ADVERT datagram to UDP port 9300 (fixed:
the listener's `-beacon-port` default) of the configured beacon multicast group:

| `-beacon-scope` | ASM group(s)     | SSM group(s)     | Purpose                                        |
| --------------- | ---------------- | ---------------- | ---------------------------------------------- |
| `site`          | `FF05::B:FFFD`   | `FF35::B:FFFD`   | Intra-site listener discovery                  |
| `org`           | `FF08::B:FFFD`   | none             | Organization-wide discovery                    |
| `global`        | `FF0E::B:FFFD`   | `FF3E::B:FFFD`   | Inter-AS discovery via MP-BGP MVPN             |
| `both` / `all`  | all three groups | none             | Mixed deployments (three ADVERTs per interval) |

The group address is a function of the source mode as well as the scope, per
BRC-126 §Beacon Scopes and BRC-129 §Source Mode and Address Range: under SSM
the control-plane groups take the source-specific `FF3x` prefix, exactly as
the data-plane shard groups do. Which form this endpoint advertises into is
selected by `-control-group-compat`, whose default (`asm-only`) keeps the
any-source group so a listener not yet joined to the source-specific group
still hears it. See
[configuration.md](./configuration.md#-control-group-compat--control_group_compat-default-asm-only)
for the rollout order; moving a sender to the source-specific group before
every listener has joined it stops discovery silently. BRC-129 has no SSM
control group at org scope, so `org` / `both` / `all` are ASM-only.

The ADVERT carries the endpoint's NACKAddr, NACKPort, Tier, Preference, Flags,
and a stable InstanceID (CRC32c hash of `-instance`, the hostname by default). Listeners upsert endpoints
into a `discovery.Registry` sorted by **(Tier ASC, Preference DESC)**; entries not
refreshed within `3 × beacon-interval` are evicted automatically.

**Interface binding:** The beacon socket sets `IPV6_MULTICAST_IF` explicitly after
`net.DialUDP` to force datagrams out the first `-egress-iface`. Without this the
kernel may route `FF05::` (or `FF35::`) via the management interface
(lower-metric default route).

## Rate limiting

Four tiers applied in order. A throttled request is dropped silently unless
`-rl-throttle-response` is set, in which case the honest-congestion tiers
(2–4) answer with a THROTTLED hint so the listener holds instead of
escalating; the per-IP flood tier (1) always stays silent. The group tier
never sends ACK on throttle: an ACK would let a listener cancel the gap with no
retransmit dispatched.

| #   | Level                 | Algorithm      | Position    | Config flags                              |
| --- | --------------------- | -------------- | ----------- | ----------------------------------------- |
| 1   | Per source IP         | Token bucket   | Pre-lookup  | `-rl-ip-rate`, `-rl-ip-burst`             |
| 2   | Per (HashKey, SeqNum) | Sliding window | Pre-lookup  | `-rl-sequence-max`, `-rl-sequence-window` |
| 3   | Per (srcIP, HashKey)  | Sliding window | Pre-lookup  | `-rl-chain-rate`, `-rl-chain-window`      |
| 4   | Per (srcIP, groupIdx) | Token bucket   | Post-lookup | `-rl-group-rate`, `-rl-group-burst`       |

`HashKey=0` (unstamped frame) bypasses tier 3 to avoid bucketing all
unattributed gaps from the same source together.

Every tier keys its state by unauthenticated NACK fields, so each tier's map
is swept once it reaches 100 000 keys (then each time it doubles) and as new
keys arrive a window or refill period after the last sweep. A sweep drops the
keys whose state a new key would start in (a sliding window that has aged
out, a token bucket that has refilled), so no decision changes; keys still in
use stay, so a map holds about the keys seen in its last window or refill
period and can pass 100 000 while that many are live. A bucket with a rate of
`0` never refills; its source starts over after a quiet minute.

## Graceful shutdown

On `SIGINT` or `SIGTERM`:

1. If `-drain-timeout` is non-zero, `rec.SetDraining()` flips `/readyz` to `503`
   and the process sleeps for that duration while the ingress and NACK workers
   continue serving.
2. The root `context.Context` is canceled, unblocking `ingress.Run` and
   `server.Run`. The `done` channel is closed, signaling the metrics server.
3. `main` waits for all goroutines via `sync.WaitGroup`, then flushes the OTLP
   exporter before returning.

An ingress or tee worker that dies takes the same path without waiting for a
signal, and the process exits with status 1 so a supervisor restarts it: a
cache that cannot fill answers every NACK with MISS while its metrics, NACK
server and beacon all look healthy.

## Logging & Tracing

The endpoint uses the shared `shard-common/logging` package: `run` calls
`logging.Init` once, installing a process-wide `slog` default carrying the
`service.{name,instance.id,version}` identity triple (shared with the OTLP
metrics resource attributes). `-log-format json` emits one JSON object per line
on stdout; `-log-level` is runtime-togglable via `POST /loglevel` and SIGHUP. A
one-shot `host.inventory` event and a `bre_host_info` gauge are emitted at
startup. With `-trace-sampling > 0` and `-otlp-endpoint` the shared OTLP trace
exporter is installed, but the endpoint starts no spans of its own. See
the [Unified Component Logging](https://github.com/lightwebinc/shard-common/blob/main/docs/logging.md).

## Package structure

```
retry-endpoint/
  main.go          entry point; wires config → cache → ingress → server → beacon
  config/          runtime configuration (flags + env vars + validation)
  ingress/         group-partitioned multicast receive workers + loopback tee and tee ring ingest; writes to cache
  cache/           Store adapter over the shard-common cache.Backend (memory / redis / aerospike)
  server/          UDP NACK receive pool; rate-limit → cache lookup → retransmit
  retransmit/      multicast + unicast retransmit egress; cross-instance dedup
  proxy/           cross-domain NACK proxying: recover misses from upstream, re-cache
  beacon/          ADVERT beacon sender; IPV6_MULTICAST_IF binding
  ratelimit/       four-tier rate limiter (IP, sequence, chain, group)
  metrics/         OTel + Prometheus instrumentation (bre_ prefix)
```

Protocol primitives are provided by
[`github.com/lightwebinc/shard-common`](https://github.com/lightwebinc/shard-common):

```
shard-common/
  frame/     BRC-124/BRC-128/BRC-130/BRC-131/BRC-132/BRC-134/BRC-149 wire format: Decode, Encode, constants
  bundle/    BRC-142 coalescing bundle (FrameVer 0x08): header constants and errors
  shard/     txid → group index → IPv6 multicast address derivation;
             control group constants and GroupAddr; BRC-148 plane engine
  cache/     modular frame store backend (memory, arena, redis, aerospike)
  netjoin/   ASM / SSM group joins
  bootstrap/ SSM source-list resolver
  teewire/   loopback tee envelope
  teering/   shared-memory tee ring
```

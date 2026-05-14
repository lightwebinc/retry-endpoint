# bitcoin-retry-endpoint — Configuration Reference

All parameters are accepted as CLI flags. Environment variables serve as
fallbacks; hard-coded defaults apply when neither is present.

---

## Network

### `-mc-iface` / `MC_IFACE` (default: `eth0`)

Network interface for multicast group joins (ingress receive). Must be the
interface the multicast fabric is reachable on.

### `-listen-port` / `LISTEN_PORT` (default: `9001`)

UDP port for multicast frame receive. Must match the proxy's `-egress-port`.

### `-scope` / `MC_SCOPE` (default: `site`)

Multicast scope nibble. Must match the proxy's and listeners' `-scope`.

| Value | Prefix | Reach |
|----------|--------|-----------------------------------------------------|
| `link` | `FF02` | Same L2 segment only |
| `site` | `FF05` | Site-local; crosses routers within a site (default) |
| `org` | `FF08` | Organisation-wide |
| `global` | `FF0E` | Internet-wide |

### `-mc-group-id` / `MC_GROUP_ID` (default: `0x000B`)

IANA group-id occupying bytes 12–13 of every IPv6 multicast address.
Default `0x000B` corresponds to the IANA-assigned Bitcoin allocation
`FF0X::B`. Must match the proxy's `-mc-group-id`.

---

## Sharding

### `-shard-bits` / `SHARD_BITS` (default: `8`)

Txid prefix bit width used as the shard key. Must exactly match the proxy's
`-shard-bits`. Determines how many multicast groups the endpoint joins (2ᴺ).

| Bits | Groups |
|------|--------|
| 1 | 2 |
| 8 | 256 |
| 12 | 4 096 |
| 15 | 32 768 (max; top of 16-bit space reserved for control) |

---

## Cache

### `-cache-backend` / `CACHE_BACKEND` (default: `memory`)

Cache storage backend. Valid values: `memory`, `redis`.

| Value | Storage | Cross-instance dedup | Notes |
|--------|---------|---------------------|-------|
| `memory` | In-process freecache | None | Single-node; lost on restart |
| `redis` | External Redis | SET NX per frame | Shared; survives restart |

### `-redis-addr` / `REDIS_ADDR` (default: empty)

Redis server address. Behaviour depends on `-cache-backend`:

| `-cache-backend` | `REDIS_ADDR` set | Behaviour |
|------------------|-----------------|----------|
| `memory` | no | freecache only; no cross-instance dedup |
| `memory` | yes | freecache for frames; Redis used **only** for `SET NX` dedup |
| `redis` | yes (required) | Redis for both frame storage and dedup |

When `REDIS_ADDR` is empty and `CACHE_BACKEND=redis`, startup fails with an
explicit error. When `CACHE_BACKEND=memory` and `REDIS_ADDR` is set, the frame
cache stays per-instance (scenario isolation is preserved) while retransmit
deduplication becomes cross-instance.

### `-cache-ttl` / `CACHE_TTL` (default: `60s`)

TTL for cached frames. Frames expire from the cache after this duration.
Listeners' `-nack-gap-ttl` should be shorter than this value.

### `-cache-max-keys` / `CACHE_MAX_KEYS` (default: `0`)

Maximum number of keys in the in-memory cache (0 = no limit). Ignored when
`-cache-backend redis`. When the limit is reached, least-recently-used entries
are evicted.

---

## NACK Server

### `-nack-port` / `NACK_PORT` (default: `9300`)

UDP port to receive 24-byte NACK datagrams from listeners. Also the port
advertised in the ADVERT beacon (listeners send NACKs here).

### `-nack-workers` / `NACK_WORKERS` (default: `runtime.NumCPU()`)

Number of NACK worker goroutines sharing the NACK socket. Workers call the
cache lookup and retransmit pipeline in parallel. Rate limiting is applied
before any cache work.

### `-nack-addr` / `NACK_ADDR` (default: auto-detected)

Explicit IPv6 unicast address to bind the NACK socket to and advertise in the
ADVERT beacon. If empty, the first non-link-local global-unicast IPv6 address
on the first `-egress-iface` is used.

> **Multi-homed hosts:** On a host with both a management NIC and a fabric NIC,
> the fabric NIC will typically have both a static address (e.g. `fd20::24`)
> and a SLAAC-derived address (e.g. `fd20::216:3eff:fe4c:8a01`). If the NACK
> socket is bound to `[::]`, the kernel may choose the SLAAC address as the
> source of outgoing ACK/MISS responses. Listeners filtering by the advertised
> address will then silently drop the responses.
>
> Set `-nack-addr` to the static fabric address to prevent this.

### `-suppress-ack` / `SUPPRESS_ACK` (default: `false`)

Do not send 16-byte ACK responses after a successful cache hit and retransmit.
Listeners fall back to timeout + exponential backoff on missing ACK. Useful for
high-volume testing or when ACK overhead is undesirable.

### `-suppress-miss` / `SUPPRESS_MISS` (default: `false`)

Do not send 16-byte MISS responses on cache miss. Listeners will wait for the
full response timeout before escalating to the next endpoint.

---

## Retransmit

### `-egress-iface` / `EGRESS_IFACE` (default: `eth0`)

Comma-separated NIC names for multicast retransmit egress. The first listed
interface is also used for beacon sending and NACK address auto-detection.
Multiple interfaces send the same retransmitted frame to each interface in order.

### `-egress-port` / `EGRESS_PORT` (default: `9001`)

UDP destination port for retransmitted frames. Must match the listeners'
`-listen-port`.

### `-dedup-window` / `DEDUP_WINDOW` (default: `60s`)

Cross-instance retransmission deduplication window. When `-cache-backend redis`,
the first endpoint to serve a NACK claims the frame with a Redis `SET NX` for
this duration. Other endpoints with the same request skip their send.

Set to match or exceed `-cache-ttl` to prevent double-retransmit on cache miss.

---

## Rate Limiting

Four tiers applied in order. Tiers 1–3 are pre-lookup; drops are silent (no
response sent). Tier 4 is post-lookup: the retransmit is skipped but an ACK is
still sent so the listener does not escalate to the next endpoint.

### `-rl-ip-rate` / `RL_IP_RATE` (default: `100`)

Token replenishment rate for the per-source-IP token bucket (tokens per second).

### `-rl-ip-burst` / `RL_IP_BURST` (default: `10`)

Burst size for the per-source-IP token bucket. Allows short bursts above the
sustained rate before limiting kicks in.

### `-rl-sequence-max` / `RL_SEQUENCE_MAX` (default: `100`)

Maximum number of requests for the same `LookupSeq` value within
`-rl-sequence-window`. Prevents a single stuck listener from flooding the
server with repeated NACKs for the same gap.

### `-rl-sequence-window` / `RL_SEQUENCE_WINDOW` (default: `1m`)

Sliding window duration for the per-`LookupSeq` counter.

### `-rl-chain-rate` / `RL_CHAIN_RATE` (default: `500`)

Maximum NACK requests allowed within `-rl-chain-window` for a given
`(srcIP, ChainID)` pair. `ChainID` is the initial `CurSeq` of the hash-chain
carried in the NACK datagram (offset 16). A `ChainID` of `0` (orphan/unattributed
gap) bypasses this tier entirely.

### `-rl-chain-window` / `RL_CHAIN_WINDOW` (default: `1m`)

Sliding window duration for the per-`(srcIP, ChainID)` counter.

### `-rl-sender-rate` / `RL_SENDER_RATE`, `-rl-sender-window` / `RL_SENDER_WINDOW`

Backward-compatible aliases for `-rl-chain-rate` / `-rl-chain-window`. If the
alias is set and the canonical flag is not, the alias value takes precedence.
New deployments should use the canonical names.

### `-rl-group-rate` / `RL_GROUP_RATE` (default: `200`)

Token replenishment rate for the per-`(srcIP, groupIdx)` retransmit limiter
(tokens per second). Applied **post-lookup** on cache hits only. When the bucket
is exhausted the retransmit is suppressed but an ACK is still returned.

### `-rl-group-burst` / `RL_GROUP_BURST` (default: `50`)

Burst size for the per-`(srcIP, groupIdx)` token bucket. `groupIdx` is derived
from the frame TxID using the same `shard.Engine` as the multicast egress path
(`-shard-bits` must match).

---

## Beacon

### `-beacon-enabled` / `BEACON_ENABLED` (default: `true`)

Periodically multicast a 56-byte ADVERT datagram to the beacon group so
listeners can discover this endpoint dynamically. See [BRC-126 — Retransmission
Protocol](https://github.com/lightwebinc/bitcoin-multicast/blob/main/docs/brc-126-retransmission-protocol.md).

When `false`, the endpoint is reachable only via static `-retry-endpoints` seeds
on listeners.

### `-beacon-tier` / `BEACON_TIER` (default: `0`)

Tier level advertised in the ADVERT. Listeners sort endpoints by
**(Tier ASC, Preference DESC)**; lower tier = higher priority. Use `0` for
endpoints closest to the source (same site) and higher values for remotely
reached fallbacks.

### `-beacon-preference` / `BEACON_PREFERENCE` (default: `128`)

Preference weight within a tier (0–255). Higher = more preferred. Endpoints at
the same tier are tried in descending preference order.

### `-beacon-interval` / `BEACON_INTERVAL` (default: `60s`)

ADVERT multicast cadence. Must be ≥ 1s (the wire format carries an integer
seconds field). Listeners evict endpoints that have not sent an ADVERT within
`3 × beacon-interval`.

### `-beacon-scope` / `BEACON_SCOPE` (default: `site`)

Multicast scope for ADVERT datagrams.

| Value | Group | Use case |
|--------|-------|----------|
| `site` | `FF05::B:FFFD` | All listeners on the local site |
| `global` | `FF0E::B:FFFD` | Inter-AS via MP-BGP MVPN |
| `both` | both groups | Site + global simultaneously (two ADVERTs per interval) |

Org scope (`FF08::B:FFFD`, wire byte `0x08`) is defined in the BRC-126 wire format but `org` is not a supported flag value.

### `-beacon-flags-multicast` / `BEACON_FLAGS_MULTICAST` (default: `true`)

Advertise that this endpoint retransmits via multicast. Listeners use this flag
to decide whether the endpoint's retransmits will arrive on the multicast fabric
(and thus be auto-closed by `Tracker.Observe`) or only via unicast.

### `-beacon-flags-unicast` / `BEACON_FLAGS_UNICAST` (default: `false`)

Advertise unicast retransmit support. When enabled, the NACK server sends the
raw frame directly back to the requesting listener via the source address of the
incoming NACK datagram. This guarantees delivery to the specific listener without
relying on multicast fabric propagation. Can be enabled alongside multicast
retransmit — both fire for the same NACK when both flags are set.

### `-beacon-flags-draining` / `BEACON_FLAGS_DRAINING` (default: `false`)

Advertise draining status. Listeners that respect this flag will not add or
retain this endpoint in their registry. Useful for graceful removal from the
pool without waiting for beacon eviction timeout.

---

## Runtime

### `-debug` / `DEBUG` (default: `false`)

Enable per-packet debug logging (decoded NACK fields, cache lookup result,
retransmit decisions, rate limit drops).

### `-drain-timeout` / `DRAIN_TIMEOUT` (default: `0s`)

Pre-shutdown drain window. When non-zero, `/readyz` returns `503` immediately
on signal receipt while ingress and NACK workers continue processing for this
duration. Useful for rolling restarts behind a load balancer.

---

## Observability

### `-metrics-addr` / `METRICS_ADDR` (default: `:9400`)

HTTP bind address for:
- `GET /metrics` — Prometheus scrape endpoint (`bre_` prefix)
- `GET /healthz` — always `200 OK` while the process is running
- `GET /readyz` — `200` when ready; `503` while starting or draining

### `-instance` / `INSTANCE_ID` (default: hostname)

OTel `service.instance.id` resource attribute. Identifies individual endpoint
instances in federated Prometheus / OTLP deployments.

### `-otlp-endpoint` / `OTLP_ENDPOINT`

gRPC OTLP endpoint for metric push (e.g. `otel-collector:4317`). Empty disables
push export; Prometheus scraping always works regardless.

### `-otlp-interval` / `OTLP_INTERVAL` (default: `30s`)

Metric export interval for the OTLP push exporter. Ignored when
`OTLP_ENDPOINT` is empty.

---

## Key metrics

| Metric | Description |
|--------|-------------|
| `bre_frames_received_total` | Frames received from multicast fabric |
| `bre_frames_cached_total` | Frames successfully written to cache |
| `bre_cache_hits_total` | NACK requests resolved from cache |
| `bre_cache_misses_total` | NACK requests with no cached frame |
| `bre_retransmits_total` | Frames sent to multicast egress |
| `bre_retransmit_dedup_total` | Retransmits skipped by cross-instance dedup (requires `REDIS_ADDR`) |
| `bre_rate_limit_drops_total{level=ip\|chain\|sequence\|group}` | Requests dropped (or retransmit suppressed) by rate limiter tier |

---

## Example: in-memory cache, single NIC

```bash
bitcoin-retry-endpoint \
  -mc-iface eth0 \
  -egress-iface eth0 \
  -shard-bits 16 \
  -cache-backend memory \
  -cache-ttl 60s
```

## Example: memory cache + Redis dedup, multi-homed host

Frame cache stays per-instance (safe for scenario 13-style tests). Redis used
only for `SET NX` retransmit deduplication across retry endpoints.

```bash
bitcoin-retry-endpoint \
  -mc-iface enp6s0 \
  -egress-iface enp6s0 \
  -shard-bits 2 \
  -cache-backend memory \
  -redis-addr 10.10.10.40:6379 \
  -nack-addr fd20::24 \
  -beacon-tier 0 \
  -beacon-preference 128 \
  -metrics-addr :9400
```

## Example: Redis cache, multi-homed host

```bash
bitcoin-retry-endpoint \
  -mc-iface enp6s0 \
  -egress-iface enp6s0 \
  -shard-bits 16 \
  -cache-backend redis \
  -redis-addr redis.local:6379 \
  -nack-addr fd20::24 \
  -beacon-tier 0 \
  -beacon-preference 128 \
  -metrics-addr :9400
```

## Example: tier-1 fallback endpoint (global beacon scope)

```bash
bitcoin-retry-endpoint \
  -mc-iface eth0 \
  -egress-iface eth0 \
  -shard-bits 16 \
  -beacon-tier 1 \
  -beacon-preference 128 \
  -beacon-scope global \
  -cache-backend redis \
  -redis-addr redis.local:6379
```

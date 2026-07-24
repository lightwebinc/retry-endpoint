# retry-endpoint — Configuration Reference

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

| Value    | Prefix | Reach                                               |
| -------- | ------ | --------------------------------------------------- |
| `link`   | `FF02` | Same L2 segment only                                |
| `site`   | `FF05` | Site-local; crosses routers within a site (default) |
| `org`    | `FF08` | Organisation-wide                                   |
| `global` | `FF0E` | Internet-wide                                       |

### `-mc-group-id` / `MC_GROUP_ID` (default: `0x000B`)

IANA group-id occupying bytes 12–13 of every IPv6 multicast address.
Default `0x000B` corresponds to the IANA-assigned Bitcoin allocation
`FF0X::B`. Must match the proxy's `-mc-group-id`.

---

## SSM (RFC 4607)

See the [SSM Support Plan](https://github.com/lightwebinc/bsv-multicast/blob/main/DESIGN.md#source-specific-multicast-ssm).
The retry-endpoint plays two roles under SSM: it **emits** NACK-discovery
beacons (so listeners need to know `bindSource` to pre-declare it in
their `ssm-bootstrap-beacon`), and it **consumes** the data plane
(so it needs its own bootstrap source lists for the multicast groups
it joins). ASM is the default.

### `-source-mode` / `SOURCE_MODE` (default: `asm`)

Addressing model. `ssm` derives the prefix via `shard.Prefix(SSM,
scope)` → FF35 site / FF3E global; rejects ASM at global per RFC 8815.

### `-bind-source` / `BIND_SOURCE` (default: `""`)

IPv6 literal bound on the beacon emit socket via `net.DialUDP(laddr=...)`.
SSM listeners list this address in their `-ssm-bootstrap-beacon`.
Each retry-endpoint replica MUST use a distinct `bindSource`.

It also marks this endpoint's **own** source: that source is excluded from the
cache's `(S,G)` join roster (`excludeOwnSource`) to avoid an endpoint joining its
own feed. **Leave empty to cache the full roster including own** — required where
the endpoint must repair its own source (e.g. the collapsed PIM-SSM fabric, where
the source node's cache is the last-resort repairer for that source's frames).

### `-ssm-bootstrap-manifest` / `SSM_BOOTSTRAP_MANIFEST` (default: `""`)

CSV of shard-manifest source IPs (literals or DNS names). Used for the
`(S,G)` join of the manifest / block-broadcast group.

### `-ssm-bootstrap-beacon` / `SSM_BOOTSTRAP_BEACON` (default: `""`)

CSV of retry-endpoint source IPs. Used for the `(S,G)` join of the
beacon group when this retry-endpoint also consumes beacons (e.g. peer
discovery in multi-retry deployments).

### `-ssm-bootstrap-subtree-announce` / `SSM_BOOTSTRAP_SUBTREE_ANNOUNCE` (default: `""`)

CSV of subtree-announce emitter source IPs for that control group.

### `-ssm-publishers-static` / `SSM_PUBLISHERS_STATIC` (default: `""`)

Lab/CI: pre-declared data-plane publisher source list. Production uses
manifest-driven discovery. Fail-closed validation rejects > 16 entries
without a manifest bootstrap.

### `-ssm-bootstrap-refresh` / `SSM_BOOTSTRAP_REFRESH` (default: `30s`)

DNS re-resolve interval; last-good set is retained on transient
refresh failures.

---

## Sharding

### `-shard-bits` / `SHARD_BITS` (default: `2`)

Txid prefix bit width used as the shard key. Must exactly match the proxy's
`-shard-bits`. Determines how many multicast groups the endpoint joins (2ᴺ).

| Bits | Groups                                                 |
| ---- | ------------------------------------------------------ |
| 1    | 2                                                      |
| 2    | 4 (default)                                            |
| 8    | 256                                                    |
| 12   | 4 096                                                  |
| 15   | 32 768 (max; top of 16-bit space reserved for control) |

### `-subtree-data-enabled` / `SUBTREE_DATA_ENABLED` (default: `false`)

Enable BRC-132 subtree data caching. When `true`, the ingress worker joins
`GroupSubtreeDataAnnounce` (`FF0X::B:FFFB`) in addition to all shard groups and
`GroupBlockBroadcast`. BRC-132 frames received on this group are cached with the standard
`HashKey ∥ SeqNum` key and served on NACK request. The retransmitter routes BRC-132 frames
back to `GroupSubtreeDataAnnounce` on cache hit.

Set this flag when any downstream `shard-listener` has `-subtree-data-enabled=true`
and relies on NACK-based retransmission for subtree data fragments.

---

## Cache

The frame cache uses the modular `shard-common/cache` backend. See
[`shard-common/docs/cache-backend.md`](https://github.com/lightwebinc/shard-common/blob/main/docs/cache-backend.md)
for the interface and backend matrix.

### `-cache-backend` / `CACHE_BACKEND` (default: `memory`)

Cache storage backend. Valid values: `memory`, `redis`, `aerospike`.

| Value       | Storage                    | Cross-instance | Notes                                            |
| ----------- | -------------------------- | -------------- | ------------------------------------------------ |
| `memory`    | In-process striped map     | No             | Single-node; lost on restart                     |
| `redis`     | Redis/Valkey/Dragonfly     | Yes            | `SET NX EX`; shared frames + dedup               |
| `aerospike` | Aerospike Community Edition | Yes           | `CREATE_ONLY`; auto-sharded; **TTL floor 1s**    |

### `-redis-addr` / `REDIS_ADDR` (default: empty)

Redis-protocol address (Redis/Valkey/Dragonfly). Behaviour depends on `-cache-backend`:

| `-cache-backend` | `REDIS_ADDR` set | Behaviour                                                      |
| ---------------- | ---------------- | ------------------------------------------------------------- |
| `memory`         | no               | per-instance frames; no cross-instance dedup                  |
| `memory`         | yes              | per-instance frames; Redis used **only** for `SET NX` dedup   |
| `redis`          | yes (required)   | Redis for both frame storage and dedup                        |
| `aerospike`      | n/a              | Aerospike for both frame storage and dedup                    |

When the backend is `redis` (resp. `aerospike`) but `-redis-addr` (resp.
`-aerospike-hosts`) is empty, startup fails with an explicit error. When
`CACHE_BACKEND=memory` and `REDIS_ADDR` is set, the frame cache stays
per-instance while retransmit deduplication becomes cross-instance.

### `-aerospike-hosts` / `AEROSPIKE_HOSTS` (default: empty)

Comma-separated Aerospike seed nodes (`host:port`, default port 3000). Required
when `-cache-backend=aerospike`. The namespace and set are selected by
`-aerospike-namespace` (default `cache`) and `-aerospike-set` (default `bre`);
the namespace must be provisioned on the cluster. Aerospike expiration is in
whole seconds with a 1s floor — all frame TTLs here satisfy that.

### `-cache-dial-timeout` / `CACHE_DIAL_TIMEOUT` (default: `1s`) · `-cache-op-timeout` / `CACHE_OP_TIMEOUT` (default: `1s`)

Backend connection and per-operation timeouts. Operations fail open (treated as
a cache miss) on timeout so a slow backend never blocks NACK handling.

### `-cache-ttl` / `CACHE_TTL` (default: `60s`)

Global fallback TTL for cached frames. Acts as the default for any
per-FrameVer TTL that is not set explicitly. If you set `CACHE_TTL=30s`
and leave the per-type knobs untouched, all frame types collapse to 30s.
Listeners' `-nack-gap-ttl` should be shorter than the smallest effective
TTL.

### Per-FrameVer TTLs

Differentiated TTLs per frame type. Each one defaults to a value tuned
for the typical retransmit window for its frame family. When unset,
they fall back to `CACHE_TTL` (if explicitly set), otherwise to the
listed default.

| Flag / env | FrameVer | BRC | Default |
|---|---|---|---|
| `-cache-ttl-tx` / `CACHE_TTL_TX` | `0x02` | BRC-124 / BRC-128 regular tx | `60s` |
| `-cache-ttl-block` / `CACHE_TTL_BLOCK` | `0x04` | BRC-131 block control | `10m` |
| `-cache-ttl-subtree` / `CACHE_TTL_SUBTREE` | `0x05` | BRC-132 subtree data | `5m` |
| `-cache-ttl-anchor` / `CACHE_TTL_ANCHOR` | `0x06` | BRC-134 anchor tx | `2m` |
| _(uses `-cache-ttl-tx`)_ | `0x08` | BRC-142 coalescing bundle | `60s` |

BRC-142 bundles (FrameVer `0x08`) have no dedicated TTL flag: they are cached
opaquely on the tx hot path and reuse `-cache-ttl-tx` (`CACHE_TTL_TX`).

Resolution order applied per frame type:

1. explicit per-FrameVer flag/env (e.g. `CACHE_TTL_BLOCK=15m`) — wins
2. else, explicit `CACHE_TTL` — overrides the differentiated default
3. else, the differentiated default above

All four values must be strictly positive; the process exits at startup
if any resolves to zero or a negative duration.

### `-cache-max-keys` / `CACHE_MAX_KEYS` (default: `0`)

Maximum number of keys in the in-memory cache (0 = no limit). Ignored when
`-cache-backend redis`. When the limit is reached, least-recently-used entries
are evicted.

---

## NACK Server

### `-nack-port` / `NACK_PORT` (default: `9300`)

UDP port to receive 64-byte NACK datagrams from listeners. Also the port
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
> the fabric NIC will typically have both a static address (e.g. `2001:db8::24`)
> and a SLAAC-derived address (e.g. `2001:db8::216:3eff:fe00:1`). If the NACK
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

### `-egress-multicast-loop` / `EGRESS_MULTICAST_LOOP` (default: `false`)

Set `IPV6_MULTICAST_LOOP` on the retransmit egress socket(s). **Required on a
collapsed / mesh router node** where retransmit egress rides a dummy `mc-local`
interface: the kernel only submits locally-originated multicast to the multicast
forwarding cache (MFC) — for fan-out onto the outbound tunnels — when loopback is
enabled. Mirrors shard-proxy's `EGRESS_MULTICAST_LOOP`. Leave off on a normal
node with a real fabric NIC.

### `-dedup-window` / `DEDUP_WINDOW` (default: `60s`)

Cross-instance retransmission deduplication window. When `-cache-backend redis`,
the first endpoint to serve a NACK claims the frame with a Redis `SET NX` for
this duration. Other endpoints with the same request skip their send.

Set to match or exceed `-cache-ttl` to prevent double-retransmit on cache miss.

---

## NACK Proxying (cross-domain recovery)

When enabled, a local cache miss triggers an asynchronous recovery from an
upstream retry-endpoint, re-caching the frame and multicast-retransmitting it
into this endpoint's domain. This closes the "frames the bridging listener never
emitted" hole for a downstream domain. See the architecture doc's
[NACK proxying](architecture.md#nack-proxying-cross-domain-recovery) section.

### `-proxy-enabled` / `PROXY_ENABLED` (default: `false`)

Master switch. When `true`, `-upstream-retry-endpoints` is required.

### `-upstream-retry-endpoints` / `UPSTREAM_RETRY_ENDPOINTS` (default: empty)

Comma-separated `host:port` list of upstream retry-endpoint NACK addresses to
recover misses from. Tried in order until one returns the frame. These are the
endpoints on the upstream fabric that cached the frame directly from the proxy.

### `-proxy-timeout` / `PROXY_TIMEOUT` (default: `300ms`)

Per-upstream wait for the unicast frame return before trying the next upstream.

### `-proxy-max-endpoints` / `PROXY_MAX_ENDPOINTS` (default: `0`)

Maximum upstream endpoints tried per gap. `0` = all configured upstreams.

### `-proxy-dedup-window` / `PROXY_DEDUP_WINDOW` (default: `60s`)

TTL of the in-flight SETNX claim (`bre:proxy:` prefix) that prevents sibling
downstream endpoints from all proxying the same gap. Effective only with a
shared cache backend (`redis`/`aerospike`); with `memory` the claim is
per-process and a startup warning is logged.

### `-proxy-workers` / `PROXY_WORKERS` (default: `4`)

Recovery worker goroutines draining the job queue.

### `-proxy-queue` / `PROXY_QUEUE` (default: `1024`)

Recovery job queue depth. Jobs are dropped (counted `bre_proxy_queue_dropped_total`)
when full, since the requester already received MISS and will retry/escalate.

> **Upstream rate limits:** all proxied NACKs share this endpoint's source IP.
> The upstream's per-IP limiter (`-rl-ip-rate`) may throttle a large recovery
> burst — raise it or exempt proxy source IPs upstream.

---

## Rate Limiting

Four tiers applied in order: per-IP, per-SeqNum, per-chain (pre-lookup), then
per-group (post-lookup, cache hits only). All four tiers drop silently by
default. With `-rl-throttle-response` the honest-congestion tiers (SeqNum,
chain, group) answer with a 16-byte THROTTLED hint instead; the per-IP flood
tier never answers (reflection guard). The group tier never sends ACK on
throttle — an ACK would cancel the listener gap with no retransmit dispatched.

### `-rl-ip-rate` / `RL_IP_RATE` (default: `100`)

Token replenishment rate for the per-source-IP token bucket (tokens per second).

### `-rl-ip-burst` / `RL_IP_BURST` (default: `10`)

Burst size for the per-source-IP token bucket. Allows short bursts above the
sustained rate before limiting kicks in.

### `-rl-sequence-max` / `RL_SEQUENCE_MAX` (default: `100`)

Maximum number of requests for the same `SeqNum` value within
`-rl-sequence-window`. Prevents a single stuck listener from flooding the
server with repeated NACKs for the same gap.

### `-rl-sequence-window` / `RL_SEQUENCE_WINDOW` (default: `1m`)

Sliding window duration for the per-`SeqNum` counter.

### `-rl-chain-rate` / `RL_CHAIN_RATE` (default: `500`)

Maximum NACK requests allowed within `-rl-chain-window` for a given
`(srcIP, HashKey)` pair. `HashKey` is the stable per-flow XXH64 identifier
carried in the NACK datagram (offset 8). A `HashKey` of `0` (unstamped frame)
bypasses this tier entirely.

### `-rl-chain-window` / `RL_CHAIN_WINDOW` (default: `1m`)

Sliding window duration for the per-`(srcIP, HashKey)` counter.

### `-rl-sender-rate` / `RL_SENDER_RATE`, `-rl-sender-window` / `RL_SENDER_WINDOW`

Backward-compatible aliases for `-rl-chain-rate` / `-rl-chain-window`. If the
alias is set and the canonical flag is not, the alias value takes precedence.

### `-rl-group-rate` / `RL_GROUP_RATE` (default: `200`)

Token replenishment rate for the per-`(srcIP, groupIdx)` retransmit limiter
(tokens per second). Applied **post-lookup** on cache hits only. When the bucket
is exhausted the retransmit is suppressed and the request is answered like the
other honest-congestion tiers: THROTTLED with `-rl-throttle-response`, silence
otherwise (never ACK — an ACK would cancel the listener gap with nothing sent).

### `-rl-group-burst` / `RL_GROUP_BURST` (default: `50`)

Burst size for the per-`(srcIP, groupIdx)` token bucket. `groupIdx` is derived
from the frame TxID using the same `shard.Engine` as the multicast egress path
(`-shard-bits` must match).

### `-rl-throttle-response` / `RL_THROTTLE_RESPONSE` (default: `false`)

When enabled, a rejection by the honest-congestion tiers (per-sequence,
per-chain, per-group) returns a 16-byte THROTTLED response (`MsgType 0x13`) carrying a
backoff-bucket hint instead of silently dropping the NACK. The listener holds
the gap for the hinted backoff (`125 ms << bucket`) and retries the **same**
endpoint without escalating or consuming its retry budget. The per-IP flood tier
**never** answers (it sheds spoofed-source load and a reply would permit
reflection). Optional load-shedding refinement for high-fan-out deployments; see
[BRC-126 — Retransmission Protocol](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/brc-126-retransmission-protocol.md).

---

## Beacon

### `-beacon-enabled` / `BEACON_ENABLED` (default: `true`)

Periodically multicast a 56-byte ADVERT datagram to the beacon group so
listeners can discover this endpoint dynamically. See [BRC-126 — Retransmission
Protocol](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/brc-126-retransmission-protocol.md).

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

| Value            | Group(s)         | Use case                                                       |
| ---------------- | ---------------- | -------------------------------------------------------------- |
| `site`           | `FF05::B:FFFD`   | All listeners on the local site                                |
| `org`            | `FF08::B:FFFD`   | Organisation-wide discovery                                    |
| `global`         | `FF0E::B:FFFD`   | Inter-AS via MP-BGP MVPN                                       |
| `both` / `all`   | all three groups | Site + org + global simultaneously (three ADVERTs per interval) |

`both` and `all` are synonyms: they set the ADVERT wire scope byte to `0xFF`
and emit one ADVERT to each of the three groups per interval.

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

**Collapsed PIM-SSM fabric** (e.g. a collapsed single-node fabric): set
`BEACON_FLAGS_UNICAST=true` **and** `BEACON_FLAGS_MULTICAST=false`. Multicast
re-injection cannot repair a *remote* receiver there — PIM-SSM RPF lets only the
source node inject into its own `(S,G)` tree — so unicast is the only working
return channel. Pair this with **no `-bind-source`** so the cache holds its own
source too (the origin becomes the last-resort repairer), and the consumer
listener must bind its NACK socket to a routable `/128` or the unicast reply
is misrouted off the tunnel.

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

### `-log-format` / `LOG_FORMAT` (default: `text`)

Structured-log output: `text` (stderr, dev default) or `json` (one JSON object
per line on stdout, for fleet aggregation). Every line carries the
`service.{name,instance.id,version}` identity triple shared with OTLP metrics.
See the [Unified Logging Plan](https://github.com/lightwebinc/shard-common/blob/main/docs/logging.md).

### `-log-level` / `LOG_LEVEL` (default: `info`)

`debug` | `info` | `warn` | `error`. Runtime-togglable via `POST /loglevel?level=<lvl>`
and SIGHUP. `-debug` is a deprecated alias for `-log-level=debug`.

### `-trace-sampling` / `TRACE_SAMPLING` (default: `0`)

Distributed-trace head sampling ratio `0.0`–`1.0`. `0` = no-op tracer (zero
cost). When `> 0` with an `-otlp-endpoint`, control-plane flows (NACK →
retransmit) are traced and exported via the collector; the cache hot path is
never traced. At startup the endpoint emits a one-shot `host.inventory` event
and a `bre_host_info` gauge.

---

## Key metrics

| Metric                                                           | Description                                                         |
| ---------------------------------------------------------------- | ------------------------------------------------------------------- |
| `bre_frames_received_total`                                      | Frames received from multicast fabric                               |
| `bre_frames_cached_total`                                        | Frames successfully written to cache                                |
| `bre_cache_hits_total`                                           | NACK requests resolved from cache                                   |
| `bre_cache_misses_total`                                         | NACK requests with no cached frame                                  |
| `bre_retransmits_total`                                          | Frames sent to multicast egress                                     |
| `bre_unicast_retransmits_total`                                  | Frames sent unicast back to the NACK requester                      |
| `bre_retransmit_dedup_total`                                     | Retransmits skipped by cross-instance dedup (requires `REDIS_ADDR`) |
| `bre_rate_limit_drops_total{level=ip\|chain\|sequence\|group}`   | Requests dropped (or retransmit suppressed) by rate limiter tier    |
| `bre_proxy_requests_total`                                       | Cross-domain proxy recovery jobs started                            |
| `bre_proxy_recovered_total`                                      | Frames recovered from upstream and re-cached                        |
| `bre_proxy_failed_total{reason}`                                 | Proxy jobs that found no frame upstream                             |
| `bre_proxy_inflight_dedup_total`                                 | Proxy jobs skipped (sibling already claimed the gap)                |
| `bre_proxy_queue_dropped_total`                                  | Proxy jobs dropped because the queue was full                       |

---

## Example: in-memory cache, single NIC

```bash
retry-endpoint \
  -mc-iface eth0 \
  -egress-iface eth0 \
  -shard-bits 2 \
  -cache-backend memory \
  -cache-ttl 60s
```

## Example: memory cache + Redis dedup, multi-homed host

Frame cache stays per-instance (safe for scenario 13-style tests). Redis used
only for `SET NX` retransmit deduplication across retry endpoints.

```bash
retry-endpoint \
  -mc-iface enp6s0 \
  -egress-iface enp6s0 \
  -shard-bits 2 \
  -cache-backend memory \
  -redis-addr redis.local:6379 \
  -nack-addr 2001:db8::24 \
  -beacon-tier 0 \
  -beacon-preference 128 \
  -metrics-addr :9400
```

## Example: Redis cache, multi-homed host

```bash
retry-endpoint \
  -mc-iface enp6s0 \
  -egress-iface enp6s0 \
  -shard-bits 2 \
  -cache-backend redis \
  -redis-addr redis.local:6379 \
  -nack-addr 2001:db8::24 \
  -beacon-tier 0 \
  -beacon-preference 128 \
  -metrics-addr :9400
```

## Example: tier-1 fallback endpoint (global beacon scope)

```bash
retry-endpoint \
  -mc-iface eth0 \
  -egress-iface eth0 \
  -shard-bits 2 \
  -beacon-tier 1 \
  -beacon-preference 128 \
  -beacon-scope global \
  -cache-backend redis \
  -redis-addr redis.local:6379
```

## Helm chart

Every flag documented in this file is exposed under `.config` in the corresponding Helm chart's `values.yaml`. See the chart repository for installation snippets and the `values.schema.json` for validation rules.

Chart: [`lightwebinc/retry-endpoint-helm`](https://github.com/lightwebinc/retry-endpoint-helm) — `config.nackAddr` is effectively required; no Redis subchart bundled.

## BRC-148 BEEF object plane

| Flag / Env | Default | Description |
|------------|---------|-------------|
| `-beef-enabled` / `BEEF_ENABLED` | `false` | Join the BEEF plane band (`0x1000 + 2^beef-shard-bits` groups) and cache V9 frames |
| `-beef-shard-bits` / `BEEF_SHARD_BITS` | `4` | Plane width; must match the proxy (retransmit groups re-derive from the TopicID at this width) |
| `-cache-ttl-beef` / `CACHE_TTL_BEEF` | `60s` | V9 cache TTL (live-tail resend window); independent of the `CACHE_TTL` collapse |

V9 frames cache under the standard HashKey ∥ SeqNum key; the retransmit
target is re-derived from the **TopicID at offset 56** (never offset 8 — the
ContentID is not a shard key). BRC-130 fragments now cache individually as
well (TTL per `OrigFrameVer`, incl. V9) and retransmit per the fragmented
class; note the listener does not yet NACK missing fragments.

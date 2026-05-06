# bitcoin-retry-endpoint

Caching endpoint for multicast NACK-based retransmission of missed Bitcoin transaction data frames.

## Overview

`bitcoin-retry-endpoint` joins IPv6 multicast groups to receive BSV transaction frames, caches them with a configurable TTL (default 60 seconds), and retransmits frames on demand via NACK requests. It operates in multicast listener mode, ensuring all retry endpoints receive all frames from all proxies for natural redundancy and cache consistency.

## Architecture

- **Ingress**: Single-worker multicast receiver (SO_REUSEPORT) joins all shard groups
- **Cache**: Modular backend supporting Redis (primary) or in-memory (fallback)
- **Server**: UDP NACK receiver (BRC-126, 24-byte) with worker pool and ACK/MISS responses
- **Beacon**: ADVERT beacon sender for dynamic endpoint discovery (BRC-126)
- **Rate Limiting**: Two-level limiting (per-IP token bucket, per-LookupSeq sliding window) with silent drops
- **Retransmit**: Sharding-based multicast egress with Redis-backed cross-instance deduplication
- **Metrics**: Prometheus + OTLP with `bre_` prefix

## Quick Start

```bash
# Build
go build -o bitcoin-retry-endpoint .

# Run with in-memory cache
./bitcoin-retry-endpoint -mc-iface eth0 -cache-backend memory

# Run with Redis cache
./bitcoin-retry-endpoint -mc-iface eth0 -cache-backend redis -redis-addr localhost:6379
```

## Configuration

All flags have environment variable equivalents (e.g., `-mc-iface` → `MC_IFACE`).

### Ingress (Multicast Receive)
- `-mc-iface` (MC_IFACE): NIC for multicast ingress (default: `eth0`)
- `-listen-port` (LISTEN_PORT): Multicast listen port (default: `9001`)
- `-shard-bits` (SHARD_BITS): Txid prefix bit width, 1–24 (default: `16`)
- `-scope` (MC_SCOPE): Multicast scope: `link | site | org | global` (default: `site`)
- `-mc-base-addr` (MC_BASE_ADDR): Base IPv6 address for assigned space (bytes 2-12)

### Cache
- `-cache-backend` (CACHE_BACKEND): `redis | memory` (default: `memory`)
- `-redis-addr` (REDIS_ADDR): Redis server address (default: `localhost:6379`)
- `-cache-ttl` (CACHE_TTL): Cache TTL (default: `60s`)
- `-cache-max-keys` (CACHE_MAX_KEYS): Maximum keys (0 = no limit)

### Server (NACK Receive)
- `-nack-port` (NACK_PORT): NACK listen port (default: `9300`)
- `-nack-workers` (NACK_WORKERS): NACK worker goroutines (default: NumCPU)

### Retransmit
- `-egress-iface` (EGRESS_IFACE): Comma-separated NICs for multicast egress (default: `eth0`)
- `-egress-port` (EGRESS_PORT): Destination UDP port for retransmitted frames (default: `9001`)
- `-dedup-window` (DEDUP_WINDOW): Retransmission deduplication window (default: `60s`)

### Rate Limiting
- `-rl-ip-rate` (RL_IP_RATE): IP rate limit (tokens/sec, default: `100`)
- `-rl-ip-burst` (RL_IP_BURST): IP burst size (default: `10`)
- `-rl-sequence-max` (RL_SEQUENCE_MAX): Max requests per LookupSeq per window (default: `100`)
- `-rl-sequence-window` (RL_SEQUENCE_WINDOW): LookupSeq sliding window (default: `1m`)
- `-rl-sender-rate`, `-rl-sender-window` (RL_SENDER_RATE, RL_SENDER_WINDOW): **Deprecated no-ops.** SenderID was removed from the NACK wire format in BRC-124.

### Observability
- `-metrics-addr` (METRICS_ADDR): HTTP bind for `/metrics`, `/healthz`, `/readyz` (default: `:9400`)
- `-instance` (INSTANCE_ID): OTel service.instance.id (default: hostname)
- `-otlp-endpoint` (OTLP_ENDPOINT): OTLP gRPC endpoint (empty = disabled)
- `-otlp-interval` (OTLP_INTERVAL): OTLP push interval (default: `30s`)

### Beacon (BRC-126 Endpoint Discovery)
- `-beacon-enabled` (BEACON_ENABLED): Enable ADVERT beacon multicasting (default: `true`)
- `-beacon-tier` (BEACON_TIER): Tier level, 0 = closest to source (default: `0`)
- `-beacon-preference` (BEACON_PREFERENCE): Weight within tier, higher = preferred (default: `128`)
- `-beacon-interval` (BEACON_INTERVAL): Beacon multicast interval (default: `60s`)
- `-beacon-scope` (BEACON_SCOPE): `site | global | both` (default: `site`)
- `-suppress-ack` (SUPPRESS_ACK): Disable ACK responses (default: `false`)
- `-suppress-miss` (SUPPRESS_MISS): Disable MISS responses (default: `false`)

### Runtime
- `-debug` (DEBUG): Enable per-packet debug logging (default: `false`)
- `-drain-timeout` (DRAIN_TIMEOUT): Pre-drain delay before closing sockets (default: `0s`)

## Security

- **No batch retrieval**: NACK format enforces single-frame requests
- **Retransmit deduplication**: Cross-instance coordination via Redis SET NX (60s window) prevents multiple endpoints from retransmitting the same frame
- **Frame validation**: Only retransmits cached frames with valid headers
- **ACK/MISS responses**: 16-byte responses to NACK senders for cache hit (ACK) or miss (MISS)
- **Rate limiting**: Silent drops at IP and LookupSeq levels; SenderID was removed from the BRC-124 NACK wire format and is no longer a rate-limiting key

## Deployment Notes

- **Single ingress worker**: Linux delivers multicast to ALL SO_REUSEPORT sockets; multiple workers would store each frame N times
- **Multicast scoping**: Ensure retry endpoints join the same scope as proxy/listener
- **Redis availability**: If Redis is unavailable, in-memory fallback loses cache on restart and cross-instance dedup coordination
- **Endpoint discovery**: Dynamic via BRC-126 ADVERT beacons; static seed list as fallback

## Metrics

All metrics use the `bre_` prefix:
- `bre_cache_hits_total`, `bre_cache_misses_total`, `bre_cache_size`, `bre_cache_errors_total`
- `bre_nack_requests_total`, `bre_retransmits_total`, `bre_retransmit_dedup_total`
- `bre_rate_limit_drops_total{level=ip|sequence}`
- `bre_frames_received_total`, `bre_frames_cached_total`, `bre_frames_dropped_total{reason}`

## Cross-Repo Dependencies

- `bitcoin-shard-common`: Provides frame encoding/decoding and sharding engine
- `bitcoin-shard-listener`: Listeners send NACK requests to retry endpoints when gaps are detected

## Protocol References

- [BRC-124 Frame Format](https://github.com/lightwebinc/bitcoin-multicast/blob/main/docs/brc-124-frame-format.md)
- [BRC-126 (Retransmission Protocol)](https://github.com/lightwebinc/bitcoin-multicast/blob/main/docs/brc-tbd-retransmission-protocol.md)
- [BRC-TBD-addressing (Multicast Addressing)](https://github.com/lightwebinc/bitcoin-multicast/blob/main/docs/brc-tbd-multicast-addressing.md)
- [NACK Retransmission Flow](https://github.com/lightwebinc/bitcoin-multicast/blob/main/docs/nack-retransmission-flow.md)

## Implementation Notes

### Retransmit fill tracking in listeners

When a retry endpoint retransmits a frame via multicast, listeners that receive
it call `nack.Tracker.Observe` with the retransmitted frame's `PrevSeq`/`CurSeq`.
If the incoming `CurSeq` matches a pending gap key, `Observe` auto-closes the gap
inline (before the next sweeper tick). This is the primary suppression path for
frames recovered by multicast retransmit and is tracked by `bsl_gaps_suppressed_total`.

Out-of-order or retransmitted frames with `prevSeq < lastCurSeq` are accepted
silently: they close gaps but never create phantom gap entries and never regress
the chain pointer.

### Beacon multicast interface binding

On multi-homed Linux hosts (management + fabric NICs), the kernel may route
`ff05::` multicast packets via the default-route interface (management) rather
than the fabric NIC. The beacon sender sets `IPV6_MULTICAST_IF` explicitly
after `net.DialUDP` to force beacons out the fabric interface (`MC_IFACE`).

## License

See LICENSE file.

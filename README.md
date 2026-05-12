# bitcoin-retry-endpoint

[![CI](https://github.com/lightwebinc/bitcoin-retry-endpoint/actions/workflows/ci.yml/badge.svg)](https://github.com/lightwebinc/bitcoin-retry-endpoint/actions/workflows/ci.yml)
[![CodeQL](https://github.com/lightwebinc/bitcoin-retry-endpoint/actions/workflows/codeql.yml/badge.svg)](https://github.com/lightwebinc/bitcoin-retry-endpoint/actions/workflows/codeql.yml)
[![Release](https://img.shields.io/github/v/release/lightwebinc/bitcoin-retry-endpoint)](https://github.com/lightwebinc/bitcoin-retry-endpoint/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/lightwebinc/bitcoin-retry-endpoint.svg)](https://pkg.go.dev/github.com/lightwebinc/bitcoin-retry-endpoint)
[![Go Report Card](https://goreportcard.com/badge/github.com/lightwebinc/bitcoin-retry-endpoint)](https://goreportcard.com/report/github.com/lightwebinc/bitcoin-retry-endpoint)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

Caching endpoint for NACK-based retransmission in the BSV multicast pipeline.
Receives BRC-124/BRC-128 frames from the multicast fabric, caches them, and retransmits
on demand to `bitcoin-shard-listener` nodes that detect sequence gaps.

```
bitcoin-shard-proxy ──multicast──▶ FF05::<shard>:9001
                                         │
                          ┌──────────────┤
                          │              │
                          ▼              ▼
               bitcoin-shard-listener  bitcoin-retry-endpoint
               (gap detected → NACK) ──UDP──▶ [nack-addr]:9300
                          │                   │
                          ◀── ACK / MISS ─────┘
```

## Documentation

- [Architecture](docs/architecture.md) — pipeline overview, ingress, cache, NACK server, retransmit, beacon, NACK bind address, package structure
- [Configuration](docs/configuration.md) — all flags, environment variables, defaults, deployment examples
- [BRC-126 — Retransmission Protocol](https://github.com/lightwebinc/bitcoin-multicast/blob/main/docs/brc-126-retransmission-protocol.md)
- [NACK Retransmission Flow](https://github.com/lightwebinc/bitcoin-multicast/blob/main/docs/nack-retransmission-flow.md)
- [BRC-124 Frame Format](https://github.com/lightwebinc/bitcoin-multicast/blob/main/docs/brc-124-frame-format.md)

## Dependencies

- [`github.com/lightwebinc/bitcoin-shard-common`](https://github.com/lightwebinc/bitcoin-shard-common) — `frame`, `shard`, `seqhash` packages
- [`github.com/coocood/freecache`](https://github.com/coocood/freecache) — GC-free in-memory cache
- Prometheus client + OpenTelemetry SDK

## Requirements

- Go 1.25 or later
- Linux kernel 3.9+ (for `SO_REUSEPORT`)
- IPv6 enabled on the multicast fabric interface
- Multicast routing configured for the same scope as proxy and listeners

## Build

```bash
go build -o bitcoin-retry-endpoint .
```

## Run

```bash
# In-memory cache (single node)
./bitcoin-retry-endpoint \
  -mc-iface eth0 \
  -egress-iface eth0 \
  -shard-bits 16

# Redis cache (multi-node with cross-instance dedup)
./bitcoin-retry-endpoint \
  -mc-iface enp6s0 \
  -egress-iface enp6s0 \
  -shard-bits 16 \
  -cache-backend redis \
  -redis-addr redis.local:6379 \
  -nack-addr fd20::24
```

See [docs/configuration.md](docs/configuration.md) for all flags and environment variable equivalents.

## License

See LICENSE file.

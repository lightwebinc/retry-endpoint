# retry-endpoint

[![CI](https://github.com/lightwebinc/retry-endpoint/actions/workflows/ci.yml/badge.svg)](https://github.com/lightwebinc/retry-endpoint/actions/workflows/ci.yml)
[![CodeQL](https://github.com/lightwebinc/retry-endpoint/actions/workflows/codeql.yml/badge.svg)](https://github.com/lightwebinc/retry-endpoint/actions/workflows/codeql.yml)
[![Release](https://img.shields.io/github/v/release/lightwebinc/retry-endpoint)](https://github.com/lightwebinc/retry-endpoint/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/lightwebinc/retry-endpoint.svg)](https://pkg.go.dev/github.com/lightwebinc/retry-endpoint)
[![Go Report Card](https://goreportcard.com/badge/github.com/lightwebinc/retry-endpoint)](https://goreportcard.com/report/github.com/lightwebinc/retry-endpoint)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

Caching endpoint for NACK-based retransmission in the BSV multicast pipeline.
Receives BRC-124/BRC-128 frames from the multicast fabric, caches them, and retransmits
on demand to `shard-listener` nodes that detect sequence gaps.

```
shard-proxy ──multicast──▶ FF05::<shard>:9001
                                         │
                          ┌──────────────┤
                          │              │
                          ▼              ▼
               shard-listener  retry-endpoint
               (gap detected → NACK) ──UDP──▶ [nack-addr]:9300
                          │                   │
                          ◀── ACK / MISS ─────┘
```

## Documentation

- [Architecture](docs/architecture.md) — pipeline overview, ingress, cache, NACK server, retransmit, beacon, NACK bind address, package structure
- [Configuration](docs/configuration.md) — all flags, environment variables, defaults, deployment examples
- [BRC-126 — Retransmission Protocol](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/brc-126-retransmission-protocol.md)
- [NACK Retransmission Flow](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/nack-retransmission-flow.md)
- [BRC-124 Frame Format](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/brc-124-frame-format.md)

## Dependencies

- [`github.com/lightwebinc/shard-common`](https://github.com/lightwebinc/shard-common) — `frame`, `shard`, `seqhash` packages
- [`github.com/coocood/freecache`](https://github.com/coocood/freecache) — GC-free in-memory cache
- Prometheus client + OpenTelemetry SDK

## Requirements

- Go 1.25 or later
- Linux kernel 3.9+ (for `SO_REUSEPORT`)
- IPv6 enabled on the multicast fabric interface
- Multicast routing configured for the same scope as proxy and listeners

## Build

```bash
go build -o retry-endpoint .
```

## Run

```bash
# In-memory cache (single node)
./retry-endpoint \
  -mc-iface eth0 \
  -egress-iface eth0 \
  -shard-bits 2

# Redis cache (multi-node with cross-instance dedup)
./retry-endpoint \
  -mc-iface enp6s0 \
  -egress-iface enp6s0 \
  -shard-bits 2 \
  -cache-backend redis \
  -redis-addr redis.local:6379 \
  -nack-addr fd20::24

# SSM (RFC 4607) — Posture C. Requires PIM-SSM in the fabric and
# raised net.ipv6.mld_max_msf. See bsv-multicast SSM Support Plan.
./retry-endpoint \
  -mc-iface enp6s0 \
  -egress-iface enp6s0 \
  -shard-bits 2 \
  -scope site \
  -source-mode ssm \
  -bind-source fd20::24 \
  -ssm-bootstrap-manifest shard-manifest-headless.svc.cluster.local \
  -ssm-publishers-static  fd20::a01,fd20::a02   # lab only
```

See [docs/configuration.md](docs/configuration.md) for all flags and environment variable equivalents.

## NACK_ADDR (required in production)

`NACK_ADDR` (or `--nack-addr`) **must** be set to the specific routable IPv6
address that this endpoint advertises in beacons and that listeners send NACKs
to.

If left empty the kernel binds the NACK socket to `[::]` and the default
source-address selection rules may pick a SLAAC address (e.g.
`fd20::216:3eff:fe4c:8a01`) for outgoing ACK responses. Listeners then either:

- discard the ACK because they use a connected socket bound to the advertised
  address (the SLAAC source does not match), or
- drop the ACK at the firewall because the allow-list only contains the
  advertised address.

Either path silently breaks NACK recovery. See
[`shard-listener/nack/nack.go`](https://github.com/lightwebinc/shard-listener/blob/main/nack/nack.go)
and the SLAAC source-address-mismatch fix history.

## Container image

The Dockerfile produces a `gcr.io/distroless/static:nonroot` image with a
single static binary at `/usr/local/bin/retry-endpoint`. No in-image
`ENV` defaults are set; configure via Helm `values.yaml` or container
environment variables / CLI flags.

## Helm chart

A Kubernetes Helm chart is published from a dedicated chart repository:

- Repository: [`lightwebinc/retry-endpoint-helm`](https://github.com/lightwebinc/retry-endpoint-helm)
- HTTPS:
  ```
  helm repo add bre https://lightwebinc.github.io/retry-endpoint-helm
  helm install retry-node-1 bre/retry-endpoint \
    --set config.nackAddr=fd20::24
  ```
- OCI: `helm install retry-node-1 oci://ghcr.io/lightwebinc/charts/retry-endpoint --version 0.1.0`

`config.nackAddr` is effectively required — the chart emits a `helm.sh/chart-warnings` annotation when empty. The chart does **not** bundle a Redis subchart; operators install Redis separately when `config.cacheBackend=redis`. See the chart README for the full reference.

## License

See LICENSE file.

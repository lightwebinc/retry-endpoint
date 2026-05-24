# syntax=docker/dockerfile:1.7
#
# Canonical multi-stage Dockerfile for bitcoin-retry-endpoint.
# Final image: distroless/static:nonroot.
#
# NOTE: in production, NACK_ADDR (or --nack-addr) must be set to the
# specific routable IPv6 advertised in beacons. See README.md.

FROM golang:1.25-alpine AS builder
RUN apk add --no-cache git ca-certificates
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .
ARG VERSION=dev
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -buildvcs=false \
      -ldflags "-s -w -X github.com/lightwebinc/bitcoin-retry-endpoint/metrics.Version=${VERSION}" \
      -o /out/bitcoin-retry-endpoint .

FROM gcr.io/distroless/static:nonroot
USER nonroot:nonroot
COPY --from=builder /out/bitcoin-retry-endpoint /usr/local/bin/bitcoin-retry-endpoint
ENTRYPOINT ["/usr/local/bin/bitcoin-retry-endpoint"]

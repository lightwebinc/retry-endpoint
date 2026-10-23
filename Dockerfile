# syntax=docker/dockerfile:1.28.0@sha256:bb22d9815c728170f72750f4e5b0d672e06176142e1d602c7e66c050100b7e5b
#
# Canonical multi-stage Dockerfile for retry-endpoint.
# Final image: distroless/static:nonroot.
#
# NOTE: in production, NACK_ADDR (or --nack-addr) must be set to the
# specific routable IPv6 advertised in beacons. See README.md.

FROM golang:1.27-alpine@sha256:b8c9b82affbbb82a020bbcf998dad2ba9516944ac7e883524642a481a9505e9c AS builder
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
      -ldflags "-s -w -X github.com/lightwebinc/retry-endpoint/metrics.Version=${VERSION}" \
      -o /out/retry-endpoint .

FROM gcr.io/distroless/static:nonroot@sha256:d62067879de890a1def9da0e7766b7d20810a5e5555cef8506b752437d7a3e88
USER nonroot:nonroot
COPY --from=builder /out/retry-endpoint /usr/local/bin/retry-endpoint
ENTRYPOINT ["/usr/local/bin/retry-endpoint"]

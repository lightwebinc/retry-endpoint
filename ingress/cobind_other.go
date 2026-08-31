//go:build !freebsd

package ingress

// coBindOpts is a no-op off FreeBSD, and deliberately so on Linux.
//
// Linux shares this port across EUIDs via SO_REUSEADDR alone (the sk_reuse bind
// path). Setting SO_REUSEPORT here would force the kernel's same-EUID anti-hijack
// check and DEFEAT that share, breaking the collapsed-node co-bind that works
// today between retry-endpoint and a differently-owned shard-listener.
func coBindOpts(int) error { return nil }

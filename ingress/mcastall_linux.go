//go:build linux

package ingress

import "golang.org/x/sys/unix"

// disableMulticastAll scopes a wildcard-bound socket to the groups THIS process
// joined.
//
// IPV6_MULTICAST_ALL defaults to 1, which delivers every group joined ANYWHERE
// on the host to this wildcard-bound socket — including groups this process
// never joined. On a collapsed node the co-resident listener's joins therefore
// side-feed this cache, which makes coverage unauditable in both directions: a
// retry reads healthy on a band it never joined (masking real misconfiguration
// — this is what made an edge look fine while a listener-less spine starved),
// and own-source frames the join roster deliberately excludes still arrive and
// create a per-source series that can never advance.
//
// Turning it off makes what this cache holds equal what it declared. Verified
// safe rather than assumed: on testnet the side-fed edge and the listener-less
// spine cached byte-identical totals, so it was contributing no coverage — the
// only band received-but-not-joined is 0xFFFC, BRC-127 group announces, which
// are TTL-refreshed soft state that self-heals and is deliberately not a
// NACK-repaired flow. Best-effort: an old kernel without the option keeps the
// previous permissive behaviour rather than failing the socket.
func disableMulticastAll(fd int) {
	_ = unix.SetsockoptInt(fd, unix.IPPROTO_IPV6, unix.IPV6_MULTICAST_ALL, 0)
}

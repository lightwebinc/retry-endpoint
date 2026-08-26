//go:build !linux

package ingress

// disableMulticastAll is a no-op off Linux, and that is correct rather than a
// missing port.
//
// IPV6_MULTICAST_ALL is a Linux extension. Linux's default of 1 delivers every
// group joined ANYWHERE on the host to a wildcard-bound socket, which is the
// behaviour mcastall_linux.go turns OFF. FreeBSD has no such option because it
// never had that behaviour: a socket receives only the groups it joined, which
// is precisely the end state the Linux call is reaching for.
//
// So the semantics match; only the knob is absent. Doing nothing here leaves the
// cache holding exactly what it declared, the same invariant the Linux path
// enforces explicitly.
func disableMulticastAll(_ int) {}

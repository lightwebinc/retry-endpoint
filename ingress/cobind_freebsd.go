//go:build freebsd

package ingress

import "golang.org/x/sys/unix"

// coBindOpts applies the options that let this socket share its wildcard UDP
// port with a co-resident shard-listener.
//
// FreeBSD has NO equivalent of Linux's cross-EUID SO_REUSEADDR share. Measured
// on 15.1-RELEASE-p3, a second wildcard UDP6 bind succeeds ONLY with
// SO_REUSEPORT set on BOTH sockets AND both processes running as the SAME uid;
// SO_REUSEADDR alone fails even same-user, and SO_REUSEPORT fails cross-user.
// So this half is necessary but not sufficient: the deployment must also give
// retry-endpoint and shard-listener-1bsv one uid on FreeBSD.
//
// Without it the listener crash-loops on `bind [::]::9001: address already in
// use` while this process holds the port, with no other symptom.
func coBindOpts(fd int) error {
	return unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
}

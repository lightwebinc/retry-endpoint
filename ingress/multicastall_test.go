package ingress

import (
	"net"
	"testing"

	"golang.org/x/sys/unix"
)

// The socket must receive ONLY the groups this process joined. IPV6_MULTICAST_ALL
// defaults to 1, which delivers anything joined anywhere on the host — on a
// collapsed node that silently side-feeds this cache from the co-resident
// listener's joins, so a retry reads healthy on a band it never joined and
// own-source frames the roster excludes still arrive.
//
// The setsockopt is deliberately best-effort (an old kernel keeps the permissive
// default rather than failing the socket), which is precisely why it needs a test:
// a silently-dropped option is indistinguishable from a working one at runtime.
func TestOpenRawSocket_MulticastAllDisabled(t *testing.T) {
	w := &Worker{port: 0, iface: &net.Interface{Index: 1}}
	fd, err := w.openRawSocket()
	if err != nil {
		t.Fatalf("openRawSocket: %v", err)
	}
	defer func() { _ = unix.Close(fd) }()

	got, err := unix.GetsockoptInt(fd, unix.IPPROTO_IPV6, unix.IPV6_MULTICAST_ALL)
	if err != nil {
		t.Skipf("kernel does not support IPV6_MULTICAST_ALL: %v", err)
	}
	if got != 0 {
		t.Errorf("IPV6_MULTICAST_ALL = %d, want 0 — the socket would receive groups "+
			"it never joined, making cache coverage unauditable", got)
	}
}

// SO_REUSEADDR is the cross-EUID co-bind with a co-resident listener; disabling
// MULTICAST_ALL must not disturb it.
func TestOpenRawSocket_ReuseAddrStillSet(t *testing.T) {
	w := &Worker{port: 0, iface: &net.Interface{Index: 1}}
	fd, err := w.openRawSocket()
	if err != nil {
		t.Fatalf("openRawSocket: %v", err)
	}
	defer func() { _ = unix.Close(fd) }()

	if got, err := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR); err != nil || got == 0 {
		t.Errorf("SO_REUSEADDR = %d (err %v), want non-zero", got, err)
	}
}

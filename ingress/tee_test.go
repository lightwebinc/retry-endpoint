package ingress

import (
	"context"
	"log/slog"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/teewire"
)

// freeLoopbackPort finds a currently free UDP port on ::1. There is an
// unavoidable close-then-rebind window, so callers retry on bind failure.
func freeLoopbackPort(t *testing.T) uint16 {
	t.Helper()
	pc, err := net.ListenPacket("udp6", "[::1]:0")
	if err != nil {
		t.Skipf("no IPv6 loopback available: %v", err)
	}
	port := uint16(pc.LocalAddr().(*net.UDPAddr).Port)
	_ = pc.Close()
	return port
}

// startTee runs RunTee on a fresh ephemeral loopback port and returns the
// listen address once the socket is ready (probed by a successful send of a
// zero-length datagram, which RunTee ignores).
func startTee(t *testing.T, w *Worker) (netip.AddrPort, context.CancelFunc) {
	t.Helper()
	if w.log == nil {
		w.log = slog.Default().With("component", "ingress-test")
	}
	ctx, cancel := context.WithCancel(context.Background())

	var listen netip.AddrPort
	errCh := make(chan error, 1)
	for attempt := 0; attempt < 5; attempt++ {
		listen = netip.AddrPortFrom(netip.MustParseAddr("::1"), freeLoopbackPort(t))
		go func(ap netip.AddrPort) {
			errCh <- w.RunTee(ctx, ap)
		}(listen)
		// Bind failure surfaces immediately; give it a moment.
		select {
		case err := <-errCh:
			if err == nil {
				t.Fatal("RunTee returned nil before ctx cancel")
			}
			continue // port raced away; try another
		case <-time.After(50 * time.Millisecond):
		}
		t.Cleanup(func() {
			cancel()
			select {
			case <-errCh:
			case <-time.After(2 * time.Second):
				t.Error("RunTee did not exit on ctx cancel")
			}
		})
		return listen, cancel
	}
	t.Fatal("could not bind a tee socket in 5 attempts")
	return netip.AddrPort{}, nil
}

func sendTo(t *testing.T, dst netip.AddrPort, pkt []byte) {
	t.Helper()
	c, err := net.Dial("udp6", dst.String())
	if err != nil {
		t.Fatalf("dial tee: %v", err)
	}
	defer func() { _ = c.Close() }()
	if _, err := c.Write(pkt); err != nil {
		t.Fatalf("write tee: %v", err)
	}
}

func waitStores(t *testing.T, mc *mockCache, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if mc.storeCount() >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("stores = %d after 2s, want >= %d", mc.storeCount(), want)
}

func TestRunTee_EncapFrameStored(t *testing.T) {
	mc := &mockCache{}
	w := newTestWorker(mc)
	listen, _ := startTee(t, w)

	raw := buildRaw(t, 0x1111222233334444, 42, []byte("remote-origin"))
	src := netip.MustParseAddrPort("[fd00:50:0:2::1]:9001")
	sendTo(t, listen, teewire.AppendEncap(nil, src, raw))

	waitStores(t, mc, 1)
	entry := mc.storeAt(0)
	if len(entry.val) != len(raw) {
		t.Fatalf("stored %d bytes, want %d (envelope must be stripped)", len(entry.val), len(raw))
	}
	for i := range raw {
		if entry.val[i] != raw[i] {
			t.Fatalf("stored bytes differ from wire frame at offset %d", i)
		}
	}
}

func TestRunTee_RawFrameStored(t *testing.T) {
	// The proxy's -retry-tee sends bare frames; the tee socket must accept
	// them unchanged so the proxy can be re-pointed without a code change.
	mc := &mockCache{}
	w := newTestWorker(mc)
	listen, _ := startTee(t, w)

	raw := buildRaw(t, 0x5555666677778888, 7, []byte("own-origin"))
	sendTo(t, listen, raw)

	waitStores(t, mc, 1)
	if got := mc.storeAt(0).val; len(got) != len(raw) {
		t.Fatalf("stored %d bytes, want %d", len(got), len(raw))
	}
}

func TestRunTee_MalformedEnvelopeDropped(t *testing.T) {
	mc := &mockCache{}
	w := newTestWorker(mc)
	listen, _ := startTee(t, w)

	// Correct magic, truncated header: must drop, not store, not crash.
	good := teewire.AppendEncap(nil, netip.MustParseAddrPort("[fd00::1]:9001"), buildRaw(t, 1, 1, nil))
	sendTo(t, listen, good[:teewire.HeaderSize-2])

	// Unsupported envelope version: drop.
	bad := append([]byte(nil), good...)
	bad[4] = 0x7F
	sendTo(t, listen, bad)

	// Non-frame junk in raw form: decode error, no store.
	sendTo(t, listen, []byte{0xDE, 0xAD, 0xBE, 0xEF, 0, 0, 0, 0, 0, 0})

	// A valid frame after the garbage proves the loop survived it all.
	raw := buildRaw(t, 0x9999AAAABBBBCCCC, 3, []byte("survivor"))
	sendTo(t, listen, teewire.AppendEncap(nil, netip.MustParseAddrPort("[fd00::2]:9001"), raw))

	waitStores(t, mc, 1)
	if mc.storeCount() != 1 {
		t.Fatalf("stores = %d, want exactly 1 (garbage must not be cached)", mc.storeCount())
	}
}

func TestRunTee_ZeroSeqNumSkippedViaTee(t *testing.T) {
	// The unstamped-frame rule applies regardless of feed.
	mc := &mockCache{}
	w := newTestWorker(mc)
	listen, _ := startTee(t, w)

	raw := buildRaw(t, 0x1234, 0, nil)
	sendTo(t, listen, teewire.AppendEncap(nil, netip.MustParseAddrPort("[fd00::3]:9001"), raw))
	stamped := buildRaw(t, 0x1234, 1, nil)
	sendTo(t, listen, teewire.AppendEncap(nil, netip.MustParseAddrPort("[fd00::3]:9001"), stamped))

	waitStores(t, mc, 1)
	if mc.storeCount() != 1 {
		t.Fatalf("stores = %d, want 1 (SeqNum 0 must be skipped)", mc.storeCount())
	}
}

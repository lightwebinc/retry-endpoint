package ingress

import (
	"context"
	"log/slog"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"
)

// A JOIN IS NOT A DURABLE DECLARATION.
//
// The kernel's membership can be lost underneath a running socket when the
// forwarding state it depends on changes — a routing re-convergence that moves
// the multicast RPF, a forwarder restart, an interface rebuild. Nothing in the
// socket API reports it: recvfrom just stops producing frames for that source
// while every other source keeps flowing, so the process looks healthy and its
// per-source counter freezes. Four nodes hit this in one day after a single BGP
// best-path flip, and each needed a manual restart to re-join.
//
// So the worker must re-assert on its own cadence. This asserts the two
// properties that make that safe rather than merely present.
func TestReassertJoinsToleratesAlreadyJoinedAndStops(t *testing.T) {
	// A worker with a group and a bogus fd: every join will fail, which is the
	// case that must NOT be fatal on a re-assert (re-joining a live membership is
	// reported as an error by some stacks, and a node must not lose a working
	// ingress because a redundant join was refused).
	w := &Worker{
		iface:  &net.Interface{Index: 1, Name: "lo"},
		groups: []*net.UDPAddr{{IP: net.ParseIP("ff35::b:0"), Port: 9001}},
		log:    slog.Default(),
	}

	// First pass: an error IS fatal — a worker that cannot join has nothing to do.
	if err := w.joinAll(-1, true); err == nil {
		t.Error("a failed FIRST join was swallowed; the worker would run deaf")
	}
	// Re-assert pass: the same error must be tolerated.
	if err := w.joinAll(-1, false); err != nil {
		t.Errorf("a failed re-assert was fatal: %v", err)
	}

	// And the loop must exit with its context rather than outliving the socket.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.reassertJoins(ctx, -1); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reassertJoins did not exit on context cancel")
	}
}

// The cadence has to be well inside the fabric's MLD query intervals, or a
// membership can lapse and stay lapsed for longer than a query cycle — which is
// the window this whole mechanism exists to close.
func TestJoinIntervalIsInsideTheQueryWindow(t *testing.T) {
	if joinInterval > 60*time.Second {
		t.Errorf("joinInterval %s is too slow to close a lapsed membership", joinInterval)
	}
	if joinInterval < 5*time.Second {
		t.Errorf("joinInterval %s re-asserts more often than any forwarding change occurs", joinInterval)
	}
}

// SetGroupSources feeds the SSM source list the re-assert replays; a worker that
// lost it would silently downgrade to an ASM join on every re-assert.
func TestReassertKeepsTheSSMSources(t *testing.T) {
	ga := netip.MustParseAddr("ff35::b:0")
	w := &Worker{iface: &net.Interface{Index: 1}, log: slog.Default()}
	w.SetGroupSources(GroupSources{ga: {netip.MustParseAddr("fd00:50:0:6::1")}})
	if got := w.sources[ga]; len(got) != 1 || got[0].String() != "fd00:50:0:6::1" {
		t.Errorf("sources lost: %v", w.sources)
	}
	if !strings.Contains(ga.String(), "ff35") {
		t.Fatal("test group is not in the SSM range")
	}
}

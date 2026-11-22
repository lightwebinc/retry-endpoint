package proxy

import (
	"context"
	"encoding/binary"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
)

// switchUpstream is an upstream retry-endpoint that answers MISS until it is
// told it has the frame, then ACK and the frame.
type switchUpstream struct {
	conn  *net.UDPConn
	frame []byte
	has   atomic.Bool
	nacks atomic.Int64
}

func startSwitchUpstream(t *testing.T, fr []byte) *switchUpstream {
	t.Helper()
	conn, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.IPv6loopback})
	if err != nil {
		t.Fatalf("upstream listen: %v", err)
	}
	u := &switchUpstream{conn: conn, frame: fr}
	go u.serve()
	t.Cleanup(func() { _ = conn.Close() })
	return u
}

func (u *switchUpstream) addr() string { return u.conn.LocalAddr().String() }

func (u *switchUpstream) serve() {
	buf := make([]byte, 1500)
	for {
		n, src, err := u.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		if n < nackSize {
			continue
		}
		u.nacks.Add(1)
		var resp [respSize]byte
		binary.BigEndian.PutUint32(resp[0:4], frame.MagicBSV)
		binary.BigEndian.PutUint16(resp[4:6], frame.ProtoVer)
		if !u.has.Load() {
			resp[6] = msgTypeMISS
			_, _ = u.conn.WriteToUDP(resp[:], src)
			continue
		}
		resp[6] = msgTypeACK
		_, _ = u.conn.WriteToUDP(resp[:], src)
		_, _ = u.conn.WriteToUDP(u.frame, src)
	}
}

// Regression: a recovery that found the frame nowhere kept its in-flight claim
// for -proxy-dedup-window (60 s by default), so every later NACK for the gap
// was dropped as if a sibling's fetch were in flight, and the gap could not be
// recovered for that long even once an upstream had the frame. A failed fetch
// releases its claim; a successful one keeps it, so siblings still share it.
func TestRecover_FailedFetchReleasesClaim(t *testing.T) {
	up := startSwitchUpstream(t, makeFrame(4, 4))
	frt := &fakeRetrans{}
	c := newTestClient(Config{
		Upstreams:   []string{up.addr()},
		Cache:       newFakeCache(),
		Retrans:     frt,
		Multicast:   true,
		Dedup:       newFakeDedup(),
		DedupWindow: time.Minute,
		Timeout:     200 * time.Millisecond,
	})

	c.recover(context.Background(), job{hashKey: 4, seq: 4})
	if got := frt.count(); got != 0 {
		t.Fatalf("recovery against an upstream without the frame retransmitted %d", got)
	}

	up.has.Store(true)
	c.recover(context.Background(), job{hashKey: 4, seq: 4})
	if got := up.nacks.Load(); got != 2 {
		t.Fatalf("upstream asked %d times, want 2: the failed fetch kept its claim and the next NACK was dropped", got)
	}
	if got := frt.count(); got != 1 {
		t.Fatalf("retransmits = %d after the upstream got the frame, want 1", got)
	}

	// The successful recovery keeps its claim: a sibling's NACK for the gap is
	// still deduplicated.
	c.recover(context.Background(), job{hashKey: 4, seq: 4})
	if got := up.nacks.Load(); got != 2 {
		t.Errorf("upstream asked %d times, want 2: a successful recovery must keep its claim", got)
	}
}

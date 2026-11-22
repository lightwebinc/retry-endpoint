package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	scache "github.com/lightwebinc/shard-common/cache"
	"github.com/lightwebinc/shard-common/shard"

	"github.com/lightwebinc/retry-endpoint/cache"
	"github.com/lightwebinc/retry-endpoint/retransmit"
)

// multicastACKs counts the ACKs written to conn that report a multicast copy.
func multicastACKs(conn *mockPacketConn) int {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	n := 0
	for _, b := range conn.written {
		if len(b) == ResponseSize && b[6] == msgTypeACK && b[7]&ackFlagMulticastSent != 0 {
			n++
		}
	}
	return n
}

// siblingServer is a NACK server whose multicast retransmits claim in dedup,
// the store shared by every sibling endpoint.
func siblingServer(frames *mockCache, dedup cache.Deduper) *Server {
	rt := retransmit.New(shard.New(0xFF05, shard.DefaultGroupID, 2), nil, 9001, time.Minute, dedup, nil, false)
	return New(9300, frames, permissiveRL(), nil, rt, 1, false)
}

// Regression: the retransmit claim (HashKey ∥ SeqNum, -dedup-window, 60 s by
// default) also matched the repeat NACK of a requester the copy had not
// reached. A listener whose multicast repair was lost asks again, of the next
// endpoint after an unanswered round, and every endpoint, the one that sent
// the copy included, held the frame as claimed: no copy and no ACK for the
// window, past the listener's last round, so the gap was lost. The first NACK
// of each requester must still share one copy across siblings; a repeat must
// be repaired.
func TestProcessNACK_RepeatNACKIsRepairedAcrossSiblings(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	backend, err := scache.Open(context.Background(), scache.Config{Backend: scache.BackendRedis, RedisAddr: mr.Addr()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = backend.Close() }()
	dedup := cache.NewStore(backend, "bre:dedup:", time.Second)

	const hashKey, seq = uint64(0xFEED), uint64(9)
	frames := newMockCache()
	storeFrame(frames, hashKey, seq, buildCacheFrame(t, seq))
	e1, e2 := siblingServer(frames, dedup), siblingServer(frames, dedup)
	c1, c2 := &mockPacketConn{}, &mockPacketConn{}
	nack := buildNACK(msgTypeNACK, hashKey, seq, seq)
	// A listener sends each NACK from a fresh port.
	listenerA := func(port int) *net.UDPAddr { return &net.UDPAddr{IP: net.ParseIP("2001:db8::a"), Port: port} }
	listenerB := func(port int) *net.UDPAddr { return &net.UDPAddr{IP: net.ParseIP("2001:db8::b"), Port: port} }

	// One loss, two listeners asking two endpoints: one copy serves both.
	e1.processNACK(c1, 0, nack, listenerA(40001))
	e2.processNACK(c2, 0, nack, listenerB(40002))
	if got := multicastACKs(c1); got != 1 {
		t.Fatalf("A's first NACK to e1: %d multicast ACKs, want 1 (the copy sent)", got)
	}
	if got := multicastACKs(c2); got != 0 {
		t.Fatalf("B's first NACK to e2: %d multicast ACKs, want 0 (e1's copy serves B too)", got)
	}

	// Neither copy arrived: each listener asks the other endpoint again.
	e2.processNACK(c2, 0, nack, listenerA(40011))
	e1.processNACK(c1, 0, nack, listenerB(40012))
	if got := multicastACKs(c2); got != 1 {
		t.Errorf("A's repeat NACK to e2: %d multicast ACKs, want 1: a repeat must be repaired", got)
	}
	if got := multicastACKs(c1); got != 2 {
		t.Errorf("B's repeat NACK to e1: %d multicast ACKs on e1, want 2: a repeat must be repaired", got)
	}
}

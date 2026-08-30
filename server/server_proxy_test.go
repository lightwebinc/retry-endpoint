package server

import (
	"net"
	"sync"
	"testing"

	"github.com/lightwebinc/retry-endpoint/proxy"
)

type mockEnqueuer struct {
	mu    sync.Mutex
	calls []enqueueCall
}

type enqueueCall struct {
	hashKey uint64
	seq     uint64
	subtree [32]byte
}

func (m *mockEnqueuer) Enqueue(hashKey, seq uint64, subtree [32]byte, requester *net.UDPAddr) bool {
	m.mu.Lock()
	m.calls = append(m.calls, enqueueCall{hashKey, seq, subtree})
	m.mu.Unlock()
	return true
}

func (m *mockEnqueuer) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func TestProcessNACK_Miss_EnqueuesProxy(t *testing.T) {
	mc := newMockCache() // empty → miss
	rt := &mockRetransmitter{}
	conn := &mockPacketConn{}
	enq := &mockEnqueuer{}

	s := New(9300, mc, permissiveRL(), nil, rt, 1, false)
	s.SetProxy(enq)

	const hashKey uint64 = 0xABCD
	const seq uint64 = 99
	src := &net.UDPAddr{IP: net.IPv6loopback, Port: 1}
	s.processNACK(conn, 0, buildNACK(msgTypeNACK, hashKey, seq, seq), src)

	if enq.count() != 1 {
		t.Fatalf("expected 1 enqueue on miss, got %d", enq.count())
	}
	if enq.calls[0].hashKey != hashKey || enq.calls[0].seq != seq {
		t.Errorf("enqueue args = (%d,%d), want (%d,%d)", enq.calls[0].hashKey, enq.calls[0].seq, hashKey, seq)
	}
}

func TestProcessNACK_Miss_ProxiedNotReEnqueued(t *testing.T) {
	mc := newMockCache() // empty → miss
	rt := &mockRetransmitter{}
	conn := &mockPacketConn{}
	enq := &mockEnqueuer{}

	s := New(9300, mc, permissiveRL(), nil, rt, 1, false)
	s.SetProxy(enq)

	nack := buildNACK(msgTypeNACK, 1, 1, 1)
	nack[7] |= proxy.FlagProxied // already proxied → must not re-proxy

	s.processNACK(conn, 0, nack, &net.UDPAddr{IP: net.IPv6loopback, Port: 1})

	if enq.count() != 0 {
		t.Errorf("proxied NACK must not be re-enqueued, got %d enqueues", enq.count())
	}
}

func TestProcessNACK_ProxiedHit_ReturnsUnicast(t *testing.T) {
	mc := newMockCache()
	rt := &mockRetransmitter{}
	conn := &mockPacketConn{}

	const hashKey uint64 = 0x2222
	const seq uint64 = 7
	storeFrame(mc, hashKey, seq, buildCacheFrame(t, seq))

	// Unicast mode OFF — a proxied NACK must still get a unicast copy.
	s := New(9300, mc, permissiveRL(), nil, rt, 1, false)

	nack := buildNACK(msgTypeNACK, hashKey, seq, seq)
	nack[7] |= proxy.FlagProxied
	s.processNACK(conn, 0, nack, &net.UDPAddr{IP: net.IPv6loopback, Port: 1})

	if !rt.unicastCalled {
		t.Error("proxied NACK on a cache hit must trigger unicast retransmit")
	}
}

func TestProcessNACK_UnicastMode_Hit_BothPaths(t *testing.T) {
	mc := newMockCache()
	rt := &mockRetransmitter{}
	conn := &mockPacketConn{}

	const hashKey uint64 = 0x3333
	const seq uint64 = 4
	storeFrame(mc, hashKey, seq, buildCacheFrame(t, seq))

	s := New(9300, mc, permissiveRL(), nil, rt, 1, false)
	s.SetRetransmitModes(true, true) // multicast + unicast

	s.processNACK(conn, 0, buildNACK(msgTypeNACK, hashKey, seq, seq), &net.UDPAddr{IP: net.IPv6loopback, Port: 1})

	if !rt.called {
		t.Error("multicast retransmit not called")
	}
	if !rt.unicastCalled {
		t.Error("unicast retransmit not called")
	}
}

func TestProcessNACK_MulticastOff_Hit_NoMulticast(t *testing.T) {
	mc := newMockCache()
	rt := &mockRetransmitter{}
	conn := &mockPacketConn{}

	const hashKey uint64 = 0x4444
	const seq uint64 = 4
	storeFrame(mc, hashKey, seq, buildCacheFrame(t, seq))

	s := New(9300, mc, permissiveRL(), nil, rt, 1, false)
	s.SetRetransmitModes(false, false) // neither mode

	s.processNACK(conn, 0, buildNACK(msgTypeNACK, hashKey, seq, seq), &net.UDPAddr{IP: net.IPv6loopback, Port: 1})

	if rt.called {
		t.Error("multicast retransmit should be suppressed when mode is off")
	}
	// Still ACKs so the listener does not escalate.
	if len(conn.written) != 1 || conn.written[0][6] != msgTypeACK {
		t.Error("expected an ACK even when retransmit modes are off")
	}
}

package server

import (
	"net"
	"testing"
	"time"

	"github.com/lightwebinc/retry-endpoint/ratelimit"
)

// TestProcessNACK_ThrottleResponse_SequenceTier verifies that, with the throttle
// response enabled, a sequence-tier throttle returns a THROTTLED hint (bucket
// for the sequence tier) rather than silence.
func TestProcessNACK_ThrottleResponse_SequenceTier(t *testing.T) {
	mc := newMockCache()
	rt := &mockRetransmitter{}
	conn := &mockPacketConn{}

	const seqNum uint64 = 0xABCD0001
	storeFrame(mc, 0, seqNum, buildCacheFrame(t, seqNum))

	// Sequence limiter allows 1 per window; IP/chain wide open.
	rl := ratelimit.New(ratelimit.Config{
		IPRate: 1e9, IPBurst: 1_000_000,
		ChainRate: 1e9, ChainWindow: time.Second,
		SequenceMax: 1, SequenceWindow: time.Minute,
		GroupRate: 1e9, GroupBurst: 1_000_000,
	})
	s := New(9300, mc, rl, nil, rt, 1, false)
	s.SetThrottleResponse(true)
	src := &net.UDPAddr{IP: net.IPv6loopback, Port: 12345}

	s.processNACK(conn, 0, buildNACK(msgTypeNACK, 0, seqNum, seqNum), src) // 1st: served
	conn.written = nil
	s.processNACK(conn, 0, buildNACK(msgTypeNACK, 0, seqNum, seqNum), src) // 2nd: seq-throttled

	if len(conn.written) != 1 {
		t.Fatalf("expected 1 THROTTLED response, got %d", len(conn.written))
	}
	if got := conn.written[0][6]; got != msgTypeThrottled {
		t.Fatalf("response type = 0x%02X, want THROTTLED (0x%02X)", got, msgTypeThrottled)
	}
	if got := conn.written[0][7] & 0x0F; got != throttleBucketSequence {
		t.Errorf("bucket = %d, want %d (sequence)", got, throttleBucketSequence)
	}
}

// TestProcessNACK_ThrottleResponse_ChainTier verifies the chain tier returns a
// THROTTLED hint with the chain bucket.
func TestProcessNACK_ThrottleResponse_ChainTier(t *testing.T) {
	mc := newMockCache()
	rt := &mockRetransmitter{}
	conn := &mockPacketConn{}

	const hashKey, seqNum uint64 = 0xF00D, 0xABCD0002
	storeFrame(mc, hashKey, seqNum, buildCacheFrame(t, seqNum))

	// Chain limiter allows 1 per window; IP/sequence wide open.
	rl := ratelimit.New(ratelimit.Config{
		IPRate: 1e9, IPBurst: 1_000_000,
		ChainRate: 1, ChainWindow: time.Minute,
		SequenceMax: 1_000_000, SequenceWindow: time.Minute,
		GroupRate: 1e9, GroupBurst: 1_000_000,
	})
	s := New(9300, mc, rl, nil, rt, 1, false)
	s.SetThrottleResponse(true)
	src := &net.UDPAddr{IP: net.IPv6loopback, Port: 12345}

	s.processNACK(conn, 0, buildNACK(msgTypeNACK, hashKey, seqNum, seqNum), src) // 1st: served
	conn.written = nil
	s.processNACK(conn, 0, buildNACK(msgTypeNACK, hashKey, seqNum, seqNum), src) // 2nd: chain-throttled

	if len(conn.written) != 1 {
		t.Fatalf("expected 1 THROTTLED response, got %d", len(conn.written))
	}
	if got := conn.written[0][6]; got != msgTypeThrottled {
		t.Fatalf("response type = 0x%02X, want THROTTLED (0x%02X)", got, msgTypeThrottled)
	}
	if got := conn.written[0][7] & 0x0F; got != throttleBucketChain {
		t.Errorf("bucket = %d, want %d (chain)", got, throttleBucketChain)
	}
}

// TestProcessNACK_ThrottleResponse_IPTierStaysSilent verifies the IP flood tier
// never answers, even with the throttle response enabled — so a spoofed-source
// flood cannot be reflected.
func TestProcessNACK_ThrottleResponse_IPTierStaysSilent(t *testing.T) {
	mc := newMockCache()
	rt := &mockRetransmitter{}
	conn := &mockPacketConn{}

	const seqNum uint64 = 0xABCD0003
	storeFrame(mc, 0, seqNum, buildCacheFrame(t, seqNum))

	rl := ratelimit.New(ratelimit.Config{
		IPRate: 1, IPBurst: 1,
		ChainRate: 1e9, ChainWindow: time.Second,
		SequenceMax: 1_000_000, SequenceWindow: time.Minute,
		GroupRate: 1e9, GroupBurst: 1_000_000,
	})
	s := New(9300, mc, rl, nil, rt, 1, false)
	s.SetThrottleResponse(true)
	src := &net.UDPAddr{IP: net.IPv6loopback, Port: 12345}

	s.processNACK(conn, 0, buildNACK(msgTypeNACK, 0, seqNum, seqNum), src) // 1st: served
	conn.written = nil
	s.processNACK(conn, 0, buildNACK(msgTypeNACK, 0, seqNum, seqNum), src) // 2nd: IP-throttled

	if len(conn.written) != 0 {
		t.Errorf("IP tier must stay silent, got %d response(s)", len(conn.written))
	}
}

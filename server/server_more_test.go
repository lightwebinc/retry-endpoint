package server

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/lightwebinc/bitcoin-retry-endpoint/ratelimit"
	"github.com/lightwebinc/bitcoin-shard-common/frame"
)

func TestProcessNACK_IPRateLimit_DropsRequest(t *testing.T) {
	mc := newMockCache()
	rt := &mockRetransmitter{}
	conn := &mockPacketConn{}

	const curSeq uint64 = 0xCAFE0003
	storePrimary(mc, curSeq, buildCacheFrame(t, curSeq))

	// Tight IP limiter: burst=1, rate=1/s
	tightRL := ratelimit.New(ratelimit.Config{
		IPRate:      1,
		IPBurst:     1,
		ChainRate:   1e9,
		ChainWindow: time.Second,
		SequenceMax: 1_000_000,
		GroupRate:   1e9,
		GroupBurst:  1_000_000,
	})

	s := New(9300, mc, tightRL, nil, rt, 1, false)
	src := &net.UDPAddr{IP: net.IPv6loopback, Port: 12345}

	// First passes
	s.processNACK(conn, 0, buildNACK(msgTypeNACK, lookupByCurSeq, curSeq), src)
	if !rt.called {
		t.Fatal("first IP request should pass")
	}
	rt.called = false
	s.processNACK(conn, 0, buildNACK(msgTypeNACK, lookupByCurSeq, curSeq), src)
	if rt.called {
		t.Error("second request should be IP-rate-limited")
	}
}

func TestProcessNACK_UnknownLookupType(t *testing.T) {
	mc := newMockCache()
	rt := &mockRetransmitter{}
	conn := &mockPacketConn{}

	s := New(9300, mc, permissiveRL(), nil, rt, 1, false)
	src := &net.UDPAddr{IP: net.IPv6loopback, Port: 12345}
	// LookupType = 0x99 (unknown) → silently dropped
	s.processNACK(conn, 0, buildNACK(msgTypeNACK, 0x99, 0x1234), src)
	if rt.called || len(conn.written) != 0 {
		t.Errorf("unknown lookup type should be dropped silently")
	}
}

func TestSetBindAddr(t *testing.T) {
	s := New(9300, newMockCache(), permissiveRL(), nil, nil, 1, false)
	s.SetBindAddr("fd20::24")
	if s.bindAddr != "fd20::24" {
		t.Errorf("bindAddr = %q", s.bindAddr)
	}
}

func TestSetShardEngine(t *testing.T) {
	s := New(9300, newMockCache(), permissiveRL(), nil, nil, 1, false)
	if s.shardEngine != nil {
		t.Error("default shardEngine should be nil")
	}
}

// TestRun_StartStop verifies Run binds, accepts a NACK, and stops on ctx cancel.
func TestRun_StartStop(t *testing.T) {
	mc := newMockCache()
	rt := &mockRetransmitter{}

	// Allocate a free UDP port.
	probe, err := net.ListenPacket("udp6", "[::1]:0")
	if err != nil {
		t.Skipf("udp6 unavailable: %v", err)
	}
	port := probe.LocalAddr().(*net.UDPAddr).Port
	_ = probe.Close()

	const curSeq uint64 = 0xC0DECAFE
	storePrimary(mc, curSeq, buildCacheFrame(t, curSeq))

	s := New(port, mc, permissiveRL(), nil, rt, 2, false)
	s.SetBindAddr("::1")

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx) }()
	defer cancel()

	// Wait briefly for socket to bind.
	time.Sleep(150 * time.Millisecond)

	// Send a NACK datagram.
	c, err := net.Dial("udp6", net.JoinHostPort("::1", itoa(port)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	_, _ = c.Write(buildNACK(msgTypeNACK, lookupByCurSeq, curSeq))

	// Read ACK response.
	buf := make([]byte, ResponseSize)
	n, err := c.Read(buf)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if n != ResponseSize || buf[6] != msgTypeACK {
		t.Errorf("bad response: n=%d type=0x%02X", n, buf[6])
	}
	gotSeq := binary.BigEndian.Uint64(buf[8:16])
	if gotSeq != curSeq {
		t.Errorf("ACK seq mismatch: 0x%X want 0x%X", gotSeq, curSeq)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("Run returned err: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}

	// Magic helper to silence unused import warning when frame helpers are not invoked here.
	_ = frame.MagicBSV
}

func TestRun_BindFailure(t *testing.T) {
	// Try to bind on a port that's already in use.
	probe, err := net.ListenPacket("udp6", "[::1]:0")
	if err != nil {
		t.Skipf("udp6 unavailable: %v", err)
	}
	port := probe.LocalAddr().(*net.UDPAddr).Port
	defer func() { _ = probe.Close() }()

	s := New(port, newMockCache(), permissiveRL(), nil, &mockRetransmitter{}, 1, false)
	s.SetBindAddr("::1")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.Run(ctx); err == nil {
		t.Error("expected bind error")
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

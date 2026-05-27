package server

import (
	"encoding/binary"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/lightwebinc/retry-endpoint/ratelimit"
	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/shard"
)

// ── test doubles ─────────────────────────────────────────────────────────────

type mockCache struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMockCache() *mockCache { return &mockCache{data: make(map[string][]byte)} }

func (m *mockCache) Store(key []byte, value []byte, _ time.Duration) error {
	m.mu.Lock()
	m.data[string(key)] = append([]byte{}, value...)
	m.mu.Unlock()
	return nil
}

func (m *mockCache) Retrieve(key []byte) ([]byte, error) {
	m.mu.Lock()
	v := m.data[string(key)]
	m.mu.Unlock()
	return v, nil
}

func (m *mockCache) Delete(key []byte) error {
	m.mu.Lock()
	delete(m.data, string(key))
	m.mu.Unlock()
	return nil
}

func (m *mockCache) Close() error { return nil }

type mockRetransmitter struct {
	mu      sync.Mutex
	called  bool
	lastRaw []byte
}

func (m *mockRetransmitter) Retransmit(raw []byte, _ [32]byte) error {
	m.mu.Lock()
	m.called = true
	m.lastRaw = append([]byte{}, raw...)
	m.mu.Unlock()
	return nil
}

type mockPacketConn struct {
	mu      sync.Mutex
	written [][]byte
}

func (m *mockPacketConn) WriteTo(b []byte, _ net.Addr) (int, error) {
	m.mu.Lock()
	m.written = append(m.written, append([]byte{}, b...))
	m.mu.Unlock()
	return len(b), nil
}

func (m *mockPacketConn) ReadFrom([]byte) (int, net.Addr, error) { return 0, nil, nil }
func (m *mockPacketConn) Close() error                           { return nil }
func (m *mockPacketConn) LocalAddr() net.Addr                    { return nil }
func (m *mockPacketConn) SetDeadline(_ time.Time) error          { return nil }
func (m *mockPacketConn) SetReadDeadline(_ time.Time) error      { return nil }
func (m *mockPacketConn) SetWriteDeadline(_ time.Time) error     { return nil }

// ── test helpers ──────────────────────────────────────────────────────────────

func permissiveRL() *ratelimit.Limiter {
	return ratelimit.New(ratelimit.Config{
		IPRate:         1e9,
		IPBurst:        1_000_000,
		ChainRate:      1_000_000,
		ChainWindow:    time.Second,
		SequenceMax:    1_000_000,
		SequenceWindow: time.Second,
		GroupRate:      1e9,
		GroupBurst:     1_000_000,
	})
}

func buildCacheFrame(t *testing.T, seqNum uint64) []byte {
	t.Helper()
	f := &frame.Frame{SeqNum: seqNum}
	f.TxID[0] = 0xAB
	payload := []byte("test-payload")
	buf := make([]byte, frame.HeaderSize+len(payload))
	n, err := frame.Encode(f, buf)
	if err != nil {
		t.Fatalf("frame.Encode: %v", err)
	}
	return buf[:n]
}

func storeFrame(c *mockCache, hashKey, seqNum uint64, raw []byte) {
	var key [16]byte
	binary.BigEndian.PutUint64(key[0:8], hashKey)
	binary.BigEndian.PutUint64(key[8:16], seqNum)
	_ = c.Store(key[:], raw, time.Minute)
}

func buildNACK(msgType byte, hashKey, startSeq, endSeq uint64) []byte {
	buf := make([]byte, NACKSize)
	binary.BigEndian.PutUint32(buf[0:4], frame.MagicBSV)
	binary.BigEndian.PutUint16(buf[4:6], frame.ProtoVer)
	buf[6] = msgType
	buf[7] = 0x00 // Flags (reserved)
	binary.BigEndian.PutUint64(buf[8:16], hashKey)
	binary.BigEndian.PutUint64(buf[16:24], startSeq)
	binary.BigEndian.PutUint64(buf[24:32], endSeq)
	return buf
}

func TestNACKSize_is64(t *testing.T) {
	if NACKSize != 64 {
		t.Errorf("NACKSize = %d, want 64", NACKSize)
	}
}

func TestResponseSize_is16(t *testing.T) {
	if ResponseSize != 16 {
		t.Errorf("ResponseSize = %d, want 16", ResponseSize)
	}
}

func TestValidateNACK_valid(t *testing.T) {
	buf := buildNACK(msgTypeNACK, 0xDEADBEEFCAFEBABE, 0x1234, 0x1234)
	if err := validateNACK(buf); err != nil {
		t.Fatalf("expected valid NACK, got error: %v", err)
	}
}

func TestValidateNACK_badMagic(t *testing.T) {
	buf := buildNACK(msgTypeNACK, 0, 42, 42)
	buf[0] = 0xFF
	if err := validateNACK(buf); err == nil {
		t.Fatal("expected error for bad magic")
	}
}

func TestValidateNACK_badMsgType(t *testing.T) {
	buf := buildNACK(0xFF, 0, 42, 42)
	if err := validateNACK(buf); err == nil {
		t.Fatal("expected error for invalid msg type")
	}
}

func TestValidateNACK_tooShort(t *testing.T) {
	if err := validateNACK(make([]byte, NACKSize-1)); err == nil {
		t.Fatal("expected error for short datagram")
	}
}

func TestValidateNACK_parsesHashKeyAndStartSeq(t *testing.T) {
	const wantHashKey uint64 = 0xAABBCCDDEEFF0011
	const wantStartSeq uint64 = 0x1122334455667788
	buf := buildNACK(msgTypeNACK, wantHashKey, wantStartSeq, wantStartSeq)
	if err := validateNACK(buf); err != nil {
		t.Fatalf("validate: %v", err)
	}
	gotHK := binary.BigEndian.Uint64(buf[8:16])
	if gotHK != wantHashKey {
		t.Errorf("HashKey = 0x%016X, want 0x%016X", gotHK, wantHashKey)
	}
	gotSS := binary.BigEndian.Uint64(buf[16:24])
	if gotSS != wantStartSeq {
		t.Errorf("StartSeq = 0x%016X, want 0x%016X", gotSS, wantStartSeq)
	}
}

// ── processNACK behavioural tests ─────────────────────────────────────────────

func TestProcessNACK_CacheHit_RetransmitsAndACKs(t *testing.T) {
	mc := newMockCache()
	rt := &mockRetransmitter{}
	conn := &mockPacketConn{}

	const hashKey uint64 = 0x1111000000000001
	const seqNum uint64 = 0xDEADBEEFCAFE0001
	raw := buildCacheFrame(t, seqNum)
	storeFrame(mc, hashKey, seqNum, raw)

	s := New(9300, mc, permissiveRL(), nil, rt, 1, false)
	src := &net.UDPAddr{IP: net.IPv6loopback, Port: 12345}
	s.processNACK(conn, 0, buildNACK(msgTypeNACK, hashKey, seqNum, seqNum), src)

	if !rt.called {
		t.Error("retransmitter not called on cache hit")
	}
	if len(conn.written) != 1 {
		t.Fatalf("expected 1 ACK response, got %d", len(conn.written))
	}
	if conn.written[0][6] != msgTypeACK {
		t.Errorf("response[6] = 0x%02X, want ACK (0x%02X)", conn.written[0][6], msgTypeACK)
	}
	gotSeq := binary.BigEndian.Uint64(conn.written[0][8:16])
	if gotSeq != seqNum {
		t.Errorf("ACK carries SeqNum = %d, want %d", gotSeq, seqNum)
	}
}

func TestProcessNACK_CacheMiss_SendsMISS(t *testing.T) {
	mc := newMockCache() // empty cache
	rt := &mockRetransmitter{}
	conn := &mockPacketConn{}

	s := New(9300, mc, permissiveRL(), nil, rt, 1, false)
	src := &net.UDPAddr{IP: net.IPv6loopback, Port: 12345}
	s.processNACK(conn, 0, buildNACK(msgTypeNACK, 0xCAFE, 0xBABE, 0xBABE), src)

	if rt.called {
		t.Error("retransmitter must not be called on cache miss")
	}
	if len(conn.written) != 1 {
		t.Fatalf("expected 1 MISS response, got %d", len(conn.written))
	}
	if conn.written[0][6] != msgTypeMISS {
		t.Errorf("response[6] = 0x%02X, want MISS (0x%02X)", conn.written[0][6], msgTypeMISS)
	}
}

func TestProcessNACK_SuppressACK_NoResponseOnHit(t *testing.T) {
	mc := newMockCache()
	rt := &mockRetransmitter{}
	conn := &mockPacketConn{}

	const seqNum uint64 = 0xDEAD
	storeFrame(mc, 0, seqNum, buildCacheFrame(t, seqNum))

	s := New(9300, mc, permissiveRL(), nil, rt, 1, false)
	s.SetSuppressACK(true)
	src := &net.UDPAddr{IP: net.IPv6loopback, Port: 12345}
	s.processNACK(conn, 0, buildNACK(msgTypeNACK, 0, seqNum, seqNum), src)

	if !rt.called {
		t.Error("retransmitter must still fire when suppressACK=true")
	}
	if len(conn.written) != 0 {
		t.Errorf("expected no response with suppressACK=true, got %d", len(conn.written))
	}
}

func TestProcessNACK_SuppressMISS_NoResponseOnMiss(t *testing.T) {
	mc := newMockCache() // empty → miss
	rt := &mockRetransmitter{}
	conn := &mockPacketConn{}

	s := New(9300, mc, permissiveRL(), nil, rt, 1, false)
	s.SetSuppressMISS(true)
	src := &net.UDPAddr{IP: net.IPv6loopback, Port: 12345}
	s.processNACK(conn, 0, buildNACK(msgTypeNACK, 0, 0x1234, 0x1234), src)

	if len(conn.written) != 0 {
		t.Errorf("expected no response with suppressMISS=true, got %d", len(conn.written))
	}
}

func TestProcessNACK_InvalidDatagram_SilentDrop(t *testing.T) {
	mc := newMockCache()
	rt := &mockRetransmitter{}
	conn := &mockPacketConn{}

	s := New(9300, mc, permissiveRL(), nil, rt, 1, false)
	bad := buildNACK(msgTypeNACK, 0, 42, 42)
	bad[0] = 0x00 // corrupt magic
	src := &net.UDPAddr{IP: net.IPv6loopback, Port: 12345}
	s.processNACK(conn, 0, bad, src)

	if rt.called || len(conn.written) != 0 {
		t.Error("invalid NACK must be silently dropped")
	}
}

func TestProcessNACK_NilSrc_RetransmitsNoResponse(t *testing.T) {
	mc := newMockCache()
	rt := &mockRetransmitter{}
	conn := &mockPacketConn{}

	const seqNum uint64 = 0xABCD1234
	storeFrame(mc, 0, seqNum, buildCacheFrame(t, seqNum))

	s := New(9300, mc, permissiveRL(), nil, rt, 1, false)
	s.processNACK(conn, 0, buildNACK(msgTypeNACK, 0, seqNum, seqNum), nil)

	if !rt.called {
		t.Error("retransmitter must fire even with nil src")
	}
	if len(conn.written) != 0 {
		t.Errorf("no response should be sent when src is nil (no return address), got %d", len(conn.written))
	}
}

func TestProcessNACK_GroupLimit_SkipsRetransmit_StillACKs(t *testing.T) {
	mc := newMockCache()
	rt := &mockRetransmitter{}
	conn := &mockPacketConn{}

	const seqNum uint64 = 0xCAFE0001
	raw := buildCacheFrame(t, seqNum)
	storeFrame(mc, 0, seqNum, raw)

	// Build a tight group limiter: burst=1, rate=1/s.
	tightRL := ratelimit.New(ratelimit.Config{
		IPRate:      1e9,
		IPBurst:     1_000_000,
		ChainRate:   1e9,
		ChainWindow: time.Second,
		SequenceMax: 1_000_000,
		GroupRate:   1,
		GroupBurst:  1,
	})

	engine := shard.New(0xFF05, shard.DefaultGroupID, 2)
	s := New(9300, mc, tightRL, nil, rt, 1, false)
	s.SetShardEngine(engine)
	src := &net.UDPAddr{IP: net.IPv6loopback, Port: 12345}

	// First request consumes the burst of 1 — retransmit fires, ACK sent.
	s.processNACK(conn, 0, buildNACK(msgTypeNACK, 0, seqNum, seqNum), src)
	if !rt.called {
		t.Fatal("first request: retransmitter must fire")
	}
	if len(conn.written) != 1 || conn.written[0][6] != msgTypeACK {
		t.Fatalf("first request: expected 1 ACK, got %d responses", len(conn.written))
	}

	// Reset for second call.
	rt.called = false

	// Second request: group limiter exhausted — retransmit must be skipped
	// but ACK must still be sent (frame exists).
	s.processNACK(conn, 0, buildNACK(msgTypeNACK, 0, seqNum, seqNum), src)
	if rt.called {
		t.Error("second request: retransmitter must NOT be called when group throttled")
	}
	if len(conn.written) != 2 || conn.written[1][6] != msgTypeACK {
		t.Fatalf("second request: expected ACK even when group throttled, got %d responses", len(conn.written)-1)
	}
}

func TestProcessNACK_ChainLimit_DropsRequest(t *testing.T) {
	mc := newMockCache()
	rt := &mockRetransmitter{}
	conn := &mockPacketConn{}

	const seqNum uint64 = 0xCAFE0002
	const hashKey uint64 = 0x1234567890ABCDEF
	storeFrame(mc, hashKey, seqNum, buildCacheFrame(t, seqNum))

	// Tight chain limiter: 1 request per minute per (ip, hashKey).
	tightRL := ratelimit.New(ratelimit.Config{
		IPRate:      1e9,
		IPBurst:     1_000_000,
		ChainRate:   1,
		ChainWindow: time.Minute,
		SequenceMax: 1_000_000,
		GroupRate:   1e9,
		GroupBurst:  1_000_000,
	})

	s := New(9300, mc, tightRL, nil, rt, 1, false)
	src := &net.UDPAddr{IP: net.IPv6loopback, Port: 12345}

	// First call with hashKey: passes.
	s.processNACK(conn, 0, buildNACK(msgTypeNACK, hashKey, seqNum, seqNum), src)
	if !rt.called {
		t.Fatal("first request must pass")
	}

	// Second call: chain limit exhausted → silently dropped.
	rt.called = false
	s.processNACK(conn, 0, buildNACK(msgTypeNACK, hashKey, seqNum, seqNum), src)
	if rt.called {
		t.Error("second request must be dropped (chain rate limited)")
	}
	if len(conn.written) != 1 {
		t.Errorf("chain-limited request must produce no additional response; total=%d", len(conn.written))
	}
}

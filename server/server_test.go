package server

import (
	"encoding/binary"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/lightwebinc/bitcoin-retry-endpoint/ratelimit"
	"github.com/lightwebinc/bitcoin-shard-common/frame"
	"github.com/lightwebinc/bitcoin-shard-common/shard"
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
		ChainRate:      1e9,
		ChainWindow:    time.Second,
		SequenceMax:    1_000_000,
		SequenceWindow: time.Second,
		GroupRate:      1e9,
		GroupBurst:     1_000_000,
	})
}

func buildCacheFrame(t *testing.T, curSeq uint64) []byte {
	t.Helper()
	f := &frame.Frame{CurSeq: curSeq}
	f.TxID[0] = 0xAB
	payload := []byte("test-payload")
	buf := make([]byte, frame.HeaderSize+len(payload))
	n, err := frame.Encode(f, buf)
	if err != nil {
		t.Fatalf("frame.Encode: %v", err)
	}
	return buf[:n]
}

func storePrimary(c *mockCache, curSeq uint64, raw []byte) {
	storePrimarySub(c, [32]byte{}, curSeq, raw)
}

func storePrimarySub(c *mockCache, sub [32]byte, curSeq uint64, raw []byte) {
	var pk [41]byte
	pk[0] = lookupByCurSeq
	copy(pk[1:33], sub[:])
	binary.BigEndian.PutUint64(pk[33:41], curSeq)
	_ = c.Store(pk[:], raw, time.Minute)
}

func storeSecondary(c *mockCache, prevSeq, curSeq uint64) {
	storeSecondarySub(c, [32]byte{}, prevSeq, curSeq)
}

func storeSecondarySub(c *mockCache, sub [32]byte, prevSeq, curSeq uint64) {
	var sk [41]byte
	sk[0] = lookupByPrevSeq
	copy(sk[1:33], sub[:])
	binary.BigEndian.PutUint64(sk[33:41], prevSeq)
	var val [8]byte
	binary.BigEndian.PutUint64(val[:], curSeq)
	_ = c.Store(sk[:], val[:], time.Minute)
}

func buildNACK(msgType byte, lookupType byte, lookupSeq uint64) []byte {
	return buildNACKWithChain(msgType, lookupType, lookupSeq, 0)
}

func buildNACKWithChain(msgType byte, lookupType byte, lookupSeq uint64, chainID uint64) []byte {
	buf := make([]byte, NACKSize)
	binary.BigEndian.PutUint32(buf[0:4], frame.MagicBSV)
	binary.BigEndian.PutUint16(buf[4:6], frame.ProtoVer)
	buf[6] = msgType
	buf[7] = lookupType
	binary.BigEndian.PutUint64(buf[8:16], lookupSeq)
	binary.BigEndian.PutUint64(buf[16:24], chainID) // ChainID
	return buf
}

func TestNACKSize_is56(t *testing.T) {
	if NACKSize != 56 {
		t.Errorf("NACKSize = %d, want 56", NACKSize)
	}
}

func TestResponseSize_is16(t *testing.T) {
	if ResponseSize != 16 {
		t.Errorf("ResponseSize = %d, want 16", ResponseSize)
	}
}

func TestValidateNACK_valid_byCurSeq(t *testing.T) {
	buf := buildNACK(msgTypeNACK, lookupByCurSeq, 0xDEADBEEFCAFEBABE)
	if err := validateNACK(buf); err != nil {
		t.Fatalf("expected valid NACK, got error: %v", err)
	}
}

func TestValidateNACK_valid_byPrevSeq(t *testing.T) {
	buf := buildNACK(msgTypeNACK, lookupByPrevSeq, 0x0102030405060708)
	if err := validateNACK(buf); err != nil {
		t.Fatalf("expected valid NACK, got error: %v", err)
	}
}

func TestValidateNACK_badMagic(t *testing.T) {
	buf := buildNACK(msgTypeNACK, lookupByCurSeq, 42)
	buf[0] = 0xFF
	if err := validateNACK(buf); err == nil {
		t.Fatal("expected error for bad magic")
	}
}

func TestValidateNACK_badMsgType(t *testing.T) {
	buf := buildNACK(0xFF, lookupByCurSeq, 42)
	if err := validateNACK(buf); err == nil {
		t.Fatal("expected error for invalid msg type")
	}
}

func TestValidateNACK_tooShort(t *testing.T) {
	if err := validateNACK(make([]byte, NACKSize-1)); err == nil {
		t.Fatal("expected error for short datagram")
	}
}

func TestValidateNACK_parsesLookupSeq(t *testing.T) {
	const want uint64 = 0xAABBCCDDEEFF0011
	buf := buildNACK(msgTypeNACK, lookupByCurSeq, want)
	if err := validateNACK(buf); err != nil {
		t.Fatalf("validate: %v", err)
	}
	got := binary.BigEndian.Uint64(buf[8:16])
	if got != want {
		t.Errorf("LookupSeq = 0x%016X, want 0x%016X", got, want)
	}
}

// ── processNACK behavioural tests ─────────────────────────────────────────────

func TestProcessNACK_CacheHit_ByCurSeq_RetransmitsAndACKs(t *testing.T) {
	mc := newMockCache()
	rt := &mockRetransmitter{}
	conn := &mockPacketConn{}

	const curSeq uint64 = 0xDEADBEEFCAFE0001
	raw := buildCacheFrame(t, curSeq)
	storePrimary(mc, curSeq, raw)

	s := New(9300, mc, permissiveRL(), nil, rt, 1, false)
	src := &net.UDPAddr{IP: net.IPv6loopback, Port: 12345}
	s.processNACK(conn, 0, buildNACK(msgTypeNACK, lookupByCurSeq, curSeq), src)

	if !rt.called {
		t.Error("retransmitter not called on cache hit (byCurSeq)")
	}
	if len(conn.written) != 1 {
		t.Fatalf("expected 1 ACK response, got %d", len(conn.written))
	}
	if conn.written[0][6] != msgTypeACK {
		t.Errorf("response[6] = 0x%02X, want ACK (0x%02X)", conn.written[0][6], msgTypeACK)
	}
	gotSeq := binary.BigEndian.Uint64(conn.written[0][8:16])
	if gotSeq != curSeq {
		t.Errorf("ACK carries CurSeq = %d, want %d", gotSeq, curSeq)
	}
}

func TestProcessNACK_CacheMiss_ByCurSeq_SendsMISS(t *testing.T) {
	mc := newMockCache() // empty cache
	rt := &mockRetransmitter{}
	conn := &mockPacketConn{}

	s := New(9300, mc, permissiveRL(), nil, rt, 1, false)
	src := &net.UDPAddr{IP: net.IPv6loopback, Port: 12345}
	s.processNACK(conn, 0, buildNACK(msgTypeNACK, lookupByCurSeq, 0xCAFEBABE), src)

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

func TestProcessNACK_CacheHit_ByPrevSeq_RetransmitsAndACKs(t *testing.T) {
	mc := newMockCache()
	rt := &mockRetransmitter{}
	conn := &mockPacketConn{}

	const prevSeq uint64 = 0xAAAA000000000001
	const curSeq uint64 = 0xBBBB000000000001
	raw := buildCacheFrame(t, curSeq)
	storePrimary(mc, curSeq, raw)
	storeSecondary(mc, prevSeq, curSeq)

	s := New(9300, mc, permissiveRL(), nil, rt, 1, false)
	src := &net.UDPAddr{IP: net.IPv6loopback, Port: 12345}
	s.processNACK(conn, 0, buildNACK(msgTypeNACK, lookupByPrevSeq, prevSeq), src)

	if !rt.called {
		t.Error("retransmitter not called on cache hit (byPrevSeq)")
	}
	if len(conn.written) != 1 {
		t.Fatalf("expected 1 ACK response, got %d", len(conn.written))
	}
	if conn.written[0][6] != msgTypeACK {
		t.Errorf("response[6] = 0x%02X, want ACK (0x%02X)", conn.written[0][6], msgTypeACK)
	}
}

func TestProcessNACK_CacheMiss_ByPrevSeq_NoSecondaryEntry_SendsMISS(t *testing.T) {
	mc := newMockCache() // no secondary index entry
	rt := &mockRetransmitter{}
	conn := &mockPacketConn{}

	s := New(9300, mc, permissiveRL(), nil, rt, 1, false)
	src := &net.UDPAddr{IP: net.IPv6loopback, Port: 12345}
	s.processNACK(conn, 0, buildNACK(msgTypeNACK, lookupByPrevSeq, 0x1234), src)

	if rt.called {
		t.Error("retransmitter must not be called when secondary index has no entry")
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

	const curSeq uint64 = 0xDEAD
	storePrimary(mc, curSeq, buildCacheFrame(t, curSeq))

	s := New(9300, mc, permissiveRL(), nil, rt, 1, false)
	s.SetSuppressACK(true)
	src := &net.UDPAddr{IP: net.IPv6loopback, Port: 12345}
	s.processNACK(conn, 0, buildNACK(msgTypeNACK, lookupByCurSeq, curSeq), src)

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
	s.processNACK(conn, 0, buildNACK(msgTypeNACK, lookupByCurSeq, 0x1234), src)

	if len(conn.written) != 0 {
		t.Errorf("expected no response with suppressMISS=true, got %d", len(conn.written))
	}
}

func TestProcessNACK_InvalidDatagram_SilentDrop(t *testing.T) {
	mc := newMockCache()
	rt := &mockRetransmitter{}
	conn := &mockPacketConn{}

	s := New(9300, mc, permissiveRL(), nil, rt, 1, false)
	bad := buildNACK(msgTypeNACK, lookupByCurSeq, 42)
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

	const curSeq uint64 = 0xABCD1234
	storePrimary(mc, curSeq, buildCacheFrame(t, curSeq))

	s := New(9300, mc, permissiveRL(), nil, rt, 1, false)
	s.processNACK(conn, 0, buildNACK(msgTypeNACK, lookupByCurSeq, curSeq), nil)

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

	const curSeq uint64 = 0xCAFE0001
	raw := buildCacheFrame(t, curSeq)
	storePrimary(mc, curSeq, raw)

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
	s.processNACK(conn, 0, buildNACK(msgTypeNACK, lookupByCurSeq, curSeq), src)
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
	s.processNACK(conn, 0, buildNACK(msgTypeNACK, lookupByCurSeq, curSeq), src)
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

	const curSeq uint64 = 0xCAFE0002
	const chainID uint64 = 0x1234567890ABCDEF
	storePrimary(mc, curSeq, buildCacheFrame(t, curSeq))

	// Tight chain limiter: 1 request per minute per (ip, chainID).
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

	// First call with chainID: passes.
	s.processNACK(conn, 0, buildNACKWithChain(msgTypeNACK, lookupByCurSeq, curSeq, chainID), src)
	if !rt.called {
		t.Fatal("first chain request must pass")
	}

	// Second call: chain limit exhausted → silently dropped.
	rt.called = false
	s.processNACK(conn, 0, buildNACKWithChain(msgTypeNACK, lookupByCurSeq, curSeq, chainID), src)
	if rt.called {
		t.Error("second chain request must be dropped (chain rate limited)")
	}
	if len(conn.written) != 1 {
		t.Errorf("chain-limited request must produce no additional response; total=%d", len(conn.written))
	}
}

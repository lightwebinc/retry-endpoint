package retransmit

import (
	"net"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/lightwebinc/bitcoin-shard-common/shard"

	"github.com/lightwebinc/bitcoin-retry-endpoint/cache/redis"
)

func TestNew(t *testing.T) {
	eng := shard.New(0xFF05, shard.DefaultGroupID, 2)
	r := New(eng, nil, 9001, time.Second, nil, nil, false)
	if r.engine != eng {
		t.Error("engine not set")
	}
	if r.egressPort != 9001 {
		t.Errorf("egressPort = %d", r.egressPort)
	}
}

func TestBuildDedupKey(t *testing.T) {
	r := New(nil, nil, 0, 0, nil, nil, false)

	// Too short → nil.
	if got := r.buildDedupKey(make([]byte, 10)); got != nil {
		t.Errorf("expected nil for short frame, got %v", got)
	}
	// Valid: copy bytes 48..55.
	raw := make([]byte, 56)
	for i := 48; i < 56; i++ {
		raw[i] = byte(i)
	}
	got := r.buildDedupKey(raw)
	if len(got) != 8 {
		t.Fatalf("len=%d", len(got))
	}
	for i, b := range got {
		if b != byte(48+i) {
			t.Errorf("byte[%d] = 0x%02X, want 0x%02X", i, b, 48+i)
		}
	}
}

func TestClose_NoSockets(t *testing.T) {
	r := New(nil, nil, 0, 0, nil, nil, false)
	if err := r.Close(); err != nil {
		t.Errorf("close empty: %v", err)
	}
}

func TestRetransmit_DedupSuppresses(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rc, err := redis.New(mr.Addr(), "test:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rc.Close() }()

	eng := shard.New(0xFF05, shard.DefaultGroupID, 2)
	r := New(eng, nil, 9001, time.Minute, rc, nil, false)
	// No egress sockets opened — but dedup path runs first and the second call
	// must short-circuit before reaching the (empty) socket loop.

	raw := make([]byte, 100)
	raw[48] = 0xAA // CurSeq[0]
	raw[55] = 0xBB

	// First call: SET NX succeeds → proceeds to socket loop (empty → no error).
	if err := r.Retransmit(raw, [32]byte{}); err != nil {
		t.Fatalf("first: %v", err)
	}
	// Second call with same CurSeq: SET NX returns false → returns nil early.
	if err := r.Retransmit(raw, [32]byte{}); err != nil {
		t.Errorf("dedup second: %v", err)
	}
}

func TestOpen_NoIfaces(t *testing.T) {
	r := New(nil, nil, 0, 0, nil, nil, false)
	if err := r.Open(); err != nil {
		t.Errorf("Open with no ifaces should succeed, got %v", err)
	}
}

func TestOpen_LoopbackIface(t *testing.T) {
	ifs, err := net.Interfaces()
	if err != nil {
		t.Fatal(err)
	}
	var lo *net.Interface
	for i := range ifs {
		if ifs[i].Flags&net.FlagLoopback != 0 {
			lo = &ifs[i]
			break
		}
	}
	if lo == nil {
		t.Skip("no loopback")
	}
	eng := shard.New(0xFF05, shard.DefaultGroupID, 2)
	r := New(eng, []*net.Interface{lo}, 9001, 0, nil, nil, false)
	if err := r.Open(); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

package retransmit

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	scache "github.com/lightwebinc/shard-common/cache"
	"github.com/lightwebinc/shard-common/shard"

	"github.com/lightwebinc/retry-endpoint/cache"
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
	// Valid: copy bytes 40..55 — HashKey followed by SeqNum.
	raw := make([]byte, 56)
	for i := 40; i < 56; i++ {
		raw[i] = byte(i)
	}
	got := r.buildDedupKey(raw)
	if len(got) != 16 {
		t.Fatalf("len=%d, want 16 (HashKey ∥ SeqNum)", len(got))
	}
	for i, b := range got {
		if b != byte(40+i) {
			t.Errorf("byte[%d] = 0x%02X, want 0x%02X", i, b, 40+i)
		}
	}
}

// TestBuildDedupKey_DistinctFlowsSameSeq pins the defect the key width exists to
// prevent: two different flows repairing the same SeqNum must not collide. Every
// flow's counter starts at 1, so a SeqNum-only key made low sequence numbers
// collide across the fabric and suppressed live repairs.
func TestBuildDedupKey_DistinctFlowsSameSeq(t *testing.T) {
	r := New(nil, nil, 0, 0, nil, nil, false)

	flowA := make([]byte, 56)
	binary.BigEndian.PutUint64(flowA[40:48], 0xAAAAAAAAAAAAAAAA) // HashKey A
	binary.BigEndian.PutUint64(flowA[48:56], 1)                  // SeqNum 1

	flowB := make([]byte, 56)
	binary.BigEndian.PutUint64(flowB[40:48], 0xBBBBBBBBBBBBBBBB) // HashKey B
	binary.BigEndian.PutUint64(flowB[48:56], 1)                  // same SeqNum

	if bytes.Equal(r.buildDedupKey(flowA), r.buildDedupKey(flowB)) {
		t.Error("distinct flows at the same SeqNum share a dedup key: a repair for one flow would suppress the other")
	}

	// Same flow, same SeqNum → same key (dedup must still engage).
	if !bytes.Equal(r.buildDedupKey(flowA), r.buildDedupKey(append([]byte(nil), flowA...))) {
		t.Error("identical frames must share a dedup key")
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
	backend, err := scache.Open(context.Background(), scache.Config{Backend: scache.BackendRedis, RedisAddr: mr.Addr()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = backend.Close() }()
	rc := cache.NewStore(backend, "test:", time.Second)

	eng := shard.New(0xFF05, shard.DefaultGroupID, 2)
	r := New(eng, nil, 9001, time.Minute, rc, nil, false)
	// No egress sockets opened — but dedup path runs first and the second call
	// must short-circuit before reaching the (empty) socket loop.

	raw := make([]byte, 100)
	raw[48] = 0xAA // SeqNum[0]
	raw[55] = 0xBB

	// First call: SET NX succeeds → proceeds to socket loop (empty → no error).
	if err := r.Retransmit(raw, [32]byte{}); err != nil {
		t.Fatalf("first: %v", err)
	}
	// Second call with the same (HashKey, SeqNum): SET NX returns false, and the
	// suppression is reported distinctly so the caller cannot ACK it as a send.
	if err := r.Retransmit(raw, [32]byte{}); !errors.Is(err, ErrDedupSuppressed) {
		t.Errorf("dedup second: err = %v, want ErrDedupSuppressed", err)
	}
}

func TestOpen_NoIfaces(t *testing.T) {
	r := New(nil, nil, 0, 0, nil, nil, false)
	if err := r.Open(); err != nil {
		t.Errorf("Open with no ifaces should succeed, got %v", err)
	}
	_ = r.Close()
}

func TestRetransmitUnicast_DeliversFrame(t *testing.T) {
	// A listening UDP socket stands in for the requester.
	conn, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.IPv6loopback})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = conn.Close() }()
	dst := conn.LocalAddr().(*net.UDPAddr)

	r := New(nil, nil, 0, 0, nil, nil, false)
	if err := r.Open(); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = r.Close() }()

	want := []byte("frame-bytes-verbatim")
	if err := r.RetransmitUnicast(want, dst); err != nil {
		t.Fatalf("unicast: %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1500)
	n, _, err := conn.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf[:n]) != string(want) {
		t.Errorf("got %q, want %q", buf[:n], want)
	}
}

func TestRetransmitUnicast_Errors(t *testing.T) {
	r := New(nil, nil, 0, 0, nil, nil, false)
	// Socket not open yet.
	if err := r.RetransmitUnicast([]byte("x"), &net.UDPAddr{IP: net.IPv6loopback, Port: 1}); err == nil {
		t.Error("expected error when socket not open")
	}
	if err := r.Open(); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = r.Close() }()
	// Nil dst.
	if err := r.RetransmitUnicast([]byte("x"), nil); err == nil {
		t.Error("expected error for nil dst")
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

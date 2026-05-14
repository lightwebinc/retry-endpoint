package ingress

import (
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/lightwebinc/bitcoin-shard-common/frame"
)

// mockCache captures Store calls for assertion.
type mockCache struct {
	mu     sync.Mutex
	stores []storeCall
}

type storeCall struct {
	key []byte
	val []byte
	ttl time.Duration
}

func (m *mockCache) Store(key, value []byte, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := make([]byte, len(key))
	copy(k, key)
	v := make([]byte, len(value))
	copy(v, value)
	m.stores = append(m.stores, storeCall{key: k, val: v, ttl: ttl})
	return nil
}

func (m *mockCache) Retrieve([]byte) ([]byte, error) { return nil, nil }
func (m *mockCache) Delete([]byte) error             { return nil }
func (m *mockCache) Close() error                    { return nil }

func (m *mockCache) storeCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.stores)
}

func (m *mockCache) storeAt(i int) storeCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stores[i]
}

// buildRaw encodes a BRC-124/BRC-128 frame with the given PrevSeq, CurSeq, and payload.
func buildRaw(t *testing.T, prevSeq, curSeq uint64, payload []byte) []byte {
	t.Helper()
	f := &frame.Frame{
		Version: frame.FrameVerV2,
		PrevSeq: prevSeq,
		CurSeq:  curSeq,
		Payload: payload,
	}
	f.TxID[0] = 0xAB
	buf := make([]byte, frame.HeaderSize+len(payload))
	n, err := frame.Encode(f, buf)
	if err != nil {
		t.Fatalf("frame.Encode: %v", err)
	}
	return buf[:n]
}

func newTestWorker(mc *mockCache) *Worker {
	return &Worker{
		cache: mc,
		ttl:   60 * time.Second,
	}
}

func TestProcessFrame_DualIndex(t *testing.T) {
	mc := &mockCache{}
	w := newTestWorker(mc)

	prevSeq := uint64(0xAABBCCDDEEFF0011)
	curSeq := uint64(0x1122334455667788)
	raw := buildRaw(t, prevSeq, curSeq, []byte("tx-payload"))

	w.processFrame(raw)

	if mc.storeCount() != 2 {
		t.Fatalf("expected 2 Store calls, got %d", mc.storeCount())
	}

	// Primary: key = 0x01 || SubtreeID(32) || CurSeq → raw frame
	pk := mc.storeAt(0)
	if len(pk.key) != 41 {
		t.Errorf("primary key len = %d, want 41", len(pk.key))
	}
	if pk.key[0] != 0x01 {
		t.Errorf("primary key prefix = 0x%02X, want 0x01", pk.key[0])
	}
	gotCurSeq := binary.BigEndian.Uint64(pk.key[33:41])
	if gotCurSeq != curSeq {
		t.Errorf("primary key CurSeq = 0x%016X, want 0x%016X", gotCurSeq, curSeq)
	}
	if len(pk.val) != len(raw) {
		t.Errorf("primary value len = %d, want %d", len(pk.val), len(raw))
	}

	// Secondary: key = 0x00 || SubtreeID(32) || PrevSeq → CurSeq (8 bytes)
	sk := mc.storeAt(1)
	if len(sk.key) != 41 {
		t.Errorf("secondary key len = %d, want 41", len(sk.key))
	}
	if sk.key[0] != 0x00 {
		t.Errorf("secondary key prefix = 0x%02X, want 0x00", sk.key[0])
	}
	gotPrevSeq := binary.BigEndian.Uint64(sk.key[33:41])
	if gotPrevSeq != prevSeq {
		t.Errorf("secondary key PrevSeq = 0x%016X, want 0x%016X", gotPrevSeq, prevSeq)
	}
	if len(sk.val) != 8 {
		t.Fatalf("secondary value len = %d, want 8", len(sk.val))
	}
	gotPtr := binary.BigEndian.Uint64(sk.val)
	if gotPtr != curSeq {
		t.Errorf("secondary value CurSeq = 0x%016X, want 0x%016X", gotPtr, curSeq)
	}
}

func TestProcessFrame_ZeroCurSeq_Skip(t *testing.T) {
	mc := &mockCache{}
	w := newTestWorker(mc)

	raw := buildRaw(t, 0x1234, 0, []byte("payload"))
	w.processFrame(raw)

	if mc.storeCount() != 0 {
		t.Errorf("expected 0 Store calls for CurSeq=0, got %d", mc.storeCount())
	}
}

func TestProcessFrame_ZeroPrevSeq_PrimaryOnly(t *testing.T) {
	mc := &mockCache{}
	w := newTestWorker(mc)

	curSeq := uint64(0xFFEEDDCCBBAA9988)
	raw := buildRaw(t, 0, curSeq, []byte("first-in-chain"))
	w.processFrame(raw)

	if mc.storeCount() != 1 {
		t.Fatalf("expected 1 Store call for PrevSeq=0, got %d", mc.storeCount())
	}
	pk := mc.storeAt(0)
	if pk.key[0] != 0x01 {
		t.Errorf("key prefix = 0x%02X, want 0x01 (primary)", pk.key[0])
	}
}

func TestProcessFrame_DecodeError(t *testing.T) {
	mc := &mockCache{}
	w := newTestWorker(mc)

	w.processFrame([]byte{0xFF, 0xFF}) // too short, bad magic
	if mc.storeCount() != 0 {
		t.Errorf("expected 0 Store calls for corrupt input, got %d", mc.storeCount())
	}
}

func TestProcessFrame_TTLPropagated(t *testing.T) {
	mc := &mockCache{}
	w := newTestWorker(mc)
	w.ttl = 42 * time.Second

	raw := buildRaw(t, 0x11, 0x22, nil)
	w.processFrame(raw)

	if mc.storeCount() < 1 {
		t.Fatal("expected at least 1 Store call")
	}
	if mc.storeAt(0).ttl != 42*time.Second {
		t.Errorf("Store TTL = %v, want 42s", mc.storeAt(0).ttl)
	}
}

package ingress

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/objfmt"
)

// beefTestRaw builds a stamped V9 frame (the retry endpoint only caches
// stamped frames).
func beefTestRaw(t *testing.T, hashKey, seqNum uint64) []byte {
	t.Helper()
	raw, err := objfmt.BEEFMulticastBytes(objfmt.TopicID("tm_cache"), []byte{0x01, 0x00, 0xBE, 0xEF, 0x99})
	if err != nil {
		t.Fatalf("BEEFMulticastBytes: %v", err)
	}
	binary.BigEndian.PutUint64(raw[40:48], hashKey)
	binary.BigEndian.PutUint64(raw[48:56], seqNum)
	return raw
}

func newBEEFTestWorker(mc *mockCache) *Worker {
	w := newTestWorker(mc)
	w.ttls.BEEF = 42 * time.Second
	return w
}

// TestProcessBEEFFrame_CachesUnderFlowKey proves a stamped V9 frame is
// cached under HashKey ∥ SeqNum with the BEEF TTL, and an unstamped one is
// not cached.
func TestProcessBEEFFrame_CachesUnderFlowKey(t *testing.T) {
	mc := &mockCache{}
	w := newBEEFTestWorker(mc)

	raw := beefTestRaw(t, 0xDEADBEEFCAFEBABE, 7)
	w.processFrame(raw)

	if len(mc.stores) != 1 {
		t.Fatalf("stored %d entries, want 1", len(mc.stores))
	}
	var key [16]byte
	binary.BigEndian.PutUint64(key[0:8], 0xDEADBEEFCAFEBABE)
	binary.BigEndian.PutUint64(key[8:16], 7)
	sc := mc.stores[0]
	if !bytes.Equal(sc.key, key[:]) {
		t.Errorf("cache key %x, want HashKey∥SeqNum %x", sc.key, key)
	}
	if !bytes.Equal(sc.val, raw) {
		t.Error("cached bytes not verbatim")
	}
	if sc.ttl != 42*time.Second {
		t.Errorf("cached with TTL %v, want the BEEF TTL 42s", sc.ttl)
	}

	// Unstamped (SeqNum 0) is never cached.
	mc2 := &mockCache{}
	w2 := newBEEFTestWorker(mc2)
	w2.processFrame(beefTestRaw(t, 1, 0))
	if len(mc2.stores) != 0 {
		t.Fatal("unstamped V9 frame was cached")
	}
}

// TestProcessFrame_V9NotDroppedAsDecodeError guards the dispatch order: V9
// must be handled before the generic Decode fallthrough (which rejects it).
func TestProcessFrame_V9NotDroppedAsDecodeError(t *testing.T) {
	if !frame.IsBEEFFrame(beefTestRaw(t, 1, 1)) {
		t.Fatal("fixture is not a V9 frame")
	}
	mc := &mockCache{}
	w := newBEEFTestWorker(mc)
	w.processFrame(beefTestRaw(t, 1, 1))
	if len(mc.stores) != 1 {
		t.Fatal("V9 frame fell through to decode_error path")
	}
}

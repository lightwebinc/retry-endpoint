package ingress

import (
	"encoding/binary"
	"testing"

	"github.com/lightwebinc/shard-common/bundle"
)

// A BRC-142 bundle is dispatched (via frame.IsBundle) to processBundleFrame and
// cached opaquely by its (HashKey, SeqNum) flow key, exactly like a BRC-124 frame.
func TestProcessBundleFrame_CachedByFlowKey(t *testing.T) {
	mc := &mockCache{}
	w := newTestWorker(mc)

	b := &bundle.Bundle{
		HashKey:   0xAABBCCDDEEFF0011,
		SeqNum:    0x1122334455667788,
		GroupIdx:  7,
		ShardBits: 4,
		Members:   []bundle.Member{{Tx: []byte("tx-one")}, {Tx: []byte("tx-two")}},
	}
	raw, err := b.Encode()
	if err != nil {
		t.Fatal(err)
	}

	w.processFrame(raw) // dispatches to processBundleFrame

	if mc.storeCount() != 1 {
		t.Fatalf("expected 1 Store, got %d", mc.storeCount())
	}
	entry := mc.storeAt(0)
	if got := binary.BigEndian.Uint64(entry.key[0:8]); got != b.HashKey {
		t.Errorf("key HashKey = 0x%016X, want 0x%016X", got, b.HashKey)
	}
	if got := binary.BigEndian.Uint64(entry.key[8:16]); got != b.SeqNum {
		t.Errorf("key SeqNum = 0x%016X, want 0x%016X", got, b.SeqNum)
	}
	if len(entry.val) != len(raw) {
		t.Errorf("value len = %d, want %d", len(entry.val), len(raw))
	}
}

func TestProcessBundleFrame_ZeroSeqNum_Skip(t *testing.T) {
	mc := &mockCache{}
	w := newTestWorker(mc)

	b := &bundle.Bundle{HashKey: 0x1234, SeqNum: 0, Members: []bundle.Member{{Tx: []byte("x")}}}
	raw, _ := b.Encode()
	w.processFrame(raw)

	if mc.storeCount() != 0 {
		t.Fatalf("zero-seqnum bundle must not be cached, got %d", mc.storeCount())
	}
}

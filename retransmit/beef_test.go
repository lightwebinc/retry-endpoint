package retransmit

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/shard-common/shard"
)

// TestTargetGroup_BEEF proves a cached V9 frame retransmits to the
// domain-tagged group derived from its TopicID at offset 56 — never from the
// offset-8 ContentID (which is not a shard key).
func TestTargetGroup_BEEF(t *testing.T) {
	eng := shard.New(0xFF35, shard.DefaultGroupID, 2)
	pe, err := shard.NewPlane(0xFF35, shard.DefaultGroupID, 4, shard.DomainBEEF)
	if err != nil {
		t.Fatalf("NewPlane: %v", err)
	}
	r := New(eng, nil, 9001, time.Second, nil, nil, false)
	r.SetBEEF(pe)

	topic := objfmt.TopicID("tm_retrans")
	obj := []byte{0x01, 0x00, 0xBE, 0xEF, 0x01}
	raw, err := objfmt.BEEFMulticastBytes(topic, obj)
	if err != nil {
		t.Fatalf("BEEFMulticastBytes: %v", err)
	}

	var contentID [32]byte
	copy(contentID[:], raw[8:40]) // what the server passes as "txID"

	addr, err := r.targetGroup(raw, contentID)
	if err != nil {
		t.Fatalf("targetGroup: %v", err)
	}
	gotIdx := uint32(binary.BigEndian.Uint16(addr.IP[14:16]))
	wantIdx := pe.GroupIndex(&topic)
	if gotIdx != wantIdx {
		t.Fatalf("retransmit group 0x%04X, want 0x%04X (TopicID-derived, banded)", gotIdx, wantIdx)
	}
	if gotIdx < 0x1000 || gotIdx > 0x1FFF {
		t.Fatalf("group 0x%04X outside the BEEF band", gotIdx)
	}

	// Regression: the ContentID-derived tx-plane group must NOT be used.
	if wrong := eng.GroupIndex(&contentID); gotIdx == wrong {
		t.Fatalf("group derived from ContentID (0x%04X) — wrong field", wrong)
	}
}

// TestTargetGroup_BEEFDisabled proves a V9 frame errors (rather than
// misrouting) when the plane engine is not wired.
func TestTargetGroup_BEEFDisabled(t *testing.T) {
	eng := shard.New(0xFF35, shard.DefaultGroupID, 2)
	r := New(eng, nil, 9001, time.Second, nil, nil, false)

	raw, _ := objfmt.BEEFMulticastBytes(objfmt.TopicID("tm_x"), []byte{0x01, 0x00, 0xBE, 0xEF})
	var txID [32]byte
	if _, err := r.targetGroup(raw, txID); err == nil {
		t.Fatal("expected error for V9 with plane disabled")
	}
}

// TestTargetGroup_TxUnchanged guards the existing derivations around the new
// V9 case.
func TestTargetGroup_TxUnchanged(t *testing.T) {
	eng := shard.New(0xFF35, shard.DefaultGroupID, 2)
	r := New(eng, nil, 9001, time.Second, nil, nil, false)

	var txID [32]byte
	txID[0] = 0xC0
	f := &frame.Frame{TxID: txID, Payload: []byte{0x01}}
	raw := make([]byte, frame.HeaderSize+1)
	if _, err := frame.Encode(f, raw); err != nil {
		t.Fatal(err)
	}
	addr, err := r.targetGroup(raw, txID)
	if err != nil {
		t.Fatalf("targetGroup: %v", err)
	}
	gotIdx := uint32(binary.BigEndian.Uint16(addr.IP[14:16]))
	if gotIdx != eng.GroupIndex(&txID) {
		t.Fatalf("tx group 0x%04X, want 0x%04X", gotIdx, eng.GroupIndex(&txID))
	}
}

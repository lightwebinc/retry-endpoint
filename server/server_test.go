package server

import (
	"encoding/binary"
	"testing"

	"github.com/lightwebinc/bitcoin-shard-common/frame"
)

func buildNACK(msgType byte, txID [32]byte, senderID uint32, sequenceID uint32, seqNum uint32) []byte {
	buf := make([]byte, NACKSize)
	binary.BigEndian.PutUint32(buf[0:4], frame.MagicBSV)
	binary.BigEndian.PutUint16(buf[4:6], frame.ProtoVer)
	buf[6] = msgType
	buf[7] = 0
	copy(buf[8:40], txID[:])
	binary.BigEndian.PutUint32(buf[40:44], senderID)
	binary.BigEndian.PutUint32(buf[44:48], sequenceID)
	binary.BigEndian.PutUint32(buf[48:52], seqNum)
	binary.BigEndian.PutUint32(buf[52:56], 0) // reserved
	return buf
}

func TestValidateNACK_valid(t *testing.T) {
	var txID [32]byte
	buf := buildNACK(0x10, txID, 1, 2, 3)
	if err := validateNACK(buf); err != nil {
		t.Fatalf("expected valid NACK, got error: %v", err)
	}
}

func TestValidateNACK_badMagic(t *testing.T) {
	var txID [32]byte
	buf := buildNACK(0x10, txID, 1, 2, 3)
	buf[0] = 0xFF
	if err := validateNACK(buf); err == nil {
		t.Fatal("expected error for bad magic")
	}
}

func TestValidateNACK_badMsgType(t *testing.T) {
	var txID [32]byte
	buf := buildNACK(0xFF, txID, 1, 2, 3)
	if err := validateNACK(buf); err == nil {
		t.Fatal("expected error for invalid msg type")
	}
}

func TestValidateNACK_tooShort(t *testing.T) {
	if err := validateNACK(make([]byte, 10)); err == nil {
		t.Fatal("expected error for short datagram")
	}
}

func TestExtractFields(t *testing.T) {
	var txID [32]byte
	txID[0] = 0xAB

	var senderID uint32 = 0xCDEF0123
	var sequenceID uint32 = 67890
	var seqNum uint32 = 12345

	buf := buildNACK(0x10, txID, senderID, sequenceID, seqNum)

	if got := extractTxID(buf); got != txID {
		t.Errorf("TxID mismatch: got %x, want %x", got, txID)
	}
	if got := extractSenderID(buf); got != senderID {
		t.Errorf("SenderID mismatch: got 0x%08X, want 0x%08X", got, senderID)
	}
	if got := extractSequenceID(buf); got != sequenceID {
		t.Errorf("SequenceID mismatch: got %d, want %d", got, sequenceID)
	}
	if got := extractSeqNum(buf); got != seqNum {
		t.Errorf("SeqNum mismatch: got %d, want %d", got, seqNum)
	}
}

func TestNACKSize_is56(t *testing.T) {
	if NACKSize != 56 {
		t.Errorf("NACKSize = %d, want 56", NACKSize)
	}
}

func TestResponseSize_is24(t *testing.T) {
	if ResponseSize != 24 {
		t.Errorf("ResponseSize = %d, want 24", ResponseSize)
	}
}

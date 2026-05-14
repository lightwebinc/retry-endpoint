package beacon

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/lightwebinc/bitcoin-shard-common/frame"
	"github.com/lightwebinc/bitcoin-shard-common/shard"
)

func testConfig() Config {
	return Config{
		NACKAddr:   net.ParseIP("fd20::41"),
		NACKPort:   9300,
		Tier:       0,
		Preference: 128,
		Interval:   60 * time.Second,
		Scope:      0x05,
		Flags:      FlagMulticastRetransmit,
		InstanceID: 0xDEADBEEF,
		GroupID:    shard.DefaultGroupID,
	}
}

func TestBuildADVERT_magic(t *testing.T) {
	s := New(testConfig())
	buf := s.buildADVERT()
	if len(buf) != ADVERTSize {
		t.Fatalf("ADVERT size = %d, want %d", len(buf), ADVERTSize)
	}
	magic := binary.BigEndian.Uint32(buf[0:4])
	if magic != frame.MagicBSV {
		t.Errorf("magic = 0x%08X, want 0x%08X", magic, frame.MagicBSV)
	}
}

func TestBuildADVERT_protoVer(t *testing.T) {
	s := New(testConfig())
	buf := s.buildADVERT()
	pv := binary.BigEndian.Uint16(buf[4:6])
	if pv != frame.ProtoVer {
		t.Errorf("protoVer = 0x%04X, want 0x%04X", pv, frame.ProtoVer)
	}
}

func TestBuildADVERT_msgType(t *testing.T) {
	s := New(testConfig())
	buf := s.buildADVERT()
	if buf[6] != MsgTypeADVERT {
		t.Errorf("msgType = 0x%02X, want 0x%02X", buf[6], MsgTypeADVERT)
	}
}

func TestBuildADVERT_scope(t *testing.T) {
	cfg := testConfig()
	cfg.Scope = 0x0E
	s := New(cfg)
	buf := s.buildADVERT()
	if buf[7] != 0x0E {
		t.Errorf("scope = 0x%02X, want 0x0E", buf[7])
	}
}

func TestBuildADVERT_nackAddr(t *testing.T) {
	s := New(testConfig())
	buf := s.buildADVERT()
	addr := make(net.IP, 16)
	copy(addr, buf[8:24])
	want := net.ParseIP("fd20::41")
	if !addr.Equal(want) {
		t.Errorf("NACKAddr = %v, want %v", addr, want)
	}
}

func TestBuildADVERT_nackPort(t *testing.T) {
	s := New(testConfig())
	buf := s.buildADVERT()
	port := binary.BigEndian.Uint16(buf[24:26])
	if port != 9300 {
		t.Errorf("NACKPort = %d, want 9300", port)
	}
}

func TestBuildADVERT_tierPreference(t *testing.T) {
	cfg := testConfig()
	cfg.Tier = 3
	cfg.Preference = 200
	s := New(cfg)
	buf := s.buildADVERT()
	if buf[26] != 3 {
		t.Errorf("tier = %d, want 3", buf[26])
	}
	if buf[27] != 200 {
		t.Errorf("preference = %d, want 200", buf[27])
	}
}

func TestBuildADVERT_interval(t *testing.T) {
	s := New(testConfig())
	buf := s.buildADVERT()
	interval := binary.BigEndian.Uint16(buf[28:30])
	if interval != 60 {
		t.Errorf("interval = %d, want 60", interval)
	}
}

func TestBuildADVERT_flags(t *testing.T) {
	s := New(testConfig())
	buf := s.buildADVERT()
	flags := binary.BigEndian.Uint16(buf[30:32])
	if flags != FlagMulticastRetransmit {
		t.Errorf("flags = 0x%04X, want 0x%04X", flags, FlagMulticastRetransmit)
	}
}

func TestBuildADVERT_instanceID(t *testing.T) {
	s := New(testConfig())
	buf := s.buildADVERT()
	id := binary.BigEndian.Uint32(buf[32:36])
	if id != 0xDEADBEEF {
		t.Errorf("instanceID = 0x%08X, want 0xDEADBEEF", id)
	}
}

func TestBeaconGroups_siteOnly(t *testing.T) {
	cfg := testConfig()
	cfg.Scope = 0x05
	s := New(cfg)
	groups := s.beaconGroups()
	if len(groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(groups))
	}
	if groups[0].IP[1] != 0x05 {
		t.Errorf("expected FF05 scope, got %02X%02X", groups[0].IP[0], groups[0].IP[1])
	}
}

func TestBeaconGroups_globalOnly(t *testing.T) {
	cfg := testConfig()
	cfg.Scope = 0x0E
	s := New(cfg)
	groups := s.beaconGroups()
	if len(groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(groups))
	}
	if groups[0].IP[1] != 0x0E {
		t.Errorf("expected FF0E scope, got %02X%02X", groups[0].IP[0], groups[0].IP[1])
	}
}

func TestBeaconGroups_orgOnly(t *testing.T) {
	cfg := testConfig()
	cfg.Scope = 0x08
	s := New(cfg)
	groups := s.beaconGroups()
	if len(groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(groups))
	}
	if groups[0].IP[1] != 0x08 {
		t.Errorf("expected FF08 scope, got %02X%02X", groups[0].IP[0], groups[0].IP[1])
	}
}

func TestBeaconGroups_both(t *testing.T) {
	cfg := testConfig()
	cfg.Scope = 0xFF
	s := New(cfg)
	groups := s.beaconGroups()
	if len(groups) != 3 {
		t.Fatalf("expected 3 groups, got %d", len(groups))
	}
}

func TestBeaconGroups_all(t *testing.T) {
	cfg := testConfig()
	cfg.Scope = 0xFF
	s := New(cfg)
	groups := s.beaconGroups()
	if len(groups) != 3 {
		t.Fatalf("expected 3 groups (site+org+global), got %d", len(groups))
	}
	scopes := []byte{groups[0].IP[1], groups[1].IP[1], groups[2].IP[1]}
	for i, want := range []byte{0x05, 0x08, 0x0E} {
		if scopes[i] != want {
			t.Errorf("group[%d]: expected scope byte FF%02X, got FF%02X", i, want, scopes[i])
		}
	}
}

func TestBeaconGroups_invalid(t *testing.T) {
	cfg := testConfig()
	cfg.Scope = 0x00
	s := New(cfg)
	groups := s.beaconGroups()
	if len(groups) != 0 {
		t.Fatalf("expected 0 groups, got %d", len(groups))
	}
}

func TestDefaultInterval(t *testing.T) {
	cfg := testConfig()
	cfg.Interval = 0
	s := New(cfg)
	if s.cfg.Interval != 60*time.Second {
		t.Errorf("default interval = %v, want 60s", s.cfg.Interval)
	}
}

func TestADVERTSize_is56(t *testing.T) {
	if ADVERTSize != 56 {
		t.Errorf("ADVERTSize = %d, want 56", ADVERTSize)
	}
}

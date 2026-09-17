package beacon

import (
	"net"
	"testing"
)

// THE DEFECT, at the socket the endpoint actually writes to. BRC-126
// §Beacon Scopes and BRC-129 §Source Mode and Address Range require the
// 0xFFFD control group to take the source-specific FF3x prefix under SSM.
// This package used to hardcode 0xFF05/0xFF08/0xFF0E, so an SSM fabric
// advertised into a group outside the ff35::/16 range it forwards.
func TestBeaconGroups_SSMPrefixIsAdvertisedInto(t *testing.T) {
	cfg := testConfig()
	cfg.Scope = 0x05
	cfg.GroupPrefixes = []uint16{0xFF35}
	s := New(cfg)

	groups := s.beaconGroups()
	if len(groups) != 1 {
		t.Fatalf("groups = %d, want 1", len(groups))
	}
	want := net.ParseIP("ff35::b:fffd")
	if !groups[0].IP.Equal(want) {
		t.Errorf("beacon destination = %s, want %s", groups[0].IP, want)
	}
	if groups[0].Port != 9300 {
		t.Errorf("beacon port = %d, want 9300", groups[0].Port)
	}
}

// -control-group-compat=both hands down two prefixes: the endpoint writes
// into the group an un-upgraded listener still joins AND the conformant
// one, so neither side of the flag day goes deaf.
func TestBeaconGroups_BothPrefixesDuringTransition(t *testing.T) {
	cfg := testConfig()
	cfg.Scope = 0x05
	cfg.GroupPrefixes = []uint16{0xFF05, 0xFF35}
	s := New(cfg)

	groups := s.beaconGroups()
	if len(groups) != 2 {
		t.Fatalf("groups = %d, want 2", len(groups))
	}
	for i, want := range []string{"ff05::b:fffd", "ff35::b:fffd"} {
		if !groups[i].IP.Equal(net.ParseIP(want)) {
			t.Errorf("group %d = %s, want %s", i, groups[i].IP, want)
		}
	}
}

// The ADVERT scope byte is the scope, not the source mode: moving to FF3x
// must not change byte 7 of the datagram, or listeners would mis-file the
// endpoint's scope.
func TestBeaconGroups_SSMKeepsADVERTScopeByte(t *testing.T) {
	cfg := testConfig()
	cfg.Scope = 0x05
	cfg.GroupPrefixes = []uint16{0xFF35}
	s := New(cfg)

	buf := s.buildADVERT()
	if buf[7] != 0x05 {
		t.Errorf("ADVERT scope byte = 0x%02X, want 0x05", buf[7])
	}
}

// An un-updated caller that sets no GroupPrefixes keeps the exact pre-fix
// wire rather than silently changing group.
func TestBeaconGroups_FallbackIsTheLegacyASMSet(t *testing.T) {
	cfg := testConfig()
	cfg.Scope = 0xFF
	s := New(cfg)

	groups := s.beaconGroups()
	if len(groups) != 3 {
		t.Fatalf("groups = %d, want 3", len(groups))
	}
	for i, want := range []byte{0x05, 0x08, 0x0E} {
		if groups[i].IP[0] != 0xFF || groups[i].IP[1] != want {
			t.Errorf("group %d prefix = %02X%02X, want FF%02X",
				i, groups[i].IP[0], groups[i].IP[1], want)
		}
	}
}

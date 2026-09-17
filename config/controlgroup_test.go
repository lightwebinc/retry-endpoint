package config

import (
	"net"
	"testing"

	"github.com/lightwebinc/shard-common/shard"
)

func groupAddr(prefix uint16) net.IP {
	return shard.GroupAddr(prefix, shard.DefaultGroupID, shard.GroupBeacon)
}

// THE DEFECT. BRC-126 §Beacon Scopes and BRC-129 §Source Mode and Address
// Range both require the 0xFFFD control groups to take the source-specific
// FF3x prefix under SSM. Every release before this one advertised into
// FF05::B:FFFD whatever -source-mode said.
func TestControlGroupPrefixes_SSMUsesSourceSpecificPrefix(t *testing.T) {
	for _, tc := range []struct {
		scope string
		want  uint16
		addr  string
	}{
		{"site", 0xFF35, "ff35::b:fffd"},
		{"global", 0xFF3E, "ff3e::b:fffd"},
	} {
		got, err := ControlGroupPrefixes(ControlGroupDerived, "ssm", tc.scope)
		if err != nil {
			t.Fatalf("scope %s: %v", tc.scope, err)
		}
		if len(got) != 1 || got[0] != tc.want {
			t.Fatalf("scope %s under ssm: prefixes = %#04x, want [%#04x]", tc.scope, got, tc.want)
		}
		if ip := groupAddr(got[0]); !ip.Equal(net.ParseIP(tc.addr)) {
			t.Errorf("scope %s under ssm: group = %s, want %s", tc.scope, ip, tc.addr)
		}
	}
}

// ASM is unchanged in every compat mode: nothing to derive, so all four
// scope names keep the prefixes they always had.
func TestControlGroupPrefixes_ASMUnchanged(t *testing.T) {
	for scope, want := range Scopes {
		for _, compat := range ControlGroupCompatValues {
			got, err := ControlGroupPrefixes(compat, "asm", scope)
			if err != nil {
				t.Fatalf("compat %s scope %s: %v", compat, scope, err)
			}
			if len(got) != 1 || got[0] != want {
				t.Errorf("compat %s scope %s under asm: prefixes = %#04x, want [%#04x]",
					compat, scope, got, want)
			}
		}
	}
}

// The sender-safe default: "asm-only" must never consult -source-mode.
// This is what keeps an un-upgraded listener reachable on upgrade.
func TestControlGroupPrefixes_ASMOnlyIgnoresSourceMode(t *testing.T) {
	got, err := ControlGroupPrefixes(ControlGroupASMOnly, "ssm", "site")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != 0xFF05 {
		t.Fatalf("asm-only under ssm: prefixes = %#04x, want [0xff05]", got)
	}
}

func TestControlGroupPrefixes_BothSpansTheFlagDay(t *testing.T) {
	got, err := ControlGroupPrefixes(ControlGroupBoth, "ssm", "site")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != 0xFF05 || got[1] != 0xFF35 {
		t.Fatalf("both under ssm: prefixes = %#04x, want [0xff05 0xff35]", got)
	}
}

// The shipped default must be the one that cannot strand a peer: a fresh
// Config advertises into the legacy group until an operator says otherwise.
func TestBeaconGroupPrefixes_DefaultIsSenderSafe(t *testing.T) {
	c := &Config{BeaconScope: "site", SourceMode: "ssm", ControlGroupCompat: ControlGroupASMOnly}
	got, err := c.BeaconGroupPrefixes()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != 0xFF05 {
		t.Fatalf("default prefixes = %#04x, want [0xff05]", got)
	}
}

func TestBeaconGroupPrefixes_DerivedSiteIsFF35(t *testing.T) {
	c := &Config{BeaconScope: "site", SourceMode: "ssm", ControlGroupCompat: ControlGroupDerived}
	got, err := c.BeaconGroupPrefixes()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != 0xFF35 {
		t.Fatalf("derived prefixes = %#04x, want [0xff35]", got)
	}
}

// -beacon-scope=all under ASM keeps its three groups, in scope order.
func TestBeaconGroupPrefixes_AllUnderASM(t *testing.T) {
	c := &Config{BeaconScope: "all", SourceMode: "asm", ControlGroupCompat: ControlGroupBoth}
	got, err := c.BeaconGroupPrefixes()
	if err != nil {
		t.Fatal(err)
	}
	want := []uint16{0xFF05, 0xFF08, 0xFF0E}
	if len(got) != len(want) {
		t.Fatalf("prefixes = %#04x, want %#04x", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("prefixes = %#04x, want %#04x", got, want)
		}
	}
}

// BRC-129 tables an SSM control group at site and global only, so
// -beacon-scope=all (which covers org) cannot be made fully conformant.
// "derived" fails at load with a message naming the fix rather than quietly
// dropping org or falling back to FF08 — a wrong group is invisible on the
// wire.
func TestBeaconGroupPrefixes_AllUnderSSMIsRejectedByDerived(t *testing.T) {
	c := &Config{BeaconScope: "all", SourceMode: "ssm", ControlGroupCompat: ControlGroupDerived}
	if _, err := c.BeaconGroupPrefixes(); err == nil {
		t.Error("derived with beacon-scope=all under ssm: want error, got none")
	}
	// asm-only and both both still work, so an existing deployment upgrades
	// without tripping over it.
	for _, compat := range []string{ControlGroupASMOnly, ControlGroupBoth} {
		c := &Config{BeaconScope: "all", SourceMode: "ssm", ControlGroupCompat: compat}
		if _, err := c.BeaconGroupPrefixes(); err != nil {
			t.Errorf("compat %s with beacon-scope=all under ssm: %v", compat, err)
		}
	}
}

// "both" with -beacon-scope=all keeps the three legacy groups and adds the
// SSM forms only where BRC-129 defines one (site and global, not org).
func TestBeaconGroupPrefixes_BothAllUnderSSM(t *testing.T) {
	c := &Config{BeaconScope: "all", SourceMode: "ssm", ControlGroupCompat: ControlGroupBoth}
	got, err := c.BeaconGroupPrefixes()
	if err != nil {
		t.Fatal(err)
	}
	want := []uint16{0xFF05, 0xFF35, 0xFF08, 0xFF0E, 0xFF3E}
	if len(got) != len(want) {
		t.Fatalf("prefixes = %#04x, want %#04x", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("prefixes = %#04x, want %#04x", got, want)
		}
	}
}

func TestControlGroupPrefixes_Invalid(t *testing.T) {
	if _, err := ControlGroupPrefixes("sometimes", "ssm", "site"); err == nil {
		t.Error("unknown compat mode: want error, got none")
	}
	if _, err := ControlGroupPrefixes(ControlGroupDerived, "ssm", "nonsense"); err == nil {
		t.Error("unknown scope: want error, got none")
	}
	c := &Config{BeaconScope: "nonsense", SourceMode: "asm", ControlGroupCompat: ControlGroupASMOnly}
	if _, err := c.BeaconGroupPrefixes(); err == nil {
		t.Error("unknown beacon scope: want error, got none")
	}
}

// The derived prefix must come from the shared helper the data plane uses,
// not a second table.
func TestControlGroupPrefixes_MatchesSharedHelper(t *testing.T) {
	for scopeName, scope := range map[string]shard.Scope{
		"site":   shard.ScopeSite,
		"global": shard.ScopeGlobal,
	} {
		want, err := shard.Prefix(shard.SourceModeSSM, scope)
		if err != nil {
			t.Fatal(err)
		}
		got, err := ControlGroupPrefixes(ControlGroupDerived, "ssm", scopeName)
		if err != nil {
			t.Fatal(err)
		}
		if got[0] != want {
			t.Errorf("scope %s: derived %#04x, shard.Prefix says %#04x", scopeName, got[0], want)
		}
	}
}

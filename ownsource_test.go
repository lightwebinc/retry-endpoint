package main

import (
	"net/netip"
	"testing"
)

// Regression: a node must never (S,G)-join its own source — the MLD state
// installs an iif==oif mroute on a collapsed edge and originated frames loop
// until hop-limit death (~60x amplification, measured in a geo SSM lab).
func TestExcludeOwnSource(t *testing.T) {
	a := netip.MustParseAddr
	cases := []struct {
		name string
		srcs []netip.Addr
		own  netip.Addr
		want []netip.Addr
	}{
		{"excludes own", []netip.Addr{a("fd00:5::a8"), a("fd00:5::b8"), a("fd00:5::c8")},
			a("fd00:5::a8"), []netip.Addr{a("fd00:5::b8"), a("fd00:5::c8")}},
		{"own absent", []netip.Addr{a("fd00:5::b8")}, a("fd00:5::a8"),
			[]netip.Addr{a("fd00:5::b8")}},
		{"no local source configured", []netip.Addr{a("fd00:5::a8")}, netip.Addr{},
			[]netip.Addr{a("fd00:5::a8")}},
		{"only own -> empty", []netip.Addr{a("fd00:5::a8")}, a("fd00:5::a8"),
			[]netip.Addr{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := excludeOwnSource(append([]netip.Addr(nil), tc.srcs...), tc.own)
			if len(got) != len(tc.want) {
				t.Fatalf("got %v want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got %v want %v", got, tc.want)
				}
			}
		})
	}
}

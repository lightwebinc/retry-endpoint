package main

import (
	"context"
	"net/netip"
	"slices"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/shard"

	"github.com/lightwebinc/retry-endpoint/config"
)

// ssmGroup returns the receiver-side join target of group idx under cfg.
func ssmGroup(t *testing.T, cfg *config.Config, idx shard.GroupIdx) netip.Addr {
	t.Helper()
	ga, ok := netip.AddrFromSlice(shard.GroupAddr(cfg.MCPrefix, cfg.MCGroupID, idx).To16())
	if !ok {
		t.Fatalf("group %#04x: not IPv6", uint16(idx))
	}
	return ga
}

// Regression: the block-broadcast group (0xFFFE) was (S,G)-joined against the
// manifest emitters. Its coinbase and block-announce frames leave from the
// emitting proxy (BRC-133), a data-plane publisher source (BRC-129), so an SSM
// endpoint whose manifest emitter is not the proxy cached no block frame and
// answered every block NACK with MISS. It must take the publisher roster, the
// same one the shard groups and the BEEF band take.
func TestBuildSSMGroupSources_BlockBroadcastFollowsPublishers(t *testing.T) {
	a := netip.MustParseAddr
	pubA, pubB, own := a("2001:db8::a01"), a("2001:db8::a02"), a("2001:db8::a03")
	manifest := a("2001:db8::f01")
	cfg := &config.Config{
		SourceMode:           "ssm",
		MCPrefix:             0xFF35,
		MCGroupID:            shard.DefaultGroupID,
		NumGroups:            4,
		BEEFEnabled:          true,
		BEEFShardBits:        1,
		BindSource:           own.String(),
		SSMBootstrapRefresh:  time.Minute,
		SSMBootstrapManifest: []string{manifest.String()},
		// This node's own source is on the roster, as on a node that is also
		// a publisher: no group may be joined against it.
		SSMPublishersStatic: []string{pubA.String(), pubB.String(), own.String()},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gs, err := buildSSMGroupSources(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	want := []netip.Addr{pubA, pubB}

	if got := gs[ssmGroup(t, cfg, shard.GroupBlockBroadcast)]; !slices.Equal(got, want) {
		t.Errorf("0xFFFE sources = %v, want the publisher roster %v", got, want)
	}
	for _, idx := range []shard.GroupIdx{0, 3, 0x1000, 0x1001} {
		if got := gs[ssmGroup(t, cfg, idx)]; !slices.Equal(got, want) {
			t.Errorf("group %#04x sources = %v, want %v", uint16(idx), got, want)
		}
	}
	for ga, srcs := range gs {
		if slices.Contains(srcs, manifest) {
			t.Errorf("group %s is joined against the manifest emitter %s", ga, manifest)
		}
	}
}

// With no publisher list the block-broadcast group has no source list, so it
// is joined any-source exactly like the shard groups; the manifest list still
// feeds no join.
func TestBuildSSMGroupSources_NoPublishers(t *testing.T) {
	cfg := &config.Config{
		SourceMode:           "ssm",
		MCPrefix:             0xFF35,
		MCGroupID:            shard.DefaultGroupID,
		NumGroups:            4,
		SSMBootstrapRefresh:  time.Minute,
		SSMBootstrapManifest: []string{"2001:db8::f01"},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gs, err := buildSSMGroupSources(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(gs) != 0 {
		t.Errorf("group sources = %v, want none (every joined group any-source)", gs)
	}
}

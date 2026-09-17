package config

import (
	"fmt"
	"strings"

	"github.com/lightwebinc/shard-common/shard"
)

// Control-plane group compatibility (BRC-126 §Beacon Scopes, BRC-129
// §Source Mode and Address Range).
//
// Both BRCs require the control-plane groups at index 0xFFFD — the BRC-126
// ADVERT beacon and the BRC-139 shard manifest, which share that group
// address — to take the source-specific FF3x prefix when the fabric runs
// SSM: FF35::B:FFFD at site scope, FF3E::B:FFFD at global scope. Releases
// before this one always advertised into the any-source FF0x prefix,
// whatever -source-mode said, even though the data plane in the same
// process derived FF3x correctly via [shard.Prefix].
//
// Correcting that moves a live group address, so it is a flag day: a
// listener joined to FF35::B:FFFD hears nothing from a retry endpoint still
// advertising into FF05::B:FFFD, and a beacon that lands on the wrong group
// raises no error anywhere — the symptom is silence. This switch exists
// only to make that transition ordered and observable. It is temporary:
// once the fleet is on "derived" the flag and this file go away.
//
// ROLLOUT ORDER — the same note is in shard-listener and shard-manifest:
//
//  1. Roll RECEIVERS (shard-listener, shard-proxy). Their default is
//     "both", so an upgraded receiver joins FF0x AND FF3x and hears
//     un-upgraded and upgraded senders alike. No config change is needed
//     and there is no window in which discovery stops.
//  2. Roll SENDERS (retry-endpoint, shard-manifest) — this binary. The
//     default here is "asm-only", so upgrading does not move the wire.
//  3. One converge sets the SENDERS to "derived". ADVERTs move to FF3x,
//     which every receiver from step 1 already joined.
//  4. After a soak, one converge sets the RECEIVERS to "derived", dropping
//     the legacy join.
//
// Setting this endpoint to "derived" before step 1 has covered every
// listener is the one ordering that silently strands a peer. The default is
// picked so that doing nothing but upgrading binaries can never produce it.
//
// Under -source-mode=asm every mode collapses to the same FF0x prefixes, so
// this flag is a no-op for an ASM deployment.
const (
	// ControlGroupASMOnly always derives the any-source FF0x prefix from
	// the scope table, ignoring -source-mode. Pre-fix behaviour, and the
	// default for this sender.
	ControlGroupASMOnly = "asm-only"

	// ControlGroupBoth derives both prefixes: the endpoint advertises into
	// FF0x and FF3x. Transition value; use it when listeners are known to
	// be mixed and a second converge is not wanted.
	ControlGroupBoth = "both"

	// ControlGroupDerived derives the prefix from (-source-mode, scope)
	// per BRC-126/BRC-129. Conformant; the end state.
	ControlGroupDerived = "derived"
)

// ControlGroupCompatValues lists the accepted -control-group-compat values
// for flag help and validation.
var ControlGroupCompatValues = []string{ControlGroupASMOnly, ControlGroupBoth, ControlGroupDerived}

// ControlGroupPrefixes returns the upper-16-bit multicast prefixes the
// control-plane groups (index 0xFFFD) are derived from for one scope name,
// in the order they should be sent to: the legacy FF0x prefix first, the
// derived prefix second, deduplicated when they coincide.
//
// The SSM prefix comes from [shard.Prefix], the same helper the data plane
// uses to build Config.MCPrefix — the scope table below is only consulted
// for the any-source form, which it already held.
//
// BRC-129 defines SSM control groups at site and global scope only (FF35
// and FF3E); "link" and "org" have no SSM control group. Under "derived" —
// an explicit request for the conformant address — asking for one at such a
// scope is a config error rather than a silent fall back to FF0x. Under
// "both" it degrades to the any-source prefix alone, so the receiver-safe
// default can never turn a binary upgrade into a startup failure. Under ASM
// all four scope names keep working exactly as before.
func ControlGroupPrefixes(compat, sourceMode, scopeName string) ([]uint16, error) {
	asm, ok := Scopes[scopeName]
	if !ok {
		return nil, fmt.Errorf("unknown scope %q; valid values: link, site, org, global", scopeName)
	}

	switch compat {
	case ControlGroupASMOnly:
		// Never consults -source-mode: this is the pre-fix wire, kept
		// byte-for-byte so an un-upgraded listener still hears us.
		return []uint16{asm}, nil
	case ControlGroupBoth, ControlGroupDerived:
	default:
		return nil, fmt.Errorf("invalid -control-group-compat %q (%s)",
			compat, strings.Join(ControlGroupCompatValues, "|"))
	}

	derived := asm
	if strings.EqualFold(sourceMode, "ssm") {
		sc, err := shard.ParseScope(scopeName)
		switch {
		case err != nil && compat == ControlGroupDerived:
			// Strict: the operator asked for the conformant address and
			// there isn't one at this scope. Say so rather than emitting
			// FF0x under a flag named "derived".
			return nil, fmt.Errorf("-control-group-compat=derived with -source-mode=ssm needs a beacon scope BRC-129 "+
				"defines an SSM control group for (site|global), got %q", scopeName)
		case err != nil:
			// Permissive: "both" means every form that exists, and at this
			// scope only the any-source one does. Falling back here is what
			// keeps the receiver-safe default from turning a binary upgrade
			// into a startup failure; the resolved group list is logged at
			// startup, so the outcome is visible.
		default:
			p, perr := shard.Prefix(shard.SourceModeSSM, sc)
			if perr != nil {
				return nil, perr
			}
			derived = p
		}
	}

	if compat == ControlGroupDerived || derived == asm {
		return []uint16{derived}, nil
	}
	return []uint16{asm, derived}, nil
}

// beaconScopeNames expands -beacon-scope into the individual scope names it
// covers. "both" and "all" are equivalent per BRC-126 and cover all three.
func beaconScopeNames(beaconScope string) ([]string, error) {
	switch beaconScope {
	case "site", "org", "global":
		return []string{beaconScope}, nil
	case "both", "all":
		return []string{"site", "org", "global"}, nil
	default:
		return nil, fmt.Errorf("beacon-scope must be one of site|org|global|both|all, got %q", beaconScope)
	}
}

// BeaconGroupPrefixes resolves the prefixes this endpoint advertises into,
// from -control-group-compat, -source-mode and -beacon-scope. Order is
// scope-major (site, org, global) and, within a scope, legacy prefix first.
func (c *Config) BeaconGroupPrefixes() ([]uint16, error) {
	names, err := beaconScopeNames(c.BeaconScope)
	if err != nil {
		return nil, err
	}
	out := make([]uint16, 0, 2*len(names))
	seen := make(map[uint16]struct{}, 2*len(names))
	for _, name := range names {
		ps, err := ControlGroupPrefixes(c.ControlGroupCompat, c.SourceMode, name)
		if err != nil {
			return nil, err
		}
		for _, p := range ps {
			if _, dup := seen[p]; dup {
				continue
			}
			seen[p] = struct{}{}
			out = append(out, p)
		}
	}
	return out, nil
}

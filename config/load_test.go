package config

import (
	"flag"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

// testIface returns a real interface name usable for flag resolution.
// Prefers the loopback so the test is portable across hosts.
func testIface(t *testing.T) string {
	t.Helper()
	ifaces, err := net.Interfaces()
	if err != nil || len(ifaces) == 0 {
		t.Skip("no network interfaces available")
	}
	for _, ifc := range ifaces {
		if ifc.Flags&net.FlagLoopback != 0 {
			return ifc.Name
		}
	}
	return ifaces[0].Name
}

// loadWithArgs resets the global flag state and invokes Load with the given
// CLI args. Not safe for t.Parallel — relies on process-global flag state.
func loadWithArgs(t *testing.T, args ...string) (*Config, error) {
	t.Helper()
	origArgs := os.Args
	origCL := flag.CommandLine
	t.Cleanup(func() {
		os.Args = origArgs
		flag.CommandLine = origCL
	})
	flag.CommandLine = flag.NewFlagSet("test", flag.ContinueOnError)
	flag.CommandLine.SetOutput(io.Discard)
	os.Args = append([]string{"retry-endpoint"}, args...)
	return Load()
}

// withEgress prepends a valid -egress-iface flag so interface resolution
// succeeds (egress iface is required and validated against real NICs).
func withEgress(t *testing.T, args ...string) []string {
	return append([]string{"-egress-iface=" + testIface(t)}, args...)
}

func TestLoad_Minimal(t *testing.T) {
	c, err := loadWithArgs(t, withEgress(t)...)
	if err != nil {
		t.Fatalf("minimal load: %v", err)
	}
	if c.SourceMode != "asm" {
		t.Errorf("default source-mode = %q, want asm", c.SourceMode)
	}
	if c.MCPrefix != 0xFF05 {
		t.Errorf("site prefix = 0x%04X, want 0xFF05", c.MCPrefix)
	}
	if c.NumWorkers != 1 {
		t.Errorf("ingress workers must be 1, got %d", c.NumWorkers)
	}
	if c.CacheBackend != "memory" {
		t.Errorf("default cache-backend = %q", c.CacheBackend)
	}
	if c.BeaconScopeByte != 0x05 {
		t.Errorf("site beacon scope byte = 0x%02X, want 0x05", c.BeaconScopeByte)
	}
	// Differentiated per-FrameVer TTL defaults remain when nothing explicit.
	if c.CacheTTLTx != defaultCacheTTLTx || c.CacheTTLBlock != defaultCacheTTLBlock {
		t.Errorf("differentiated TTL defaults not applied: tx=%v block=%v",
			c.CacheTTLTx, c.CacheTTLBlock)
	}
}

func TestLoad_EgressIfaceRequired(t *testing.T) {
	// Empty egress iface list must error.
	if _, err := loadWithArgs(t, "-egress-iface="); err == nil {
		t.Error("empty egress-iface should error")
	}
	// Nonexistent NIC must error.
	if _, err := loadWithArgs(t, "-egress-iface=definitely-not-real0"); err == nil {
		t.Error("nonexistent egress-iface should error")
	}
}

func TestLoad_ShardBitsBounds(t *testing.T) {
	for _, bits := range []string{"0", "16"} {
		if _, err := loadWithArgs(t, withEgress(t, "-shard-bits="+bits)...); err == nil {
			t.Errorf("shard-bits=%s should error", bits)
		}
	}
}

func TestLoad_CacheBackend(t *testing.T) {
	if _, err := loadWithArgs(t, withEgress(t, "-cache-backend=bogus")...); err == nil {
		t.Error("invalid cache-backend should error")
	}
	if _, err := loadWithArgs(t, withEgress(t, "-cache-backend=redis")...); err != nil {
		t.Errorf("redis backend should be valid: %v", err)
	}
}

func TestLoad_PerFrameVerTTLMustBePositive(t *testing.T) {
	if _, err := loadWithArgs(t, withEgress(t, "-cache-ttl-tx=0")...); err == nil {
		t.Error("cache-ttl-tx=0 should error")
	}
}

func TestLoad_CacheTTLFallback(t *testing.T) {
	// Setting CACHE_TTL via env makes it explicit; per-FrameVer TTLs that are
	// not themselves explicit collapse onto it.
	t.Setenv("CACHE_TTL", "120s")
	c, err := loadWithArgs(t, withEgress(t)...)
	if err != nil {
		t.Fatalf("cache-ttl fallback load: %v", err)
	}
	want := 120 * time.Second
	if c.CacheTTLTx != want || c.CacheTTLBlock != want ||
		c.CacheTTLSubtree != want || c.CacheTTLAnchor != want {
		t.Errorf("per-FrameVer TTLs did not collapse to CACHE_TTL: tx=%v block=%v sub=%v anc=%v",
			c.CacheTTLTx, c.CacheTTLBlock, c.CacheTTLSubtree, c.CacheTTLAnchor)
	}
}

func TestLoad_CacheTTLExplicitPerTypeWins(t *testing.T) {
	t.Setenv("CACHE_TTL", "120s")
	t.Setenv("CACHE_TTL_TX", "7s")
	c, err := loadWithArgs(t, withEgress(t)...)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if c.CacheTTLTx != 7*time.Second {
		t.Errorf("explicit per-type TTL should win: tx=%v", c.CacheTTLTx)
	}
	if c.CacheTTLBlock != 120*time.Second {
		t.Errorf("unset per-type should fall back to CACHE_TTL: block=%v", c.CacheTTLBlock)
	}
}

func TestLoad_SourceModeSSM(t *testing.T) {
	if _, err := loadWithArgs(t, withEgress(t, "-source-mode=ssm")...); err == nil {
		t.Error("ssm without bootstrap should error")
	}
	c, err := loadWithArgs(t, withEgress(t,
		"-source-mode=ssm", "-scope=site", "-ssm-bootstrap-beacon=2001:db8::1")...)
	if err != nil {
		t.Fatalf("ssm with bootstrap: %v", err)
	}
	if c.SourceMode != "ssm" {
		t.Errorf("source-mode = %q", c.SourceMode)
	}
	// Invalid bind-source (IPv4) under SSM.
	if _, err := loadWithArgs(t, withEgress(t,
		"-source-mode=ssm", "-scope=site",
		"-ssm-bootstrap-beacon=2001:db8::1", "-bind-source=10.0.0.1")...); err == nil {
		t.Error("ipv4 bind-source under ssm should error")
	}
}

func TestLoad_SSMRefreshNonPositive(t *testing.T) {
	if _, err := loadWithArgs(t, withEgress(t, "-ssm-bootstrap-refresh=0")...); err == nil {
		t.Error("ssm-bootstrap-refresh=0 should error")
	}
}

func TestLoad_SSMPublishersStaticCap(t *testing.T) {
	many := make([]string, 17)
	for i := range many {
		many[i] = "2001:db8::1"
	}
	args := withEgress(t, "-source-mode=ssm", "-scope=site",
		"-ssm-publishers-static="+strings.Join(many, ","))
	if _, err := loadWithArgs(t, args...); err == nil {
		t.Error(">16 static publishers without manifest discovery should error")
	}
}

func TestLoad_InvalidSourceMode(t *testing.T) {
	if _, err := loadWithArgs(t, withEgress(t, "-source-mode=bogus")...); err == nil {
		t.Error("invalid source-mode should error")
	}
}

func TestLoad_InvalidGroupID(t *testing.T) {
	if _, err := loadWithArgs(t, withEgress(t, "-mc-group-id=zzz")...); err == nil {
		t.Error("invalid group-id should error")
	}
}

func TestLoad_BeaconValidation(t *testing.T) {
	for _, scope := range []struct {
		name string
		want byte
	}{
		{"site", 0x05}, {"org", 0x08}, {"global", 0x0E}, {"both", 0xFF}, {"all", 0xFF},
	} {
		c, err := loadWithArgs(t, withEgress(t, "-beacon-scope="+scope.name)...)
		if err != nil {
			t.Fatalf("beacon-scope=%s: %v", scope.name, err)
		}
		if c.BeaconScopeByte != scope.want {
			t.Errorf("beacon-scope=%s byte=0x%02X want 0x%02X", scope.name, c.BeaconScopeByte, scope.want)
		}
	}
	if _, err := loadWithArgs(t, withEgress(t, "-beacon-scope=bogus")...); err == nil {
		t.Error("invalid beacon-scope should error")
	}
	if _, err := loadWithArgs(t, withEgress(t, "-beacon-interval=500ms")...); err == nil {
		t.Error("beacon-interval < 1s should error")
	}
	if _, err := loadWithArgs(t, withEgress(t, "-nack-addr=10.0.0.1")...); err == nil {
		t.Error("ipv4 nack-addr should error")
	}
	c, err := loadWithArgs(t, withEgress(t, "-nack-addr=2001:db8::5")...)
	if err != nil {
		t.Fatalf("valid ipv6 nack-addr: %v", err)
	}
	if c.BeaconNACKAddr != "2001:db8::5" {
		t.Errorf("nack-addr = %q", c.BeaconNACKAddr)
	}
}

func TestLoad_MultiEgressIface(t *testing.T) {
	ifc := testIface(t)
	c, err := loadWithArgs(t, "-egress-iface="+ifc+", "+ifc)
	if err != nil {
		t.Fatalf("multi egress iface: %v", err)
	}
	if len(c.EgressIfaces) != 2 {
		t.Errorf("expected 2 egress ifaces, got %v", c.EgressIfaces)
	}
}

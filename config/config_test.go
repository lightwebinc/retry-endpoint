package config

import (
	"testing"
	"time"
)

func TestEnvStr(t *testing.T) {
	t.Setenv("CFG_S", "value")
	if got := envStr("CFG_S", "def"); got != "value" {
		t.Errorf("got %q", got)
	}
	t.Setenv("CFG_S", "")
	if got := envStr("CFG_S", "def"); got != "def" {
		t.Errorf("default fallback: %q", got)
	}
}

func TestEnvInt(t *testing.T) {
	t.Setenv("CFG_I", "42")
	if got := envInt("CFG_I", 7); got != 42 {
		t.Errorf("got %d", got)
	}
	t.Setenv("CFG_I", "notanumber")
	if got := envInt("CFG_I", 7); got != 7 {
		t.Errorf("invalid → default: %d", got)
	}
}

func TestEnvBool(t *testing.T) {
	t.Setenv("CFG_B", "true")
	if !envBool("CFG_B", false) {
		t.Error("true")
	}
	t.Setenv("CFG_B", "false")
	if envBool("CFG_B", true) {
		t.Error("false")
	}
	t.Setenv("CFG_B", "xyz")
	if !envBool("CFG_B", true) {
		t.Error("invalid → default")
	}
}

func TestEnvFloat(t *testing.T) {
	t.Setenv("CFG_F", "3.14")
	if got := envFloat("CFG_F", 1.0); got != 3.14 {
		t.Errorf("got %v", got)
	}
	t.Setenv("CFG_F", "notfloat")
	if got := envFloat("CFG_F", 1.0); got != 1.0 {
		t.Errorf("invalid → default: %v", got)
	}
}

func TestResolveCacheTTLs(t *testing.T) {
	cases := []struct {
		name             string
		cacheTTL         time.Duration // value of CacheTTL field
		cacheTTLExplicit bool
		txExplicit       bool
		blockExplicit    bool
		subtreeExplicit  bool
		anchorExplicit   bool
		// initial values mirror what envDuration would have set with
		// differentiated defaults; resolveCacheTTLs may overwrite them.
		initTx, initBlock, initSubtree, initAnchor time.Duration
		wantTx, wantBlock, wantSubtree, wantAnchor time.Duration
	}{
		{
			name:        "all defaults",
			cacheTTL:    60 * time.Second,
			initTx:      defaultCacheTTLTx,
			initBlock:   defaultCacheTTLBlock,
			initSubtree: defaultCacheTTLSubtree,
			initAnchor:  defaultCacheTTLAnchor,
			wantTx:      defaultCacheTTLTx,
			wantBlock:   defaultCacheTTLBlock,
			wantSubtree: defaultCacheTTLSubtree,
			wantAnchor:  defaultCacheTTLAnchor,
		},
		{
			name:             "only CACHE_TTL set collapses all four",
			cacheTTL:         30 * time.Second,
			cacheTTLExplicit: true,
			initTx:           defaultCacheTTLTx,
			initBlock:        defaultCacheTTLBlock,
			initSubtree:      defaultCacheTTLSubtree,
			initAnchor:       defaultCacheTTLAnchor,
			wantTx:           30 * time.Second,
			wantBlock:        30 * time.Second,
			wantSubtree:      30 * time.Second,
			wantAnchor:       30 * time.Second,
		},
		{
			name:             "CACHE_TTL plus explicit block leaves block intact",
			cacheTTL:         30 * time.Second,
			cacheTTLExplicit: true,
			blockExplicit:    true,
			initTx:           defaultCacheTTLTx,
			initBlock:        15 * time.Minute, // explicit override
			initSubtree:      defaultCacheTTLSubtree,
			initAnchor:       defaultCacheTTLAnchor,
			wantTx:           30 * time.Second,
			wantBlock:        15 * time.Minute,
			wantSubtree:      30 * time.Second,
			wantAnchor:       30 * time.Second,
		},
		{
			name:        "only per-type set, no CACHE_TTL fallback",
			cacheTTL:    60 * time.Second,
			txExplicit:  true,
			initTx:      7 * time.Second,
			initBlock:   defaultCacheTTLBlock,
			initSubtree: defaultCacheTTLSubtree,
			initAnchor:  defaultCacheTTLAnchor,
			wantTx:      7 * time.Second,
			wantBlock:   defaultCacheTTLBlock,
			wantSubtree: defaultCacheTTLSubtree,
			wantAnchor:  defaultCacheTTLAnchor,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &Config{
				CacheTTL:        tc.cacheTTL,
				CacheTTLTx:      tc.initTx,
				CacheTTLBlock:   tc.initBlock,
				CacheTTLSubtree: tc.initSubtree,
				CacheTTLAnchor:  tc.initAnchor,
			}
			resolveCacheTTLs(c, tc.cacheTTLExplicit, tc.txExplicit, tc.blockExplicit, tc.subtreeExplicit, tc.anchorExplicit)

			if c.CacheTTLTx != tc.wantTx {
				t.Errorf("Tx = %s, want %s", c.CacheTTLTx, tc.wantTx)
			}
			if c.CacheTTLBlock != tc.wantBlock {
				t.Errorf("Block = %s, want %s", c.CacheTTLBlock, tc.wantBlock)
			}
			if c.CacheTTLSubtree != tc.wantSubtree {
				t.Errorf("Subtree = %s, want %s", c.CacheTTLSubtree, tc.wantSubtree)
			}
			if c.CacheTTLAnchor != tc.wantAnchor {
				t.Errorf("Anchor = %s, want %s", c.CacheTTLAnchor, tc.wantAnchor)
			}
		})
	}
}

func TestEnvDuration(t *testing.T) {
	t.Setenv("CFG_D", "750ms")
	if got := envDuration("CFG_D", time.Second); got != 750*time.Millisecond {
		t.Errorf("got %v", got)
	}
	t.Setenv("CFG_D", "bad")
	if got := envDuration("CFG_D", time.Second); got != time.Second {
		t.Errorf("invalid → default: %v", got)
	}
}

func TestSplitCSV(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"a", []string{"a"}},
		{"a,b , c", []string{"a", "b", "c"}},
		{",,", nil},
	}
	for _, tc := range cases {
		got := splitCSV(tc.in)
		if len(got) != len(tc.want) {
			t.Errorf("splitCSV(%q) = %v, want %v", tc.in, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("splitCSV(%q)[%d] = %q, want %q", tc.in, i, got[i], tc.want[i])
			}
		}
	}
}

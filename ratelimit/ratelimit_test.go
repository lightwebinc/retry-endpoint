package ratelimit

import (
	"net"
	"testing"
	"time"
)

func TestIPRateLimit(t *testing.T) {
	l := New(Config{
		IPRate:      2,
		IPBurst:     2,
		SequenceMax: 100,
	})

	ip := net.ParseIP("::1")

	// First two should pass (burst).
	for i := uint64(0); i < 2; i++ {
		ok, _ := l.Allow(ip, i)
		if !ok {
			t.Fatalf("request %d should be allowed within burst", i)
		}
	}

	// Third should be dropped.
	ok, level := l.Allow(ip, 99)
	if ok {
		t.Fatal("expected IP rate limit to drop request")
	}
	if level != LevelIP {
		t.Fatalf("expected level %q, got %q", LevelIP, level)
	}
}

func TestSequenceRateLimit(t *testing.T) {
	l := New(Config{
		IPRate:      1000,
		IPBurst:     1000,
		SequenceMax: 3,
	})

	ip := net.ParseIP("::1")
	const seqID = uint64(0xdeadbeef01020304)

	// First three requests for same seqID should pass.
	for i := 0; i < 3; i++ {
		ok, _ := l.Allow(ip, seqID)
		if !ok {
			t.Fatalf("request %d should be allowed within sequence max", i)
		}
	}

	// Fourth should be dropped.
	ok, level := l.Allow(ip, seqID)
	if ok {
		t.Fatal("expected sequence rate limit to drop request")
	}
	if level != LevelSequence {
		t.Fatalf("expected level %q, got %q", LevelSequence, level)
	}
}

func TestDifferentSequencesIndependent(t *testing.T) {
	l := New(Config{
		IPRate:      1000,
		IPBurst:     1000,
		SequenceMax: 1,
	})

	ip := net.ParseIP("::1")

	// Each unique SeqNum gets its own counter.
	for i := uint64(0); i < 5; i++ {
		ok, _ := l.Allow(ip, i<<32|0xdeadbeef)
		if !ok {
			t.Fatalf("seqID %d should be allowed (first request)", i)
		}
	}
}

// TestSequenceLimiterSlidingWindow guards against regressing to the legacy
// monotonic-counter behaviour: once a SequenceID's window elapses, new
// requests must be admitted again rather than permanently blocked.
func TestSequenceLimiterSlidingWindow(t *testing.T) {
	l := New(Config{
		IPRate:         1000,
		IPBurst:        1000,
		SequenceMax:    2,
		SequenceWindow: 50 * time.Millisecond,
	})

	ip := net.ParseIP("::1")
	const seqID = uint64(0xaabbccdd11223344)

	// Fill the window.
	for i := 0; i < 2; i++ {
		if ok, _ := l.Allow(ip, seqID); !ok {
			t.Fatalf("request %d should be allowed within window", i)
		}
	}
	// Third in the same window must be dropped.
	if ok, level := l.Allow(ip, seqID); ok || level != LevelSequence {
		t.Fatalf("expected sequence drop, got ok=%v level=%q", ok, level)
	}

	// After the window elapses the limiter must self-heal.
	time.Sleep(75 * time.Millisecond)
	if ok, _ := l.Allow(ip, seqID); !ok {
		t.Fatal("expected sequence limiter to admit requests after window expiry")
	}
}

// TestSequenceLimiterDefaultWindow verifies that a zero-value SequenceWindow
// is replaced with a sane default instead of locking the limiter open/closed.
func TestSequenceLimiterDefaultWindow(t *testing.T) {
	l := New(Config{
		IPRate:      1000,
		IPBurst:     1000,
		SequenceMax: 1,
		// SequenceWindow deliberately left zero.
	})
	ip := net.ParseIP("::1")
	const seqID = uint64(0x0102030405060708)
	if ok, _ := l.Allow(ip, seqID); !ok {
		t.Fatal("first request must pass regardless of window default")
	}
	if ok, level := l.Allow(ip, seqID); ok || level != LevelSequence {
		t.Fatalf("second request must be sequence-limited; got ok=%v level=%q", ok, level)
	}
}

// ── Chain limiter tests ───────────────────────────────────────────────────────

func TestChainRateLimit(t *testing.T) {
	l := New(Config{
		IPRate:      1e9,
		IPBurst:     1_000_000,
		ChainRate:   2,
		ChainWindow: time.Minute,
		SequenceMax: 1_000_000,
	})

	ip := net.ParseIP("::1")
	const hashKey = uint64(0xdeadbeef00000001)

	// First two calls within window should pass.
	for i := 0; i < 2; i++ {
		if !l.AllowChain(ip, hashKey) {
			t.Fatalf("chain call %d should be allowed within window", i)
		}
	}
	// Third call must be blocked.
	if l.AllowChain(ip, hashKey) {
		t.Error("expected chain rate limit to block request after window exhausted")
	}
}

func TestChainRateLimit_DifferentChains_Independent(t *testing.T) {
	l := New(Config{
		IPRate:      1e9,
		IPBurst:     1_000_000,
		ChainRate:   1,
		ChainWindow: time.Minute,
		SequenceMax: 1_000_000,
	})
	ip := net.ParseIP("::1")

	// Each distinct hashKey gets its own window.
	for i := uint64(1); i <= 5; i++ {
		if !l.AllowChain(ip, i) {
			t.Fatalf("first call for chain %d should be allowed", i)
		}
	}
}

func TestHashKeyZeroSkip(t *testing.T) {
	l := New(Config{
		IPRate:      1e9,
		IPBurst:     1_000_000,
		ChainRate:   1, // extremely tight limit
		ChainWindow: time.Minute,
		SequenceMax: 1_000_000,
	})
	ip := net.ParseIP("::1")
	const hashKey = uint64(0) // zero = orphan gap, not yet chain-attributed

	// HashKey == 0 must bypass the chain limiter; bucketing all unstamped
	// gaps together would cause premature rate limiting of distinct gaps.
	for i := 0; i < 10; i++ {
		if !l.AllowChain(ip, hashKey) {
			t.Fatalf("call %d with hashKey=0 must bypass chain limiter (orphan gap)", i)
		}
	}
}

func TestChainRateLimit_DifferentIPs_Independent(t *testing.T) {
	l := New(Config{
		IPRate:      1e9,
		IPBurst:     1_000_000,
		ChainRate:   1,
		ChainWindow: time.Minute,
		SequenceMax: 1_000_000,
	})
	const hashKey = uint64(0xaaaa)

	// Exhaust chain limit from ip1.
	ip1 := net.ParseIP("::1")
	if !l.AllowChain(ip1, hashKey) {
		t.Fatal("first call ip1 should pass")
	}
	if l.AllowChain(ip1, hashKey) {
		t.Fatal("second call ip1 should be rate limited")
	}

	// ip2 has an independent counter for the same hashKey.
	ip2 := net.ParseIP("::2")
	if !l.AllowChain(ip2, hashKey) {
		t.Fatal("first call ip2 should be allowed (independent counter)")
	}
}

// ── Group limiter tests ───────────────────────────────────────────────────────

func TestGroupRateLimit(t *testing.T) {
	l := New(Config{
		IPRate:      1e9,
		IPBurst:     1_000_000,
		SequenceMax: 1_000_000,
		GroupRate:   2,
		GroupBurst:  2,
	})
	ip := net.ParseIP("::1")
	const groupIdx = uint32(7)

	// Burst of 2 should pass.
	for i := 0; i < 2; i++ {
		if !l.AllowGroup(ip, groupIdx) {
			t.Fatalf("group call %d should be allowed within burst", i)
		}
	}
	// Third call must be blocked (burst exhausted, rate too slow).
	if l.AllowGroup(ip, groupIdx) {
		t.Error("expected group rate limit to block request after burst exhausted")
	}
}

func TestGroupRateLimit_DifferentGroups_Independent(t *testing.T) {
	l := New(Config{
		IPRate:      1e9,
		IPBurst:     1_000_000,
		SequenceMax: 1_000_000,
		GroupRate:   1,
		GroupBurst:  1,
	})
	ip := net.ParseIP("::1")

	// Each distinct groupIdx gets its own bucket.
	for g := uint32(0); g < 5; g++ {
		if !l.AllowGroup(ip, g) {
			t.Fatalf("first call for group %d should be allowed", g)
		}
	}
}

func TestGroupRateLimit_SenderAlias(t *testing.T) {
	// SenderRate/SenderWindow are backward-compat aliases for ChainRate/ChainWindow.
	l := New(Config{
		IPRate:       1e9,
		IPBurst:      1_000_000,
		SenderRate:   3,
		SenderWindow: time.Minute,
		SequenceMax:  1_000_000,
	})
	ip := net.ParseIP("::1")
	const hashKey = uint64(0xbeef)

	// SenderRate=3 → ChainRate=3; three calls should pass.
	for i := 0; i < 3; i++ {
		if !l.AllowChain(ip, hashKey) {
			t.Fatalf("call %d should pass (SenderRate aliased as ChainRate=3)", i)
		}
	}
	if l.AllowChain(ip, hashKey) {
		t.Error("fourth call should be blocked")
	}
}

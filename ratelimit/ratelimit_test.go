package ratelimit

import (
	"math/rand/v2"
	"net"
	"runtime"
	"slices"
	"testing"
	"time"
)

// testFlowKey is a stand-in HashKey: the sequence tier is scoped per flow, so a
// test that means to exercise the tier must hold the flow constant.
const testFlowKey uint64 = 0xABCD0123DEADBEEF

func TestIPRateLimit(t *testing.T) {
	l := New(Config{
		IPRate:      2,
		IPBurst:     2,
		SequenceMax: 100,
	})

	ip := net.ParseIP("::1")

	// First two should pass (burst).
	for i := uint64(0); i < 2; i++ {
		ok, _ := l.Allow(ip, testFlowKey, i)
		if !ok {
			t.Fatalf("request %d should be allowed within burst", i)
		}
	}

	// Third should be dropped.
	ok, level := l.Allow(ip, testFlowKey, 99)
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
		ok, _ := l.Allow(ip, testFlowKey, seqID)
		if !ok {
			t.Fatalf("request %d should be allowed within sequence max", i)
		}
	}

	// Fourth should be dropped.
	ok, level := l.Allow(ip, testFlowKey, seqID)
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
		ok, _ := l.Allow(ip, testFlowKey, i<<32|0xdeadbeef)
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
		if ok, _ := l.Allow(ip, testFlowKey, seqID); !ok {
			t.Fatalf("request %d should be allowed within window", i)
		}
	}
	// Third in the same window must be dropped.
	if ok, level := l.Allow(ip, testFlowKey, seqID); ok || level != LevelSequence {
		t.Fatalf("expected sequence drop, got ok=%v level=%q", ok, level)
	}

	// After the window elapses the limiter must self-heal.
	time.Sleep(75 * time.Millisecond)
	if ok, _ := l.Allow(ip, testFlowKey, seqID); !ok {
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
	if ok, _ := l.Allow(ip, testFlowKey, seqID); !ok {
		t.Fatal("first request must pass regardless of window default")
	}
	if ok, level := l.Allow(ip, testFlowKey, seqID); ok || level != LevelSequence {
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

// Two different flows repairing the SAME SeqNum must not share a bucket. Every
// flow's counter starts at 1, so a bare-SeqNum bucket was shared fabric-wide:
// ~100 junk NACKs a minute for a low sequence number exhausted repair of that
// sequence for every flow the endpoint served — targeted repair-denial for
// about 107 bytes a second, and self-inflicted contention at scale.
func TestSequenceTierIsScopedPerFlow(t *testing.T) {
	l := New(Config{
		IPRate: 1e9, IPBurst: 1_000_000,
		ChainRate: 1e9, ChainWindow: time.Second,
		SequenceMax: 2, SequenceWindow: time.Minute,
		GroupRate: 1e9, GroupBurst: 1_000_000,
	})
	ip := net.ParseIP("::1")
	const seq uint64 = 1 // the value every flow starts at

	// Exhaust flow A's budget for this SeqNum.
	for i := 0; i < 2; i++ {
		if ok, _ := l.Allow(ip, 0xAAAA, seq); !ok {
			t.Fatalf("flow A request %d rejected while under budget", i)
		}
	}
	if ok, level := l.Allow(ip, 0xAAAA, seq); ok || level != LevelSequence {
		t.Fatalf("flow A not limited after its budget: ok=%v level=%q", ok, level)
	}

	// An unrelated flow at the same SeqNum must be unaffected.
	if ok, level := l.Allow(ip, 0xBBBB, seq); !ok {
		t.Fatalf("flow B starved by flow A's budget at the same SeqNum (level=%q) — repair-denial across the fabric", level)
	}
}

// The sequence map is keyed by unauthenticated NACK fields, so it must not grow
// without bound: entries whose window has fully aged out are swept.
func TestSequenceLimiterEvictsExpiredKeys(t *testing.T) {
	sl := newSlidingWindowLimiter[seqKey](1, 10*time.Millisecond, maxWindowKeys)
	for i := 0; i < 1000; i++ {
		sl.Allow(seqKey{hashKey: uint64(i), seqNum: uint64(i)})
	}
	if len(sl.keys) != 1000 {
		t.Fatalf("pre-eviction size = %d, want 1000", len(sl.keys))
	}
	time.Sleep(20 * time.Millisecond)
	sl.mu.Lock()
	sl.evictExpiredLocked(monoNow())
	n := len(sl.keys)
	sl.mu.Unlock()
	if n != 0 {
		t.Errorf("expired keys survived eviction: %d remain", n)
	}
}

// allowAt is Allow on a synthetic clock. Calls for one key must not go back
// in time, as with the real clock read under the lock.
func allowAt[K comparable](r *slidingWindowLimiter[K], key K, now int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.allowLocked(key, now)
}

// allowChainAt is AllowChain on a synthetic clock, for the benchmarks.
func allowChainAt(l *Limiter, ip net.IP, hashKey uint64, now int64) bool {
	return allowAt(l.chainLimiter, chainKey{ip: ipKey(ip), hashKey: hashKey}, now)
}

// The event log must decide exactly as filtering and counting every stored
// timestamp does: at most limit events per key in any window, re-admitting as
// events age out.
func TestSlidingWindowMatchesRescan(t *testing.T) {
	const window = 1000
	for _, limit := range []int{0, 1, 3, 50, 1000} {
		r := newSlidingWindowLimiter[int](limit, window, maxWindowKeys)
		rng := rand.New(rand.NewPCG(uint64(limit), 7))
		var live []int64 // the rescan's own record of admitted events
		var now int64
		for i := 0; i < 50_000; i++ {
			// Alternate phases near the limit and well under it, so the log
			// both fills and drains far below its peak.
			spread := int64(2)
			if i/5000%2 == 1 {
				spread = 20
			}
			switch n := rng.IntN(1000); {
			case n == 0: // idle past a whole window
				now += window + rng.Int64N(window)
			case n < 100: // same instant
			default:
				now += rng.Int64N(spread*window/int64(limit+1) + 1)
			}
			live = slices.DeleteFunc(live, func(ts int64) bool { return ts <= now-window })
			want := len(live) < limit
			if want {
				live = append(live, now)
			}
			if got := allowAt(r, 0, now); got != want {
				t.Fatalf("limit=%d event %d at %d: allow = %v, rescan says %v", limit, i, now, got, want)
			}
		}
	}
}

// A configured max must not reserve memory for each key: a log grows only
// with the events its key admits, so a new key costs a few small objects even
// at max = 1e9, where reserving max timestamps would take about 24 GB.
func TestHugeMaxAllocatesLittlePerKey(t *testing.T) {
	l := New(Config{
		IPRate: 1e9, IPBurst: 1_000_000,
		ChainRate: 1e9, ChainWindow: time.Minute,
		SequenceMax: 1_000_000_000, SequenceWindow: time.Minute,
	})
	ip := net.ParseIP("2001:db8::1")
	var h uint64
	tiers := map[string]func(){
		"chain": func() {
			h++
			if !l.AllowChain(ip, h) {
				t.Fatal("new chain key rejected")
			}
		},
		"sequence": func() {
			h++
			if !l.sequenceLimiter.Allow(seqKey{hashKey: h, seqNum: h}) {
				t.Fatal("new sequence key rejected")
			}
		},
	}
	for name, newKey := range tiers {
		// The event log and its first array, plus map growth amortized.
		allocs := testing.AllocsPerRun(1000, newKey)
		if allocs > 3 {
			t.Errorf("%s: %.0f allocs per new key, want at most 3", name, allocs)
		}
		const keys = 10_000
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		for i := 0; i < keys; i++ {
			newKey()
		}
		runtime.ReadMemStats(&after)
		per := (after.TotalAlloc - before.TotalAlloc) / keys
		if per > 256 {
			t.Errorf("%s: %d bytes allocated per new key, want at most 256", name, per)
		}
		t.Logf("%s: %.0f allocs, %d bytes per new key", name, allocs, per)
	}
}

// The chain map is keyed by an unauthenticated (srcIP, HashKey) pair, so like
// the sequence map it is swept of aged-out keys when it reaches its bound.
// Live keys stay, and the sweep then waits for the map to double, so key
// diversity cannot force a full sweep on every new key.
func TestChainLimiterBoundsKeys(t *testing.T) {
	const bound = 100
	sec := int64(time.Second)
	r := newSlidingWindowLimiter[chainKey](1, time.Second, bound)
	ip := ipKey(net.ParseIP("2001:db8::1"))
	key := func(i int) chainKey { return chainKey{ip: ip, hashKey: uint64(i)} }

	for i := 0; i < bound; i++ {
		allowAt(r, key(i), 0)
	}
	// At the bound with every key live: nothing is reclaimed, and every
	// decision stands.
	if !allowAt(r, key(bound), sec/2) {
		t.Fatal("new key rejected at the bound")
	}
	if n := len(r.keys); n != bound+1 {
		t.Fatalf("map = %d keys, want %d (live keys kept)", n, bound+1)
	}
	if allowAt(r, key(0), sec/2) {
		t.Fatal("a live key lost its window at the sweep")
	}
	// Further new keys do not sweep again until the map doubles.
	for i := bound + 1; i < 2*bound; i++ {
		allowAt(r, key(i), sec/2)
	}
	if n := len(r.keys); n != 2*bound {
		t.Fatalf("map = %d keys, want %d", n, 2*bound)
	}
	// Once those windows age out, the next new key sweeps them all.
	allowAt(r, key(2*bound), 2*sec)
	if n := len(r.keys); n != 1 {
		t.Fatalf("map = %d keys after aged-out keys were swept, want 1", n)
	}
	if r.sweepAt != bound {
		t.Fatalf("next sweep at %d keys, want the bound %d", r.sweepAt, bound)
	}

	if l := New(Config{}); l.chainLimiter.maxKeys != maxWindowKeys || l.sequenceLimiter.maxKeys != maxWindowKeys {
		t.Fatal("New does not bound the chain and sequence maps")
	}
}

// A burst's log array is given back once the burst ages out.
func TestEventLogShrinksAfterBurst(t *testing.T) {
	ms := int64(time.Millisecond)
	r := newSlidingWindowLimiter[int](20_000, time.Second, maxWindowKeys)
	var now int64
	for i := 0; i < 20_000; i++ {
		allowAt(r, 1, now) // a 20 000-event burst on key 1
		allowAt(r, 2, now) // and on key 2
		now += 5000
	}
	// Key 1 stays active at a low rate; key 2 goes quiet.
	for i := 0; i < 30; i++ {
		now += 100 * ms
		allowAt(r, 1, now)
	}
	allowAt(r, 2, now)
	for k, l := range r.keys {
		if c := cap(l.ts); c > logShrinkCap {
			t.Errorf("key %d keeps cap %d for %d live events", k, c, len(l.ts)-l.head)
		}
	}
}

// An IPv4 source keys one bucket whether it arrives in 4-byte or IPv4-mapped
// form, as the string keys did.
func TestIPv4FormsShareBuckets(t *testing.T) {
	l := New(Config{
		IPRate: 1e9, IPBurst: 1_000_000,
		ChainRate: 1, ChainWindow: time.Minute,
		SequenceMax: 1_000_000,
		GroupRate:   1, GroupBurst: 1,
	})
	short := net.IPv4(192, 0, 2, 1).To4()
	mapped := net.ParseIP("192.0.2.1")
	if len(short) != net.IPv4len || len(mapped) != net.IPv6len {
		t.Fatal("test addresses are not in the intended forms")
	}
	if !l.AllowChain(short, 7) || l.AllowChain(mapped, 7) {
		t.Error("the two forms of one IPv4 address got separate chain buckets")
	}
	if !l.AllowGroup(short, 3) || l.AllowGroup(mapped, 3) {
		t.Error("the two forms of one IPv4 address got separate group buckets")
	}
	if !l.AllowChain(net.ParseIP("192.0.2.2"), 7) {
		t.Error("a different address shared the chain bucket")
	}
}

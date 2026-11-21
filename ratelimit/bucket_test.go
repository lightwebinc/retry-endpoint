package ratelimit

import (
	"math"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

// bucketAllowAt is Allow on a synthetic clock.
func bucketAllowAt[K comparable](tb *tokenBucketLimiter[K], key K, now time.Time) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return tb.allowLocked(key, now)
}

// A sweep drops exactly the buckets that have refilled. A bucket still short
// of full keeps its key, and with it every later decision. As in the sliding
// window, a size-triggered sweep that keeps live keys waits for the map to
// double, and a refill period after the last sweep a new key sweeps again.
func TestTokenBucketSweepsRefilledBuckets(t *testing.T) {
	const bound = 100
	tb := newTokenBucketLimiter[int](1, 2, bound) // empty to full in 2 s
	t0 := time.Unix(1_000_000, 0)
	at := func(d time.Duration) time.Time { return t0.Add(d) }

	// Key 0 drains its bucket; keys 1..bound-1 take one token each.
	bucketAllowAt(tb, 0, at(0))
	bucketAllowAt(tb, 0, at(0))
	if bucketAllowAt(tb, 0, at(0)) {
		t.Fatal("a drained bucket admitted a request")
	}
	for k := 1; k < bound; k++ {
		bucketAllowAt(tb, k, at(0))
	}
	// At the bound with no bucket refilled: nothing is reclaimed, and the
	// drained bucket stays drained.
	if !bucketAllowAt(tb, bound, at(500*time.Millisecond)) {
		t.Fatal("new key rejected at the bound")
	}
	if n := len(tb.limit); n != bound+1 {
		t.Fatalf("map = %d keys, want %d (no bucket had refilled)", n, bound+1)
	}
	if bucketAllowAt(tb, 0, at(500*time.Millisecond)) {
		t.Fatal("the sweep reset a drained bucket")
	}
	// Further new keys do not sweep again until the map doubles.
	for k := bound + 1; k < 2*bound; k++ {
		bucketAllowAt(tb, k, at(500*time.Millisecond))
	}
	if n := len(tb.limit); n != 2*bound {
		t.Fatalf("map = %d keys, want %d", n, 2*bound)
	}

	// A refill period after that sweep, every bucket is full again: the next
	// new key sweeps them all, rebuilds the sparse map and resets the bound.
	if !bucketAllowAt(tb, 2*bound, at(2600*time.Millisecond)) {
		t.Fatal("new key rejected")
	}
	if n := len(tb.limit); n != 1 {
		t.Fatalf("map = %d keys after the refilled buckets were swept, want 1", n)
	}
	if tb.sweepAt != bound {
		t.Errorf("next sweep at %d keys, want the bound %d", tb.sweepAt, bound)
	}
	if tb.peak != 1 {
		t.Errorf("peak = %d, want 1: the sparse map was not rebuilt", tb.peak)
	}

	// A bucket drained after the last sweep is not yet full at the next one,
	// so it keeps its 1.6 tokens: one admission, not the two a new bucket has.
	bucketAllowAt(tb, 7, at(3*time.Second))
	bucketAllowAt(tb, 7, at(3*time.Second))
	bucketAllowAt(tb, 2*bound+1, at(4600*time.Millisecond)) // 2 s after the last sweep
	if _, ok := tb.limit[7]; !ok {
		t.Fatal("a bucket short of full was swept")
	}
	if !bucketAllowAt(tb, 7, at(4600*time.Millisecond)) || bucketAllowAt(tb, 7, at(4600*time.Millisecond)) {
		t.Error("the kept bucket did not decide as before the sweep (1.6 tokens: admit one)")
	}

	if l := New(Config{}); l.ipLimiter.maxKeys != maxBucketKeys || l.groupLimiter.maxKeys != maxBucketKeys {
		t.Fatal("New does not bound the IP and group maps")
	}
}

// A bucket with a rate of zero never refills, so it cannot be swept as full:
// it goes once its key has been idle for bucketIdle, and the key then starts
// over. A key that keeps asking is never idle and stays refused.
func TestTokenBucketNonRefillingSweptWhenIdle(t *testing.T) {
	tb := newTokenBucketLimiter[int](0, 1, 100)
	t0 := time.Unix(1_000_000, 0)
	at := func(d time.Duration) time.Time { return t0.Add(d) }

	if !bucketAllowAt(tb, 1, at(0)) {
		t.Fatal("the burst was refused")
	}
	if bucketAllowAt(tb, 1, at(0)) {
		t.Fatal("a bucket with rate 0 refilled")
	}
	// Under a minute after the last sweep: no sweep, key 1 stays refused.
	bucketAllowAt(tb, 2, at(30*time.Second))
	if bucketAllowAt(tb, 1, at(30*time.Second)) {
		t.Fatal("a non-refilling bucket was reset before it went idle")
	}
	// Key 1 asked at 30 s; a minute later it is idle and goes.
	bucketAllowAt(tb, 3, at(91*time.Second))
	if _, ok := tb.limit[1]; ok {
		t.Fatal("an idle non-refilling bucket was kept")
	}
	if !bucketAllowAt(tb, 1, at(91*time.Second)) {
		t.Error("a key idle for bucketIdle did not start over")
	}
}

// The start-over must not depend on another key's arrival running a sweep: a
// lone source quiet for bucketIdle starts over at its own next request, and
// one that keeps asking stays refused.
func TestTokenBucketNonRefillingStartsOverWithoutSweep(t *testing.T) {
	tb := newTokenBucketLimiter[int](0, 1, 100)
	t0 := time.Unix(1_000_000, 0)
	at := func(d time.Duration) time.Time { return t0.Add(d) }

	if !bucketAllowAt(tb, 1, at(0)) || bucketAllowAt(tb, 1, at(0)) {
		t.Fatal("want the burst of one, then a refusal")
	}
	for _, d := range []time.Duration{50 * time.Second, 100 * time.Second, 150 * time.Second} {
		if bucketAllowAt(tb, 1, at(d)) {
			t.Fatalf("a source asking every 50 s was admitted at %v: it is never idle for a minute", d)
		}
	}
	if !bucketAllowAt(tb, 1, at(211*time.Second)) {
		t.Error("a lone source quiet for a minute was still refused (no other key ran a sweep)")
	}
}

func TestRefillTime(t *testing.T) {
	for _, tc := range []struct {
		r     rate.Limit
		burst int
		want  time.Duration
	}{
		{1, 2, 2 * time.Second},
		{100, 10, 100 * time.Millisecond},
		{200, 50, 250 * time.Millisecond},
		{rate.Inf, 10, 0},
		{100, 0, 0},
		{0, 10, bucketIdle},
		{-1, 10, bucketIdle},
		{1e-9, math.MaxInt32, bucketIdle}, // would overflow a Duration
		{rate.Limit(math.NaN()), 10, bucketIdle},
	} {
		if got := refillTime(tc.r, tc.burst); got != tc.want {
			t.Errorf("refillTime(%v, %d) = %v, want %v", tc.r, tc.burst, got, tc.want)
		}
	}
}

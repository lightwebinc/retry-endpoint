// Package ratelimit provides four-tier rate limiting for NACK requests:
//
//  1. Per-IP token bucket (overall flood protection).
//  2. Per-SeqNum sliding window (per-gap retry cap).
//  3. Per-(srcIP, HashKey) sliding window (per-flow NACK storm cap).
//  4. Per-(srcIP, groupIdx) token bucket (post-lookup retransmit bandwidth cap).
//
// Tiers 1-3 are pre-lookup (call Allow + AllowChain before cache access).
// Tier 4 is post-lookup (call AllowGroup after cache hit, before Retransmit).
package ratelimit

import (
	"net"
	"net/netip"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// Level represents the rate limiting tier that rejected a request.
type Level string

const (
	LevelIP       Level = "ip"
	LevelChain    Level = "chain"
	LevelSequence Level = "sequence"
	LevelGroup    Level = "group"
)

// Limiter provides four-tier rate limiting.
type Limiter struct {
	ipLimiter       *tokenBucketLimiter[string]
	chainLimiter    *slidingWindowLimiter[chainKey]
	sequenceLimiter *slidingWindowLimiter[seqKey]
	groupLimiter    *tokenBucketLimiter[groupKey]
}

// Config holds rate limiting configuration.
type Config struct {
	IPRate         float64       // Tokens per second per source IP
	IPBurst        int           // Burst size per source IP
	SenderRate     float64       // Alias for ChainRate (backward-compat)
	SenderWindow   time.Duration // Alias for ChainWindow (backward-compat)
	ChainRate      float64       // Max NACKs per ChainWindow per (srcIP, HashKey)
	ChainWindow    time.Duration // Sliding window for chain limiter
	SequenceMax    int           // Max requests per SeqNum per SequenceWindow
	SequenceWindow time.Duration // Sliding window for sequence limiter
	GroupRate      float64       // Retransmits per second per (srcIP, groupIdx)
	GroupBurst     int           // Burst size per (srcIP, groupIdx)
}

// New constructs a new Limiter.
func New(cfg Config) *Limiter {
	if cfg.SequenceWindow <= 0 {
		cfg.SequenceWindow = time.Minute
	}
	// SenderRate/SenderWindow are aliases for ChainRate/ChainWindow.
	if cfg.ChainRate == 0 && cfg.SenderRate != 0 {
		cfg.ChainRate = cfg.SenderRate
	}
	if cfg.ChainWindow <= 0 && cfg.SenderWindow > 0 {
		cfg.ChainWindow = cfg.SenderWindow
	}
	if cfg.ChainWindow <= 0 {
		cfg.ChainWindow = time.Minute
	}
	chainMax := int(cfg.ChainRate)
	if chainMax <= 0 {
		chainMax = 500
	}
	groupRate := cfg.GroupRate
	if groupRate <= 0 {
		groupRate = 200
	}
	groupBurst := cfg.GroupBurst
	if groupBurst <= 0 {
		groupBurst = 50
	}
	return &Limiter{
		ipLimiter:       newTokenBucketLimiter[string](cfg.IPRate, cfg.IPBurst),
		chainLimiter:    newSlidingWindowLimiter[chainKey](chainMax, cfg.ChainWindow, maxWindowKeys),
		sequenceLimiter: newSlidingWindowLimiter[seqKey](cfg.SequenceMax, cfg.SequenceWindow, maxWindowKeys),
		groupLimiter:    newTokenBucketLimiter[groupKey](groupRate, groupBurst),
	}
}

// Allow checks the IP and sequence tiers (pre-lookup).
// srcIP is the listener source address; hashKey/startSeq are the HashKey and
// StartSeq fields from the NACK datagram. Returns (true, "") if allowed.
//
// The sequence tier is scoped to the FLOW (hashKey, startSeq), not to the bare
// SeqNum. Every flow's counter starts at 1, so a bare-SeqNum bucket is shared by
// every flow on the fabric that is repairing the same counter value: ~100 junk
// NACKs a minute for a low sequence number exhausted repair of that sequence for
// EVERY flow this endpoint serves, which is a targeted repair-denial for about
// 107 bytes a second. It also self-inflicts at scale, since thousands of
// legitimate flows contend for one bucket per hot low SeqNum.
//
// hashKey 0 (an unstamped frame) falls back to the bare SeqNum: those gaps
// cannot be attributed to a flow, and bucketing them together is the same
// deliberate choice AllowChain makes for the same reason.
func (r *Limiter) Allow(srcIP net.IP, hashKey, startSeq uint64) (bool, Level) {
	if !r.ipLimiter.Allow(srcIP.String()) {
		return false, LevelIP
	}
	if !r.sequenceLimiter.Allow(seqKey{hashKey: hashKey, seqNum: startSeq}) {
		return false, LevelSequence
	}
	return true, ""
}

// AllowChain checks the chain tier (pre-lookup, after the IP and sequence tiers).
// hashKey is the HashKey field from the NACK datagram. 0 means the frame was
// not stamped by the proxy (unstamped); rate-limiting on HashKey=0 would
// bucket all such unattributed gaps together and prematurely exhaust a shared
// limit, so the check is skipped.
func (r *Limiter) AllowChain(srcIP net.IP, hashKey uint64) bool {
	if hashKey == 0 {
		return true
	}
	return r.chainLimiter.Allow(chainKey{ip: ipKey(srcIP), hashKey: hashKey})
}

// AllowGroup checks the group tier (post-lookup, before Retransmit).
// groupIdx is derived from the frame's TxID. Returns true if the retransmit
// should proceed; false means throttle the retransmit (answered with
// THROTTLED when the throttle response is enabled, silence otherwise).
func (r *Limiter) AllowGroup(srcIP net.IP, groupIdx uint32) bool {
	return r.groupLimiter.Allow(groupKey{ip: ipKey(srcIP), groupIdx: groupIdx})
}

// ── keys ──────────────────────────────────────────────────────────────────────

// seqKey scopes a sequence bucket to one flow. See Limiter.Allow.
type seqKey struct {
	hashKey uint64
	seqNum  uint64
}

// chainKey scopes a chain bucket to one (srcIP, HashKey) pair.
type chainKey struct {
	ip      [16]byte
	hashKey uint64
}

// groupKey scopes a group bucket to one (srcIP, groupIdx) pair.
type groupKey struct {
	ip       [16]byte
	groupIdx uint32
}

// ipKey returns ip in 16-byte form. An IPv4 address keys alike in its 4-byte
// and IPv4-mapped forms, as it did when keys were built from net.IP.String,
// and an invalid IP keys as the unspecified address.
func ipKey(ip net.IP) [16]byte {
	addr, _ := netip.AddrFromSlice(ip)
	return addr.As16()
}

// ── token bucket (per comparable key) ─────────────────────────────────────────

type tokenBucketLimiter[K comparable] struct {
	mu    sync.Mutex
	limit map[K]*rate.Limiter
	r     rate.Limit
	burst int
}

func newTokenBucketLimiter[K comparable](tokensPerSec float64, burst int) *tokenBucketLimiter[K] {
	return &tokenBucketLimiter[K]{
		limit: make(map[K]*rate.Limiter),
		r:     rate.Limit(tokensPerSec),
		burst: burst,
	}
}

func (t *tokenBucketLimiter[K]) Allow(key K) bool {
	t.mu.Lock()
	l, ok := t.limit[key]
	if !ok {
		l = rate.NewLimiter(t.r, t.burst)
		t.limit[key] = l
	}
	t.mu.Unlock()
	return l.Allow()
}

// ── sliding window (per comparable key) ───────────────────────────────────────

// maxWindowKeys bounds each sliding-window map. It is reached only under key
// diversity far above any real flow count, at which point keys whose window
// has fully aged out are swept.
const maxWindowKeys = 100_000

// logInitCap is the capacity a key's event log starts with. A log grows by
// append as its key admits events, so a high max costs memory only for the
// events a key actually has in its window.
const logInitCap = 4

// logShrinkCap is the capacity above which a log moves to a smaller array
// once its live events fill under an eighth of it, so a burst does not pin
// its peak memory for the life of the key.
const logShrinkCap = 256

// slidingWindowLimiter admits at most max events per key in any window,
// keeping an exact log of the events each key admitted within the window.
//
// The sequence tier once used a monotonic counter per SequenceID with no
// expiration, so a long-lived flow eventually exhausted its counter and lost
// every later NACK until the process restarted. The sliding window bounds
// memory and self-heals: a key that has been quiet for a window is admitted
// again at full capacity.
type slidingWindowLimiter[K comparable] struct {
	mu      sync.Mutex
	keys    map[K]*eventLog
	max     int
	window  int64 // nanoseconds
	maxKeys int   // map size at which aged-out keys are first swept
	sweepAt int   // map size that triggers the next sweep
}

func newSlidingWindowLimiter[K comparable](max int, window time.Duration, maxKeys int) *slidingWindowLimiter[K] {
	return &slidingWindowLimiter[K]{
		keys:    make(map[K]*eventLog),
		max:     max,
		window:  int64(window),
		maxKeys: maxKeys,
		sweepAt: maxKeys,
	}
}

// Allow records an event for key now and reports whether it is within the
// limit.
func (r *slidingWindowLimiter[K]) Allow(key K) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Read the clock under the lock, so each log is appended in nondecreasing
	// order, which expiring by prefix relies on.
	return r.allowLocked(key, monoNow())
}

// allowLocked is Allow at time now. Callers hold r.mu.
func (r *slidingWindowLimiter[K]) allowLocked(key K, now int64) bool {
	l, ok := r.keys[key]
	if !ok {
		// Bound the map before inserting. Keys are attacker-influenced (they
		// come from unauthenticated NACK fields and a spoofable source
		// address), and nothing else evicts them, so key diversity alone would
		// grow the map without limit for the life of the process. Sweeping
		// keys whose window has fully aged out reclaims every key that is no
		// longer rate-limiting anything.
		if len(r.keys) >= r.sweepAt {
			r.evictExpiredLocked(now)
		}
		l = &eventLog{}
		r.keys[key] = l
	}
	return l.allow(now, r.window, r.max)
}

// evictExpiredLocked drops keys whose whole window has aged out. Callers hold
// r.mu. Such a key is rate-limiting nothing, so removing it changes no
// decision; it only reclaims a key that an unauthenticated NACK was able to
// create. Live keys must stay, so the map can remain above maxKeys; the next
// sweep then waits until the map has doubled, which keeps sweeping O(1)
// amortized per new key instead of a full scan for each one.
func (r *slidingWindowLimiter[K]) evictExpiredLocked(now int64) {
	cutoff := now - r.window
	for k, l := range r.keys {
		if l.idle(cutoff) {
			delete(r.keys, k)
		}
	}
	r.sweepAt = max(r.maxKeys, 2*len(r.keys))
}

// eventLog is one key's admitted events as monotonic nanosecond timestamps in
// nondecreasing order. ts[:head] have expired.
type eventLog struct {
	ts   []int64
	head int
}

// allow expires the events at or before now-window, then records an event at
// now and reports true if fewer than max remain. now must not precede the
// last recorded event.
func (l *eventLog) allow(now, window int64, max int) bool {
	l.expire(now - window)
	if len(l.ts)-l.head >= max {
		return false
	}
	if l.ts == nil {
		l.ts = make([]int64, 0, logInitCap)
	}
	l.ts = append(l.ts, now)
	return true
}

// expire drops the events at or before cutoff. Timestamps never decrease, so
// those events are a prefix: head steps past them, and once head passes half
// the slice the live events move to the front. Both are O(1) amortized per
// event.
func (l *eventLog) expire(cutoff int64) {
	n := len(l.ts)
	if n == 0 {
		return
	}
	if l.ts[n-1] <= cutoff {
		// The whole window has aged out.
		l.head = 0
		if cap(l.ts) > logShrinkCap {
			l.ts = nil
		} else {
			l.ts = l.ts[:0]
		}
		return
	}
	for l.ts[l.head] <= cutoff {
		l.head++
	}
	if 2*l.head < n {
		return
	}
	live := l.ts[l.head:]
	if cap(l.ts) > logShrinkCap && 8*len(live) < cap(l.ts) {
		l.ts = append(make([]int64, 0, 2*len(live)), live...)
	} else {
		l.ts = l.ts[:copy(l.ts, live)]
	}
	l.head = 0
}

// idle reports whether every event is at or before cutoff.
func (l *eventLog) idle(cutoff int64) bool {
	return len(l.ts) == 0 || l.ts[len(l.ts)-1] <= cutoff
}

// epoch anchors monoNow.
var epoch = time.Now()

// monoNow returns the nanoseconds since epoch on the monotonic clock, so a
// wall-clock step cannot move a window.
func monoNow() int64 { return int64(time.Since(epoch)) }

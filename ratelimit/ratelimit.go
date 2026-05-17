// Package ratelimit provides four-tier rate limiting for NACK requests:
//
//  1. Per-IP token bucket (overall flood protection).
//  2. Per-(srcIP, HashKey) sliding window (per-flow NACK storm cap).
//  3. Per-SeqNum sliding window (per-gap retry cap).
//  4. Per-(srcIP, groupIdx) token bucket (post-lookup retransmit bandwidth cap).
//
// Tiers 1-3 are pre-lookup (call Allow + AllowChain before cache access).
// Tier 4 is post-lookup (call AllowGroup after cache hit, before Retransmit).
package ratelimit

import (
	"fmt"
	"net"
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
	ipLimiter       *tokenBucketLimiter
	chainLimiter    *slidingWindowLimiter
	sequenceLimiter *sequenceLimiter
	groupLimiter    *tokenBucketLimiter
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
		ipLimiter:       newTokenBucketLimiter(cfg.IPRate, cfg.IPBurst),
		chainLimiter:    newSlidingWindowLimiter(chainMax, cfg.ChainWindow),
		sequenceLimiter: newSequenceLimiter(cfg.SequenceMax, cfg.SequenceWindow),
		groupLimiter:    newTokenBucketLimiter(groupRate, groupBurst),
	}
}

// Allow checks the IP and sequence tiers (pre-lookup).
// srcIP is the listener source address; startSeq is the StartSeq field
// from the NACK datagram (SeqNum of the missing frame). Returns (true, "") if allowed.
func (r *Limiter) Allow(srcIP net.IP, startSeq uint64) (bool, Level) {
	if !r.ipLimiter.Allow(srcIP.String()) {
		return false, LevelIP
	}
	if !r.sequenceLimiter.Allow(startSeq) {
		return false, LevelSequence
	}
	return true, ""
}

// AllowChain checks the chain tier (pre-lookup, between IP and sequence).
// hashKey is the HashKey field from the NACK datagram. 0 means the frame was
// not stamped by the proxy (unstamped); rate-limiting on HashKey=0 would
// bucket all such unattributed gaps together and prematurely exhaust a shared
// limit, so the check is skipped.
func (r *Limiter) AllowChain(srcIP net.IP, hashKey uint64) bool {
	if hashKey == 0 {
		return true
	}
	key := fmt.Sprintf("%s:%016x", srcIP.String(), hashKey)
	return r.chainLimiter.Allow(key)
}

// AllowGroup checks the group tier (post-lookup, before Retransmit).
// groupIdx is derived from the frame's TxID. Returns true if the retransmit
// should proceed; false means throttle the retransmit (but still send ACK).
func (r *Limiter) AllowGroup(srcIP net.IP, groupIdx uint32) bool {
	key := fmt.Sprintf("%s:%d", srcIP.String(), groupIdx)
	return r.groupLimiter.Allow(key)
}

// ── token bucket (per string key) ─────────────────────────────────────────────

type tokenBucketLimiter struct {
	mu    sync.Mutex
	limit map[string]*rate.Limiter
	r     rate.Limit
	burst int
}

func newTokenBucketLimiter(tokensPerSec float64, burst int) *tokenBucketLimiter {
	return &tokenBucketLimiter{
		limit: make(map[string]*rate.Limiter),
		r:     rate.Limit(tokensPerSec),
		burst: burst,
	}
}

func (t *tokenBucketLimiter) Allow(key string) bool {
	t.mu.Lock()
	l, ok := t.limit[key]
	if !ok {
		l = rate.NewLimiter(t.r, t.burst)
		t.limit[key] = l
	}
	t.mu.Unlock()
	return l.Allow()
}

// ── sliding window (per string key) ───────────────────────────────────────────

type slidingWindowLimiter struct {
	mu     sync.Mutex
	keys   map[string]*windowEntry
	max    int
	window time.Duration
}

type windowEntry struct {
	timestamps []time.Time
}

func newSlidingWindowLimiter(max int, window time.Duration) *slidingWindowLimiter {
	return &slidingWindowLimiter{
		keys:   make(map[string]*windowEntry),
		max:    max,
		window: window,
	}
}

func (r *slidingWindowLimiter) Allow(key string) bool {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.keys[key]
	if !ok {
		entry = &windowEntry{timestamps: make([]time.Time, 0, r.max)}
		r.keys[key] = entry
	}

	cutoff := now.Add(-r.window)
	validIdx := 0
	for _, ts := range entry.timestamps {
		if ts.After(cutoff) {
			entry.timestamps[validIdx] = ts
			validIdx++
		}
	}
	entry.timestamps = entry.timestamps[:validIdx]

	if len(entry.timestamps) >= r.max {
		return false
	}
	entry.timestamps = append(entry.timestamps, now)
	return true
}

// ── sliding window (per uint64 key, for SeqNum) ───────────────────────────────────────────

// sequenceLimiter provides sliding-window rate limiting per SeqNum (uint64).
//
// The legacy implementation used a monotonic counter per SequenceID with no
// expiration. That caused long-lived flows to eventually exhaust the counter
// and drop every subsequent NACK for that flow, without any way to recover
// short of restarting the process. The sliding-window form bounds memory and
// self-heals: a SequenceID that has been quiet for [window] is re-admitted
// at full capacity.
type sequenceLimiter struct {
	mu     sync.Mutex
	seqs   map[uint64]*windowEntry
	max    int
	window time.Duration
}

func newSequenceLimiter(max int, window time.Duration) *sequenceLimiter {
	return &sequenceLimiter{
		seqs:   make(map[uint64]*windowEntry),
		max:    max,
		window: window,
	}
}

func (r *sequenceLimiter) Allow(seqNum uint64) bool {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.seqs[seqNum]
	if !ok {
		entry = &windowEntry{timestamps: make([]time.Time, 0, r.max)}
		r.seqs[seqNum] = entry
	}

	cutoff := now.Add(-r.window)
	validIdx := 0
	for _, ts := range entry.timestamps {
		if ts.After(cutoff) {
			entry.timestamps[validIdx] = ts
			validIdx++
		}
	}
	entry.timestamps = entry.timestamps[:validIdx]

	if len(entry.timestamps) >= r.max {
		return false
	}
	entry.timestamps = append(entry.timestamps, now)
	return true
}

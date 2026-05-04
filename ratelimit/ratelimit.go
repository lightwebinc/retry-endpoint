// Package ratelimit provides three-level rate limiting for NACK requests:
// per-IP token bucket, per-senderID sliding window, per-sequenceID counter.
package ratelimit

import (
	"fmt"
	"net"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// Level represents the rate limiting level.
type Level string

const (
	LevelIP       Level = "ip"
	LevelSender   Level = "sender"
	LevelSequence Level = "sequence"
)

// Limiter provides three-level rate limiting.
type Limiter struct {
	ipLimiter       *ipLimiter
	senderLimiter   *senderLimiter
	sequenceLimiter *sequenceLimiter
}

// Config holds rate limiting configuration.
type Config struct {
	IPRate         float64       // Tokens per second
	IPBurst        int           // Burst size
	SenderRate     float64       // Requests per window
	SenderWindow   time.Duration // Sliding window duration
	SequenceMax    int           // Max requests per SequenceID per SequenceWindow
	SequenceWindow time.Duration // Sliding window duration for SequenceID limiter
}

// New constructs a new rate limiter.
func New(cfg Config) *Limiter {
	if cfg.SequenceWindow <= 0 {
		// Default to 1 minute if unset — matches SenderWindow default and
		// prevents the legacy "counter never resets" behaviour.
		cfg.SequenceWindow = time.Minute
	}
	return &Limiter{
		ipLimiter:       newIPLimiter(cfg.IPRate, cfg.IPBurst),
		senderLimiter:   newSenderLimiter(cfg.SenderRate, cfg.SenderWindow),
		sequenceLimiter: newSequenceLimiter(cfg.SequenceMax, cfg.SequenceWindow),
	}
}

// Allow checks if the request should be allowed based on all three levels.
// Returns (true, "") if allowed, (false, level) if rate limited.
func (r *Limiter) Allow(srcIP net.IP, senderID uint32, sequenceID uint32) (bool, Level) {
	if !r.ipLimiter.Allow(srcIP) {
		return false, LevelIP
	}
	if !r.senderLimiter.Allow(senderID) {
		return false, LevelSender
	}
	if !r.sequenceLimiter.Allow(sequenceID) {
		return false, LevelSequence
	}
	return true, ""
}

// ipLimiter provides token bucket rate limiting per source IP.
type ipLimiter struct {
	mu    sync.Mutex
	limit map[string]*rate.Limiter
	rate  rate.Limit
	burst int
}

func newIPLimiter(tokensPerSec float64, burst int) *ipLimiter {
	return &ipLimiter{
		limit: make(map[string]*rate.Limiter),
		rate:  rate.Limit(tokensPerSec),
		burst: burst,
	}
}

func (r *ipLimiter) Allow(ip net.IP) bool {
	key := ip.String()
	r.mu.Lock()
	limiter, ok := r.limit[key]
	if !ok {
		limiter = rate.NewLimiter(r.rate, r.burst)
		r.limit[key] = limiter
	}
	r.mu.Unlock()
	return limiter.Allow()
}

// senderLimiter provides sliding window rate limiting per SenderID.
type senderLimiter struct {
	mu      sync.Mutex
	senders map[string]*senderEntry
	rate    float64
	window  time.Duration
}

type senderEntry struct {
	timestamps []time.Time
}

func newSenderLimiter(ratePerSec float64, window time.Duration) *senderLimiter {
	return &senderLimiter{
		senders: make(map[string]*senderEntry),
		rate:    ratePerSec,
		window:  window,
	}
}

func (r *senderLimiter) Allow(senderID uint32) bool {
	key := fmt.Sprintf("%08x", senderID)
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.senders[key]
	if !ok {
		entry = &senderEntry{
			timestamps: make([]time.Time, 0),
		}
		r.senders[key] = entry
	}

	// Remove timestamps outside the window
	cutoff := now.Add(-r.window)
	validIdx := 0
	for _, ts := range entry.timestamps {
		if ts.After(cutoff) {
			entry.timestamps[validIdx] = ts
			validIdx++
		}
	}
	entry.timestamps = entry.timestamps[:validIdx]

	// Check if we're within the rate limit (rate = max requests per window).
	maxRequests := int(r.rate)
	if len(entry.timestamps) >= maxRequests {
		return false
	}

	entry.timestamps = append(entry.timestamps, now)
	return true
}

// sequenceLimiter provides sliding-window rate limiting per SequenceID.
//
// The legacy implementation used a monotonic counter per SequenceID with no
// expiration. That caused long-lived flows to eventually exhaust the counter
// and drop every subsequent NACK for that flow, without any way to recover
// short of restarting the process. The sliding-window form bounds memory and
// self-heals: a SequenceID that has been quiet for [window] is re-admitted
// at full capacity.
type sequenceLimiter struct {
	mu     sync.Mutex
	seqs   map[uint32]*sequenceEntry
	max    int
	window time.Duration
}

type sequenceEntry struct {
	timestamps []time.Time
}

func newSequenceLimiter(max int, window time.Duration) *sequenceLimiter {
	return &sequenceLimiter{
		seqs:   make(map[uint32]*sequenceEntry),
		max:    max,
		window: window,
	}
}

func (r *sequenceLimiter) Allow(sequenceID uint32) bool {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.seqs[sequenceID]
	if !ok {
		entry = &sequenceEntry{timestamps: make([]time.Time, 0, r.max)}
		r.seqs[sequenceID] = entry
	}

	// Drop timestamps outside the window. Expected working set is small
	// because the window is short relative to typical flow bursts.
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

package main

import (
	"context"
	"log/slog"
	"time"

	scache "github.com/lightwebinc/shard-common/cache"

	"github.com/lightwebinc/retry-endpoint/cache"
	"github.com/lightwebinc/retry-endpoint/config"
	"github.com/lightwebinc/retry-endpoint/proxy"
)

// newDedup builds the cross-instance dedup claims: the retransmit claim
// ("bre:dedup:") and the NACK proxy's in-flight claim ("bre:proxy:"). Both are
// nil when there is no shared backend to claim in. closeFn releases what
// newDedup opened.
//
// A claim sits on the repair path: the frame waits for it. So the claims run
// on a backend client of their own, opened with -dedup-claim-timeout as its
// per-operation timeout. The frame store's -cache-op-timeout does not apply:
// a client waits that long for a backend that stopped answering, and every
// repair behind the claim waited with it. Both claims also sit behind one
// Breaker: once a claim fails, claims are skipped for -dedup-breaker-window,
// then one probe decides whether they resume. A dead dedup backend costs one
// short wait per window once the claims in flight when it failed are out, and
// every claim it cannot answer fails open (the frame is sent without it).
//
// A redis/aerospike frame backend is itself cross-instance, so the claims go
// to the same server. They share the frame client when -dedup-claim-timeout
// is not shorter than -cache-op-timeout, or when a client of their own
// cannot be opened. With CACHE_BACKEND=memory (per-instance frames) they need
// a shared store of their own: a redis client opened from REDIS_ADDR if given.
func newDedup(cfg *config.Config, frames scache.Backend) (retrans cache.Deduper, prox proxy.Deduper, closeFn func()) {
	closeFn = func() {}
	timeout := cfg.DedupClaimTimeout
	if timeout <= 0 { // a Config not built by config.Load, which rejects zero
		timeout = config.DefaultDedupClaimTimeout
	}
	var claims scache.Backend
	own := false // claims run on a client of their own
	switch cfg.CacheBackend {
	case "redis", "aerospike":
		claims = frames
		if timeout >= cfg.CacheOpTimeout {
			break
		}
		b, err := scache.Open(context.Background(), claimConfig(cfg, cfg.CacheBackend, timeout))
		if err != nil {
			slog.Warn("dedup claim client unavailable; claims share the frame client and wait up to its -cache-op-timeout",
				"backend", cfg.CacheBackend, "err", err)
			break
		}
		claims, own, closeFn = b, true, func() { _ = b.Close() }
	case "memory":
		if cfg.RedisAddr == "" {
			return nil, nil, closeFn
		}
		b, err := scache.Open(context.Background(), claimConfig(cfg, scache.BackendRedis, timeout))
		if err != nil {
			slog.Warn("redis dedup unavailable, running without cross-instance dedup", "addr", cfg.RedisAddr, "err", err)
			return nil, nil, closeFn
		}
		claims, own, closeFn = b, true, func() { _ = b.Close() }
	default:
		return nil, nil, closeFn
	}
	slog.Info("cross-instance dedup enabled", "backend", cfg.CacheBackend,
		"claim_timeout", timeout, "breaker_window", cfg.DedupBreakerWindow, "own_client", own)
	br := cache.NewBreaker(cfg.DedupBreakerWindow)
	return cache.Guard(cache.NewStore(claims, "bre:dedup:", timeout), br),
		cache.Guard(cache.NewStore(claims, "bre:proxy:", timeout), br), closeFn
}

// claimConfig is the backend config of the claims' own client: the frame
// backend's server (or REDIS_ADDR beside the memory cache), with the claim
// timeout as its per-operation timeout.
func claimConfig(cfg *config.Config, backend string, timeout time.Duration) scache.Config {
	return scache.Config{
		Backend:       backend,
		RedisAddr:     cfg.RedisAddr,
		AeroHosts:     cfg.AeroHosts,
		AeroNamespace: cfg.AeroNamespace,
		AeroSet:       cfg.AeroSet,
		DialTimeout:   cfg.CacheDialTimeout,
		OpTimeout:     timeout,
	}
}

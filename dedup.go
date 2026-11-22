package main

import (
	"context"
	"log/slog"

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
// A redis/aerospike frame backend is itself cross-instance, so the claims
// reuse it. With CACHE_BACKEND=memory (per-instance frames) they still need a
// shared store: a separate redis backend is opened from REDIS_ADDR if given.
func newDedup(cfg *config.Config, frames scache.Backend) (retrans cache.Deduper, prox proxy.Deduper, closeFn func()) {
	closeFn = func() {}
	switch cfg.CacheBackend {
	case "redis", "aerospike":
		slog.Info("cross-instance dedup enabled", "backend", cfg.CacheBackend)
		return cache.NewStore(frames, "bre:dedup:", cfg.CacheOpTimeout),
			cache.NewStore(frames, "bre:proxy:", cfg.CacheOpTimeout), closeFn
	case "memory":
		if cfg.RedisAddr == "" {
			return nil, nil, closeFn
		}
		b, err := scache.Open(context.Background(), scache.Config{
			Backend:     scache.BackendRedis,
			RedisAddr:   cfg.RedisAddr,
			DialTimeout: cfg.CacheDialTimeout,
			OpTimeout:   cfg.CacheOpTimeout,
		})
		if err != nil {
			slog.Warn("redis dedup unavailable, running without cross-instance dedup", "addr", cfg.RedisAddr, "err", err)
			return nil, nil, closeFn
		}
		slog.Info("cross-instance dedup enabled", "addr", cfg.RedisAddr)
		return cache.NewStore(b, "bre:dedup:", cfg.CacheOpTimeout),
			cache.NewStore(b, "bre:proxy:", cfg.CacheOpTimeout), func() { _ = b.Close() }
	}
	return nil, nil, closeFn
}

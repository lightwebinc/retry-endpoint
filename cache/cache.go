// Package cache adapts the modular shard-common cache.Backend to the
// frame-store surface used by the retry endpoint. A [Store] namespaces a
// single backend under a fixed key prefix and supplies a per-op context, so
// the ingress worker (Store), NACK server (Retrieve), and retransmitter
// (SetNX dedup) can share one backend with independent prefixes.
//
// The backend lifecycle is owned by the caller (main); Store.Close is a no-op
// so multiple namespaced views can wrap the same backend safely.
package cache

import (
	"context"
	"time"

	scache "github.com/lightwebinc/shard-common/cache"
	"github.com/lightwebinc/shard-common/cache/memory"
)

// Cache is the frame-store surface consumed by the ingress worker and NACK
// server. Implemented by *Store.
type Cache interface {
	Store(key, value []byte, ttl time.Duration) error
	Retrieve(key []byte) ([]byte, error)
	Delete(key []byte) error
	Close() error
}

// Deduper is the subset used by the retransmitter for cross-instance dedup.
type Deduper interface {
	SetNX(key, value []byte, ttl time.Duration) (bool, error)
}

// Store namespaces a shard-common cache.Backend under a fixed key prefix.
type Store struct {
	b         scache.Backend
	prefix    []byte
	opTimeout time.Duration
	local     bool // in-process backend: operations never wait, so no deadline
}

// NewStore wraps backend b, prepending prefix to every key and bounding each
// operation by opTimeout (<=0 → 1s).
func NewStore(b scache.Backend, prefix string, opTimeout time.Duration) *Store {
	if opTimeout <= 0 {
		opTimeout = time.Second
	}
	_, local := b.(*memory.Backend)
	return &Store{b: b, prefix: []byte(prefix), opTimeout: opTimeout, local: local}
}

func (s *Store) key(k []byte) []byte {
	out := make([]byte, len(s.prefix)+len(k))
	copy(out, s.prefix)
	copy(out[len(s.prefix):], k)
	return out
}

func (s *Store) opCtx() (context.Context, context.CancelFunc) {
	// The in-memory backend never blocks, and a deadline costs a timer per
	// operation: at a stored frame per call that was an eighth of the store.
	if s.local {
		return context.Background(), func() {}
	}
	return context.WithTimeout(context.Background(), s.opTimeout)
}

// Store writes the frame value under key with ttl.
func (s *Store) Store(key, value []byte, ttl time.Duration) error {
	ctx, cancel := s.opCtx()
	defer cancel()
	return s.b.Set(ctx, s.key(key), value, ttl)
}

// Retrieve returns the frame value for key, or (nil, nil) on miss.
func (s *Store) Retrieve(key []byte) ([]byte, error) {
	ctx, cancel := s.opCtx()
	defer cancel()
	return s.b.Get(ctx, s.key(key))
}

// Delete removes key.
func (s *Store) Delete(key []byte) error {
	ctx, cancel := s.opCtx()
	defer cancel()
	return s.b.Del(ctx, s.key(key))
}

// SetNX atomically creates key=value with ttl iff absent (cross-instance
// retransmit dedup).
func (s *Store) SetNX(key, value []byte, ttl time.Duration) (bool, error) {
	ctx, cancel := s.opCtx()
	defer cancel()
	return s.b.SetNX(ctx, s.key(key), value, ttl)
}

// Len reports the backend entry count when supported (in-memory backend),
// else 0. Used by the retry endpoint's cache-size sampler.
func (s *Store) Len() int {
	if l, ok := s.b.(interface{ Len() int }); ok {
		return l.Len()
	}
	return 0
}

// Close is a no-op: the backend is owned by the caller, which may share it
// across several namespaced Stores.
func (s *Store) Close() error { return nil }

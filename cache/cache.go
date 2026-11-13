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
	"github.com/lightwebinc/shard-common/cache/arena"
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
	local := false
	switch b.(type) {
	case *memory.Backend, *arena.Backend:
		local = true
	}
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

// Owning returns a view of c whose Store hands the value over to the backend
// instead of lending it (shard-common cache.OwnedSetter: the in-memory
// backend keeps the slice, no copy), and true; the caller must not use a
// value after storing it. When c's backend copies anyway (or c is not a
// *Store) it returns c and false.
func Owning(c Cache) (Cache, bool) {
	s, ok := c.(*Store)
	if !ok {
		return c, false
	}
	o, ok := s.b.(scache.OwnedSetter)
	if !ok {
		return c, false
	}
	return &owningStore{s: s, o: o}, true
}

type owningStore struct {
	s *Store
	o scache.OwnedSetter
}

// Store writes the frame value under key with ttl; value belongs to the
// backend afterward.
func (v *owningStore) Store(key, value []byte, ttl time.Duration) error {
	ctx, cancel := v.s.opCtx()
	defer cancel()
	return v.o.SetOwned(ctx, v.s.key(key), value, ttl)
}

func (v *owningStore) Retrieve(key []byte) ([]byte, error) { return v.s.Retrieve(key) }
func (v *owningStore) Delete(key []byte) error             { return v.s.Delete(key) }
func (v *owningStore) Close() error                        { return v.s.Close() }

// Leaser hands out leases on the memory of a cache whose backend leases
// (shard-common cache.Leaser: the arena backend). A receive loop that gets a
// datagram before its key is known leases it, which is the datagram's one
// copy, and the goroutine that works out the key stores it through a
// [LeaseView], which commits the lease in place.
type Leaser struct {
	s *Store
	l scache.Leaser
}

// Leasing returns a Leaser for c, and true, when c's backend leases its
// memory. Otherwise it returns nil and false.
func Leasing(c Cache) (*Leaser, bool) {
	s, ok := c.(*Store)
	if !ok {
		return nil, false
	}
	l, ok := s.b.(scache.Leaser)
	if !ok {
		return nil, false
	}
	return &Leaser{s: s, l: l}, true
}

// Lease copies val into the cache's memory. An empty lease (nil Buf) means
// the cache cannot take it now; the caller keeps val in memory of its own.
func (r *Leaser) Lease(val []byte) scache.Lease { return r.l.Lease(val) }

// Release ends a lease no [LeaseView] handled.
func (r *Leaser) Release(l scache.Lease) { r.l.Release(l) }

// View returns a view of the cache for one goroutine.
func (r *Leaser) View() *LeaseView { return &LeaseView{r: r} }

// LeaseView is the cache as one processing goroutine sees it. Inside
// [LeaseView.Handle], a Store of the leased bytes commits the lease (no
// copy); any other Store copies, as [Store.Store] does. Not safe for
// concurrent use.
type LeaseView struct {
	r         *Leaser
	cur       scache.Lease
	committed bool
}

// Handle runs fn with l as the current lease, then releases l. An empty
// lease is allowed: every Store in fn then copies.
func (v *LeaseView) Handle(l scache.Lease, fn func()) {
	v.cur, v.committed = l, false
	defer func() {
		v.cur = scache.Lease{}
		v.r.l.Release(l)
	}()
	fn()
}

// Store writes value under key with ttl: in place when value is the current
// lease's bytes and they have not been stored yet, else as a copy.
func (v *LeaseView) Store(key, value []byte, ttl time.Duration) error {
	b := v.cur.Buf
	if v.committed || len(b) == 0 || len(value) != len(b) || &value[0] != &b[0] {
		return v.r.s.Store(key, value, ttl)
	}
	v.committed = true
	return v.r.l.Commit(context.Background(), v.r.s.key(key), v.cur, ttl)
}

func (v *LeaseView) Retrieve(key []byte) ([]byte, error) { return v.r.s.Retrieve(key) }
func (v *LeaseView) Delete(key []byte) error             { return v.r.s.Delete(key) }
func (v *LeaseView) Close() error                        { return v.r.s.Close() }

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

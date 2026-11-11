package cache

import (
	"context"
	"testing"
	"time"

	scache "github.com/lightwebinc/shard-common/cache"
	"github.com/lightwebinc/shard-common/cache/memory"
)

// slowBackend stands in for a network backend: it reports whether each
// operation carried a deadline.
type slowBackend struct {
	scache.Backend
	deadline bool
}

func (b *slowBackend) Set(ctx context.Context, key, value []byte, ttl time.Duration) error {
	_, b.deadline = ctx.Deadline()
	return nil
}

// Operations on the in-memory backend carry no deadline (it never waits, and
// a deadline costs a timer per frame); every other backend keeps one.
func TestStoreDeadlineOnlyForRemoteBackends(t *testing.T) {
	m := memory.New(16)
	s := NewStore(m, "p:", time.Second)
	if !s.local {
		t.Fatal("in-memory backend not recognized")
	}
	if err := s.Store([]byte("k"), []byte("v"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if v, err := s.Retrieve([]byte("k")); err != nil || string(v) != "v" {
		t.Fatalf("Retrieve = %q, %v", v, err)
	}

	sb := &slowBackend{}
	r := NewStore(sb, "p:", time.Second)
	if r.local {
		t.Fatal("a remote backend was treated as in-process")
	}
	_ = r.Store([]byte("k"), []byte("v"), time.Minute)
	if !sb.deadline {
		t.Error("a remote backend's operation carried no deadline")
	}
}

// Owning hands values over to a backend that keeps them (in-memory: the
// stored value IS the caller's slice) under the same prefix, and declines
// for a backend that copies anyway.
func TestOwning(t *testing.T) {
	m := memory.New(16)
	s := NewStore(m, "p:", time.Second)
	o, ok := Owning(s)
	if !ok {
		t.Fatal("in-memory store did not take ownership")
	}
	val := []byte("handed over")
	if err := o.Store([]byte("k"), val, time.Minute); err != nil {
		t.Fatal(err)
	}
	// Read through the plain view: same namespace, same bytes, no copy.
	got, err := s.Retrieve([]byte("k"))
	if err != nil || len(got) != len(val) || &got[0] != &val[0] {
		t.Fatalf("Retrieve = %q, %v (copied: %v)", got, err, len(got) > 0 && &got[0] != &val[0])
	}
	if got, _ := o.Retrieve([]byte("k")); string(got) != "handed over" {
		t.Fatalf("owning view Retrieve = %q", got)
	}
	if err := o.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Retrieve([]byte("k")); got != nil {
		t.Fatalf("after Delete: %q", got)
	}

	r := NewStore(&slowBackend{}, "p:", time.Second)
	if c, ok := Owning(r); ok || c != Cache(r) {
		t.Fatal("a copying backend was given ownership")
	}
}

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

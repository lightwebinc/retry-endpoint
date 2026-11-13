package cache

import (
	"bytes"
	"testing"
	"time"

	scache "github.com/lightwebinc/shard-common/cache"
	"github.com/lightwebinc/shard-common/cache/arena"
	"github.com/lightwebinc/shard-common/cache/memory"
)

func TestLeasing(t *testing.T) {
	m := memory.New(0)
	defer func() { _ = m.Close() }()
	if _, ok := Leasing(NewStore(m, "p:", time.Second)); ok {
		t.Fatal("the memory backend leases")
	}
	a, err := arena.New(arena.Options{Bytes: 1 << 16})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()
	store := NewStore(a, "p:", time.Second)
	if !store.local {
		t.Fatal("the arena backend is not treated as in-process")
	}
	r, ok := Leasing(store)
	if !ok {
		t.Fatal("the arena backend does not lease")
	}
	v := r.View()
	frame, other := bytes.Repeat([]byte{7}, 500), bytes.Repeat([]byte{9}, 300)

	l := r.Lease(frame)
	v.Handle(l, func() {
		if a.Leased() != 1 {
			t.Fatalf("Leased = %d inside Handle", a.Leased())
		}
		// The leased bytes commit in place; anything else is copied.
		if err := v.Store([]byte("k1"), l.Buf, time.Minute); err != nil {
			t.Fatal(err)
		}
		if err := v.Store([]byte("k2"), l.Buf, time.Minute); err != nil { // a second store of them
			t.Fatal(err)
		}
		if err := v.Store([]byte("k3"), other, time.Minute); err != nil {
			t.Fatal(err)
		}
		if err := v.Store([]byte("k4"), l.Buf[:100], time.Minute); err != nil { // part of them
			t.Fatal(err)
		}
	})
	if a.Leased() != 0 {
		t.Fatalf("Leased = %d after Handle", a.Leased())
	}
	// An empty lease: the store copies.
	v.Handle(scache.Lease{}, func() {
		if err := v.Store([]byte("k5"), other, time.Minute); err != nil {
			t.Fatal(err)
		}
	})
	// A lease no view handled.
	r.Release(r.Lease(frame))
	if a.Leased() != 0 {
		t.Fatalf("Leased = %d after Release", a.Leased())
	}
	for k, want := range map[string][]byte{"k1": frame, "k2": frame, "k3": other, "k4": frame[:100], "k5": other} {
		got, err := v.Retrieve([]byte(k))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("%s: %d bytes, want %d (err %v)", k, len(got), len(want), err)
		}
		if got, _ := store.Retrieve([]byte(k)); !bytes.Equal(got, want) {
			t.Fatalf("%s through the store", k)
		}
	}
	if err := v.Delete([]byte("k1")); err != nil {
		t.Fatal(err)
	}
	if got, _ := v.Retrieve([]byte("k1")); got != nil {
		t.Fatal("deleted key still there")
	}
}

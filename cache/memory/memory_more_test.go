package memory

import (
	"testing"
	"time"
)

func TestLen(t *testing.T) {
	c := New(0)
	defer func() { _ = c.Close() }()
	if c.Len() != 0 {
		t.Errorf("empty: %d", c.Len())
	}
	_ = c.Store([]byte("a"), []byte("1"), time.Minute)
	_ = c.Store([]byte("b"), []byte("2"), time.Minute)
	if c.Len() != 2 {
		t.Errorf("after two stores: %d", c.Len())
	}
	_ = c.Delete([]byte("a"))
	if c.Len() != 1 {
		t.Errorf("after delete: %d", c.Len())
	}
}

func TestSweepExpired(t *testing.T) {
	c := New(0)
	defer func() { _ = c.Close() }()
	_ = c.Store([]byte("short"), []byte("1"), 10*time.Millisecond)
	_ = c.Store([]byte("long"), []byte("2"), time.Hour)
	time.Sleep(50 * time.Millisecond)
	c.sweepExpired()
	if c.Len() != 1 {
		t.Errorf("expected 1 entry after sweep, got %d", c.Len())
	}
	if v, _ := c.Retrieve([]byte("long")); string(v) != "2" {
		t.Errorf("long entry should survive: %q", v)
	}
	if v, _ := c.Retrieve([]byte("short")); v != nil {
		t.Errorf("short entry should be gone: %q", v)
	}
}

func TestEvictionOnMaxKeys(t *testing.T) {
	c := New(2)
	defer func() { _ = c.Close() }()
	_ = c.Store([]byte("a"), []byte("1"), time.Minute)
	_ = c.Store([]byte("b"), []byte("2"), time.Minute)
	if c.Len() != 2 {
		t.Fatalf("len=%d", c.Len())
	}
	_ = c.Store([]byte("c"), []byte("3"), time.Minute)
	// One of a or b should have been evicted.
	if c.Len() != 2 {
		t.Errorf("after evict-store: len=%d", c.Len())
	}
}

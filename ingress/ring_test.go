package ingress

import (
	"bytes"
	"context"
	"encoding/binary"
	"log/slog"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/cache/memory"
	"github.com/lightwebinc/shard-common/teering"
	"github.com/lightwebinc/shard-common/teewire"

	"github.com/lightwebinc/retry-endpoint/cache"
)

// TestRunTeeRing_RawFramesStored feeds raw frames through a shared-memory
// ring, as the proxy's -retry-tee-ring does, and expects each stored once,
// in order, with the ring's loss accounting at zero.
func TestRunTeeRing_RawFramesStored(t *testing.T) {
	mc := &mockCache{}
	w := newTestWorker(mc)
	if w.log == nil {
		w.log = slog.Default().With("component", "ingress-test")
	}
	path := filepath.Join(t.TempDir(), "tee-ring")
	ctx, cancel := context.WithCancel(context.Background())
	exited := make(chan error, 1)
	go func() { exited <- w.RunTeeRing(ctx, path) }()
	// The consumer waits for the producer: the file appears after it started.
	time.Sleep(20 * time.Millisecond)
	p, err := teering.Create(path, teering.MinCapacity)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	const n = 200
	for i := 0; i < n; i++ {
		raw := buildRaw(t, 0x5555666677778888, uint64(i+1), []byte("ring-origin"))
		if err := p.Write(raw); err != nil {
			t.Fatal(err)
		}
	}
	waitStores(t, mc, n)
	time.Sleep(20 * time.Millisecond)
	if got := mc.storeCount(); got != n {
		t.Fatalf("stored %d, want %d", got, n)
	}
	if p.Dropped() != 0 {
		t.Fatalf("producer dropped %d", p.Dropped())
	}
	// The producer makes a new file at the path (as a restart under
	// systemd's RemoveIPC does): the consumer notices and re-attaches.
	_ = p.Close()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	p2, err := teering.Create(path, teering.MinCapacity)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p2.Close() }()
	deadline := time.Now().Add(5 * time.Second)
	for mc.storeCount() < n+1 && time.Now().Before(deadline) {
		_ = p2.Write(buildRaw(t, 0x5555666677778888, 1000, []byte("after-replace")))
		time.Sleep(100 * time.Millisecond)
	}
	if mc.storeCount() < n+1 {
		t.Fatal("no frame stored from the replaced ring file")
	}
	cancel()
	select {
	case err := <-exited:
		if err != nil {
			t.Fatalf("RunTeeRing: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("RunTeeRing did not exit after cancel")
	}
}

// TestRunTeeRing_OwnedByMemoryCache runs the ring ingest over the in-memory
// frame store, which keeps each frame's single copy: every frame is
// retrievable under its (HashKey, SeqNum) key with the bytes the producer
// wrote, across many wraps of the ring.
func TestRunTeeRing_OwnedByMemoryCache(t *testing.T) {
	b := memory.New(0)
	defer func() { _ = b.Close() }()
	store := cache.NewStore(b, "bre:frame:", time.Second)
	if _, ok := cache.Owning(store); !ok {
		t.Fatal("in-memory store does not take ownership")
	}
	w := &Worker{cache: store, ttls: TTLConfig{Tx: time.Minute, Block: time.Minute, Subtree: time.Minute, Anchor: time.Minute, BEEF: time.Minute},
		log: slog.Default().With("component", "ingress-test")}
	path := filepath.Join(t.TempDir(), "tee-ring")
	p, err := teering.Create(path, teering.MinCapacity)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	exited := make(chan error, 1)
	go func() { exited <- w.RunTeeRing(ctx, path) }()
	const n = 3000
	payload := make([]byte, 1200)
	want := make([][]byte, n)
	for i := 0; i < n; i++ {
		for j := range payload {
			payload[j] = byte(i + j)
		}
		raw := buildRaw(t, 0x1111222233334444, uint64(i+1), payload)
		want[i] = raw
		for p.Write(raw) != nil { // the ring holds about 800 of these
			time.Sleep(time.Millisecond)
		}
	}
	key := func(i int) []byte {
		var k [16]byte
		binary.BigEndian.PutUint64(k[0:8], 0x1111222233334444)
		binary.BigEndian.PutUint64(k[8:16], uint64(i+1))
		return k[:]
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if v, _ := store.Retrieve(key(n - 1)); v != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-exited; err != nil {
		t.Fatalf("RunTeeRing: %v", err)
	}
	for i := 0; i < n; i++ {
		v, err := store.Retrieve(key(i))
		if err != nil || !bytes.Equal(v, want[i]) {
			t.Fatalf("frame %d: %d bytes stored, want %d (err %v)", i, len(v), len(want[i]), err)
		}
	}
	if p.Dropped() != 0 {
		t.Fatalf("producer dropped %d", p.Dropped())
	}
}

// TestRunTeeRing_EnvelopeDropped: the ring carries raw frames only. A
// datagram in the teewire envelope form would assert a frame source, so it
// is dropped and the raw frames around it are stored.
func TestRunTeeRing_EnvelopeDropped(t *testing.T) {
	mc := &mockCache{}
	w := newTestWorker(mc)
	if w.log == nil {
		w.log = slog.Default().With("component", "ingress-test")
	}
	path := filepath.Join(t.TempDir(), "tee-ring")
	p, err := teering.Create(path, teering.MinCapacity)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	exited := make(chan error, 1)
	go func() { exited <- w.RunTeeRing(ctx, path) }()
	src := netip.MustParseAddrPort("[fd00::7]:9001")
	for i := range 3 {
		raw := buildRaw(t, 0x1111222233334444, uint64(i+1), []byte("ring-raw"))
		if i == 1 {
			raw = teewire.AppendEncap(nil, src, raw)
		}
		if err := p.Write(raw); err != nil {
			t.Fatal(err)
		}
	}
	waitStores(t, mc, 2)
	time.Sleep(50 * time.Millisecond)
	if got := mc.storeCount(); got != 2 {
		t.Fatalf("stored %d, want 2 (the enveloped datagram dropped)", got)
	}
	cancel()
	if err := <-exited; err != nil {
		t.Fatalf("RunTeeRing: %v", err)
	}
}

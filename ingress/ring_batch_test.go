package ingress

import (
	"bytes"
	"context"
	"encoding/binary"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	scache "github.com/lightwebinc/shard-common/cache"
	"github.com/lightwebinc/shard-common/cache/arena"
	"github.com/lightwebinc/shard-common/cache/memory"
	"github.com/lightwebinc/shard-common/teering"

	"github.com/lightwebinc/retry-endpoint/cache"
)

// TestRunTeeRing_Runs feeds the ring the way a proxy that builds an object's
// fragments in ring space does: one record holding a run of equal-size frames
// and a shorter last one, between ordinary single-frame records, across many
// wraps of the ring. Every frame is retrievable under its key with the bytes
// the producer wrote.
func TestRunTeeRing_Runs(t *testing.T) {
	b := memory.New(0)
	defer func() { _ = b.Close() }()
	ringRuns(t, b, true, nil)
}

// The same feed over the arena backend: each frame is leased out of the ring
// into the cache's block and committed there, and every lease is released.
func TestRunTeeRing_RunsOverArena(t *testing.T) {
	b, err := arena.New(arena.Options{Bytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()
	ringRuns(t, b, true, nil)
	if n := b.Leased(); n != 0 {
		t.Fatalf("%d leases left outstanding", n)
	}
}

// An arena far smaller than the feed: it wraps over its own entries all the
// time and at times cannot lease. A frame is then stored or it is not, but a
// lookup never returns another frame's bytes, the newest frame is there, and
// no lease is left outstanding.
func TestRunTeeRing_ArenaLaps(t *testing.T) {
	b, err := arena.New(arena.Options{Bytes: 256 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()
	ringRuns(t, b, false, func() {
		// Idle: no lease out for 50 checks in a row.
		for idle := 0; idle < 50; {
			if b.Leased() == 0 {
				idle++
			} else {
				idle = 0
			}
			time.Sleep(time.Millisecond)
		}
	})
	if n := b.Leased(); n != 0 {
		t.Fatalf("%d leases left outstanding", n)
	}
}

// ringRuns feeds a ring the way a proxy does and checks what the cache
// holds: every frame when all, else that no frame is stored wrong and that
// a last frame, written after settle returns, is stored.
func ringRuns(t *testing.T, b scache.Backend, all bool, settle func()) {
	t.Helper()
	store := cache.NewStore(b, "bre:frame:", time.Second)
	w := &Worker{cache: store, ttls: TTLConfig{Tx: time.Minute, Block: time.Minute, Subtree: time.Minute, Anchor: time.Minute, BEEF: time.Minute},
		log: slog.Default().With("component", "ingress-test")}
	path := filepath.Join(t.TempDir(), "tee-ring")
	p, err := teering.Create(path, teering.MinCapacity)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	exited := make(chan error, 1)
	go func() { exited <- w.RunTeeRing(ctx, path) }()
	// One deadline bounds the whole ingest: a ring that stops draining, or
	// a consumer that returned early, fails the test instead of hanging it.
	ingestBy := time.Now().Add(30 * time.Second)
	retry := func(what string, err error) {
		t.Helper()
		select {
		case rerr := <-exited:
			t.Fatalf("%s: RunTeeRing returned early: %v", what, rerr)
		default:
		}
		if time.Now().After(ingestBy) {
			t.Fatalf("%s: ring not draining: %v", what, err)
		}
		time.Sleep(time.Millisecond)
	}

	const hash = 0x7777222233334444
	var want [][]byte
	seq := uint64(0)
	frame := func(n int) []byte {
		seq++
		payload := make([]byte, n)
		for j := range payload {
			payload[j] = byte(int(seq) + j)
		}
		raw := buildRaw(t, hash, seq, payload)
		want = append(want, raw)
		return raw
	}
	for round := 0; round < 120; round++ {
		// A run: round%7+2 full frames and a short last one.
		full := round%7 + 2
		var frames [][]byte
		for i := 0; i < full; i++ {
			frames = append(frames, frame(1200))
		}
		frames = append(frames, frame(1+round%900))
		total := 0
		for _, f := range frames {
			total += len(f)
		}
		var r *teering.Reservation
		for {
			if r, err = p.Reserve(total); err == nil {
				break
			}
			retry("reserve", err)
		}
		off := 0
		for _, f := range frames {
			off += copy(r.Bytes()[off:], f)
		}
		p.Commit(r, len(frames[0]))
		p.Unpin(r)
		// And a single frame the ordinary way.
		raw := frame(300)
		for {
			err := p.Write(raw)
			if err == nil {
				break
			}
			retry("write", err)
		}
	}
	key := func(s uint64) []byte {
		var k [16]byte
		binary.BigEndian.PutUint64(k[0:8], hash)
		binary.BigEndian.PutUint64(k[8:16], s)
		return k[:]
	}
	if settle != nil {
		// Once the pipeline is idle nothing is leased, so one more frame is
		// stored whatever the feed before it did to the cache.
		settle()
		raw := frame(300)
		for {
			err := p.Write(raw)
			if err == nil {
				break
			}
			retry("last write", err)
		}
	}
	stored := false
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if v, _ := store.Retrieve(key(seq)); v != nil {
			stored = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !stored {
		t.Fatalf("the newest frame (%d) was not stored", seq)
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
	hits := 0
	for i, f := range want {
		v, err := store.Retrieve(key(uint64(i + 1)))
		if v == nil && err == nil && !all {
			continue
		}
		hits++
		if err != nil || !bytes.Equal(v, f) {
			t.Fatalf("frame %d: %d bytes stored, want %d (err %v)", i+1, len(v), len(f), err)
		}
	}
	if hits == 0 {
		t.Fatal("nothing stored")
	}
	if p.Dropped() != 0 || p.Pinned() != 0 {
		t.Fatalf("producer dropped %d, pinned %d", p.Dropped(), p.Pinned())
	}
}

package ingress

import (
	"bytes"
	"context"
	"encoding/binary"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

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
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if v, _ := store.Retrieve(key(seq)); v != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
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
	for i, f := range want {
		v, err := store.Retrieve(key(uint64(i + 1)))
		if err != nil || !bytes.Equal(v, f) {
			t.Fatalf("frame %d: %d bytes stored, want %d (err %v)", i+1, len(v), len(f), err)
		}
	}
	if p.Dropped() != 0 || p.Pinned() != 0 {
		t.Fatalf("producer dropped %d, pinned %d", p.Dropped(), p.Pinned())
	}
}

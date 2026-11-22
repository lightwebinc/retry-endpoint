package cache

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// scriptedDedup answers every SetNX with err (nil = claim made) and counts
// the calls that reached it.
type scriptedDedup struct {
	mu    sync.Mutex
	err   error
	calls int
}

func (d *scriptedDedup) SetNX(_, _ []byte, _ time.Duration) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
	return d.err == nil, d.err
}

func (d *scriptedDedup) Delete([]byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
	return d.err
}

func (d *scriptedDedup) set(err error) {
	d.mu.Lock()
	d.err = err
	d.mu.Unlock()
}

func (d *scriptedDedup) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

// fakeClock is a settable clock for a Breaker.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestBreaker(window time.Duration, logs *bytes.Buffer) (*Breaker, *fakeClock) {
	clk := &fakeClock{t: time.Unix(1_000_000, 0)}
	br := NewBreaker(window)
	br.now = clk.now
	br.log = slog.New(slog.NewTextHandler(logs, nil))
	return br, clk
}

var errDown = errors.New("backend down")

// One failed claim opens the breaker: for the window the backend is not
// asked again and the claims report ErrClaimSkipped. After the window one
// probe goes through; a failed probe opens the breaker for another window, a
// successful one closes it. Only the outage's start and end are logged.
func TestBreaker_OpensSkipsProbesAndCloses(t *testing.T) {
	var logs bytes.Buffer
	br, clk := newTestBreaker(5*time.Second, &logs)
	d := &scriptedDedup{}
	g := Guard(d, br)
	claim := func() error { _, err := g.SetNX([]byte("k"), []byte("1"), time.Minute); return err }

	if err := claim(); err != nil {
		t.Fatalf("closed breaker, healthy backend: %v", err)
	}
	d.set(errDown)
	if err := claim(); !errors.Is(err, errDown) {
		t.Fatalf("first failing claim: err = %v, want the backend's error", err)
	}
	for i := 0; i < 5; i++ {
		clk.advance(time.Second - time.Millisecond)
		if err := claim(); !errors.Is(err, ErrClaimSkipped) {
			t.Fatalf("claim %d inside the window: err = %v, want ErrClaimSkipped", i, err)
		}
	}
	if got := d.count(); got != 2 {
		t.Fatalf("backend asked %d times, want 2 (none while open)", got)
	}

	// Past the window: one probe, which fails and reopens for a new window.
	clk.advance(10 * time.Millisecond)
	if err := claim(); !errors.Is(err, errDown) {
		t.Fatalf("probe: err = %v, want the backend's error", err)
	}
	clk.advance(4 * time.Second)
	if err := claim(); !errors.Is(err, ErrClaimSkipped) {
		t.Fatalf("claim after a failed probe: err = %v, want ErrClaimSkipped (window restarted)", err)
	}

	// The backend is back: the next probe closes the breaker.
	d.set(nil)
	clk.advance(time.Second)
	if err := claim(); err != nil {
		t.Fatalf("probe against a healthy backend: %v", err)
	}
	if err := claim(); err != nil {
		t.Fatalf("claim after the probe succeeded: %v, want it sent", err)
	}
	if got := d.count(); got != 5 {
		t.Errorf("backend asked %d times, want 5", got)
	}

	out := logs.String()
	if strings.Count(out, "level=WARN") != 1 || strings.Count(out, "level=INFO") != 1 ||
		!strings.Contains(out, "dedup claims answered again") {
		t.Errorf("want one warning when the outage began and one info line when it ended:\n%s", out)
	}
}

// While the probe is in flight every other claim is skipped, so a backend
// that has stopped answering holds at most one claim per window.
func TestBreaker_OneProbeAtATime(t *testing.T) {
	var logs bytes.Buffer
	br, clk := newTestBreaker(time.Second, &logs)
	if ok, _ := br.admit(); !ok {
		t.Fatal("closed breaker refused a claim")
	}
	br.done(false, errDown)
	clk.advance(time.Second)

	ok, probe := br.admit()
	if !ok || !probe {
		t.Fatalf("admit after the window = (%v, %v), want the probe", ok, probe)
	}
	for i := 0; i < 3; i++ {
		if ok, _ := br.admit(); ok {
			t.Fatal("a second claim was let through while the probe was in flight")
		}
	}
	br.done(true, nil)
	if ok, probe := br.admit(); !ok || probe {
		t.Fatalf("admit after a successful probe = (%v, %v), want a plain claim", ok, probe)
	}
}

// A claim already in flight when the breaker opened neither closes it on a
// late success nor restarts the window on a late failure.
func TestBreaker_StragglersChangeNothing(t *testing.T) {
	var logs bytes.Buffer
	br, clk := newTestBreaker(time.Second, &logs)
	for i := 0; i < 3; i++ { // three claims in flight while closed
		if ok, _ := br.admit(); !ok {
			t.Fatal("closed breaker refused a claim")
		}
	}
	br.done(false, errDown) // the first answer: a failure, which opens it
	clk.advance(500 * time.Millisecond)
	br.done(false, nil) // the second, answered late
	if ok, _ := br.admit(); ok {
		t.Fatal("a straggler's success closed the breaker")
	}
	br.done(false, errDown) // the third, failing late
	clk.advance(500 * time.Millisecond)
	if ok, probe := br.admit(); !ok || !probe {
		t.Fatal("a straggler's failure restarted the window")
	}
}

// A window of zero, or no breaker at all, asks the backend every time.
func TestBreaker_Disabled(t *testing.T) {
	var logs bytes.Buffer
	zero, _ := newTestBreaker(0, &logs)
	for _, br := range []*Breaker{zero, nil} {
		d := &scriptedDedup{err: errDown}
		g := Guard(d, br)
		for i := 0; i < 3; i++ {
			if _, err := g.SetNX([]byte("k"), []byte("1"), time.Minute); !errors.Is(err, errDown) {
				t.Fatalf("claim %d: err = %v, want the backend's error every time", i, err)
			}
		}
		if d.count() != 3 {
			t.Errorf("backend asked %d times, want 3", d.count())
		}
	}
	if logs.Len() != 0 {
		t.Errorf("a disabled breaker logged:\n%s", logs.String())
	}
}

// The claim's own result passes through unchanged and a refused claim (key
// held by a sibling) is an answer, not a failure: it does not open the
// breaker.
func TestGuard_PassesAnswers(t *testing.T) {
	var logs bytes.Buffer
	br, _ := newTestBreaker(time.Second, &logs)
	held := &heldDedup{}
	g := Guard(held, br)
	if set, err := g.SetNX([]byte("k"), []byte("1"), time.Minute); !set || err != nil {
		t.Fatalf("first claim = (%v, %v), want (true, nil)", set, err)
	}
	for i := 0; i < 3; i++ {
		if set, err := g.SetNX([]byte("k"), []byte("1"), time.Minute); set || err != nil {
			t.Fatalf("held claim = (%v, %v), want (false, nil)", set, err)
		}
	}
}

// heldDedup admits a key once until it is deleted.
type heldDedup struct{ seen map[string]bool }

func (d *heldDedup) Delete(key []byte) error {
	delete(d.seen, string(key))
	return nil
}

func (d *heldDedup) SetNX(key, _ []byte, _ time.Duration) (bool, error) {
	if d.seen == nil {
		d.seen = map[string]bool{}
	}
	if d.seen[string(key)] {
		return false, nil
	}
	d.seen[string(key)] = true
	return true, nil
}

// A released claim can be made again. A release always asks the backend, even
// while the breaker is open (a skipped release would hold the claim for its
// whole TTL), and its outcome does not move the breaker.
func TestGuard_Release(t *testing.T) {
	var logs bytes.Buffer
	br, _ := newTestBreaker(time.Second, &logs)
	held := &heldDedup{}
	g := Guard(held, br)
	if set, _ := g.SetNX([]byte("k"), []byte("1"), time.Minute); !set {
		t.Fatal("first claim not made")
	}
	if err := g.Release([]byte("k")); err != nil {
		t.Fatalf("release: %v", err)
	}
	if set, err := g.SetNX([]byte("k"), []byte("1"), time.Minute); !set || err != nil {
		t.Fatalf("claim after release = (%v, %v), want (true, nil)", set, err)
	}

	down := &scriptedDedup{err: errDown}
	gd := Guard(down, br)
	if _, err := gd.SetNX([]byte("k"), []byte("1"), time.Minute); !errors.Is(err, errDown) {
		t.Fatalf("claim against a failing backend: %v", err)
	}
	if err := gd.Release([]byte("k")); !errors.Is(err, errDown) {
		t.Errorf("release with the breaker open: err = %v, want the backend's error", err)
	}
	if down.count() != 2 {
		t.Errorf("backend asked %d times, want 2 (the release is not skipped)", down.count())
	}
	if _, err := gd.SetNX([]byte("k"), []byte("1"), time.Minute); !errors.Is(err, ErrClaimSkipped) {
		t.Errorf("claim after the release: err = %v, want ErrClaimSkipped (the breaker is still open)", err)
	}

	up := &heldDedup{}
	gu := Guard(up, br) // the breaker is open from the failed claim above
	if err := gu.Release([]byte("k")); err != nil {
		t.Errorf("release against a working backend with the breaker open: %v", err)
	}
}

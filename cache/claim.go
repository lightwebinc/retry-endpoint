package cache

import (
	"errors"
	"log/slog"
	"sync"
	"time"
)

// ErrClaimSkipped reports a dedup claim that was not sent to the backend
// because the claim breaker is open: the backend failed a recent claim. The
// caller fails open at once, as it does on a claim error, without waiting on
// the backend again.
var ErrClaimSkipped = errors.New("dedup claim skipped: backend failing")

// Breaker sheds dedup claims while their backend is failing. A claim sits on
// the repair path, so every claim a dead backend makes wait delays a repair.
//
// Closed, the breaker lets every claim through. The first claim that fails
// opens it: for window, every claim is skipped (ErrClaimSkipped) without
// touching the backend. After the window one claim goes through as a probe,
// while the others are still skipped. The probe's success closes the breaker;
// its failure opens it for another window. A backend that stays down costs
// one claim timeout per window instead of one per claim.
//
// Every claim on one backend should share one Breaker. A window of zero or
// less disables it, and so does a nil *Breaker: every claim goes through.
type Breaker struct {
	window time.Duration
	now    func() time.Time // injectable for tests
	log    *slog.Logger

	mu       sync.Mutex
	openedAt time.Time // when the breaker last opened; zero while closed
	since    time.Time // when it opened from closed: the outage's start
	probing  bool      // the half-open probe is in flight
}

// NewBreaker returns a closed Breaker that skips claims for window after one
// fails.
func NewBreaker(window time.Duration) *Breaker {
	return &Breaker{
		window: window,
		now:    time.Now,
		log:    slog.Default().With("component", "dedup"),
	}
}

// admit reports whether a claim may go to the backend now, and whether it is
// the half-open probe. A caller admitted must report the outcome to done.
func (b *Breaker) admit() (ok, probe bool) {
	if b == nil || b.window <= 0 {
		return true, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	switch {
	case b.openedAt.IsZero():
		return true, false
	case b.probing || b.now().Sub(b.openedAt) < b.window:
		return false, false
	default:
		b.probing = true
		return true, true
	}
}

// done records the outcome of a claim admit let through. Only the transitions
// that start and end an outage are logged: a probe that fails again changes
// nothing an operator needs to read.
func (b *Breaker) done(probe bool, err error) {
	if b == nil || b.window <= 0 {
		return
	}
	var opened, closed bool
	var outage time.Duration
	b.mu.Lock()
	now := b.now()
	switch {
	case err != nil && b.openedAt.IsZero():
		b.openedAt, b.since = now, now
		opened = true
	case err != nil && probe:
		b.openedAt, b.probing = now, false
	case err == nil && probe:
		outage = now.Sub(b.since)
		b.openedAt, b.since, b.probing = time.Time{}, time.Time{}, false
		closed = true
	}
	b.mu.Unlock()
	switch {
	case opened:
		b.log.Warn("dedup claims failing, skipping them until a probe succeeds",
			"err", err, "window", b.window)
	case closed:
		b.log.Info("dedup claims answered again", "skipped_for", outage.Round(time.Millisecond))
	}
}

// Claimer is a Deduper whose claims can be given back (implemented by
// *Store).
type Claimer interface {
	Deduper
	Delete(key []byte) error
}

// GuardedDeduper is a Claimer behind a Breaker.
type GuardedDeduper struct {
	d  Claimer
	br *Breaker
}

// Guard puts d behind br. Claims made on one backend should share br, so a
// failure seen by one of them spares the others the wait.
func Guard(d Claimer, br *Breaker) *GuardedDeduper {
	return &GuardedDeduper{d: d, br: br}
}

// SetNX makes the claim through the breaker: while it is open the backend is
// not asked and the result is (false, ErrClaimSkipped).
func (g *GuardedDeduper) SetNX(key, value []byte, ttl time.Duration) (bool, error) {
	ok, probe := g.br.admit()
	if !ok {
		return false, ErrClaimSkipped
	}
	set, err := g.d.SetNX(key, value, ttl)
	g.br.done(probe, err)
	return set, err
}

// Release gives back a claim this endpoint made, through the breaker like a
// claim: while it is open the backend is not asked (ErrClaimSkipped), and the
// claim lapses at its TTL instead.
func (g *GuardedDeduper) Release(key []byte) error {
	ok, probe := g.br.admit()
	if !ok {
		return ErrClaimSkipped
	}
	err := g.d.Delete(key)
	g.br.done(probe, err)
	return err
}

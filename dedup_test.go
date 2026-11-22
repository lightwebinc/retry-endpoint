package main

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	scache "github.com/lightwebinc/shard-common/cache"
	"github.com/lightwebinc/shard-common/shard"

	"github.com/lightwebinc/retry-endpoint/cache"
	"github.com/lightwebinc/retry-endpoint/config"
	"github.com/lightwebinc/retry-endpoint/retransmit"
)

// stallProxy forwards TCP to upstream until stall(true); from then on it keeps
// every connection open, and accepts new ones, but drops every byte: a backend
// that stopped answering without closing anything, which is what a client
// holding connections to a Redis whose host went away sees.
type stallProxy struct {
	ln       net.Listener
	upstream string
	stalled  atomic.Bool
	mu       sync.Mutex
	conns    []net.Conn
}

func startStallProxy(t *testing.T, upstream string) *stallProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &stallProxy{ln: ln, upstream: upstream}
	go p.serve()
	t.Cleanup(p.close)
	return p
}

func (p *stallProxy) addr() string  { return p.ln.Addr().String() }
func (p *stallProxy) stall(on bool) { p.stalled.Store(on) }

func (p *stallProxy) track(c net.Conn) {
	p.mu.Lock()
	p.conns = append(p.conns, c)
	p.mu.Unlock()
}

func (p *stallProxy) close() {
	_ = p.ln.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
}

func (p *stallProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.track(c)
		go p.handle(c)
	}
}

func (p *stallProxy) handle(c net.Conn) {
	u, err := net.Dial("tcp", p.upstream)
	if err != nil {
		_ = c.Close()
		return
	}
	p.track(u)
	go p.pipe(u, c)
	p.pipe(c, u)
}

// pipe copies src to dst, dropping what it reads while the proxy is stalled.
func (p *stallProxy) pipe(dst, src net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 && !p.stalled.Load() {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// claimFrame is a cached frame whose dedup key is (HashKey 0, SeqNum seq).
func claimFrame(seq byte) []byte {
	raw := make([]byte, 100)
	raw[55] = seq
	return raw
}

// Regression: a dedup claim against a backend that stopped answering waited
// the frame store's -cache-op-timeout (1 s by default) on every retransmit and
// every proxied recovery, so a repair left after the listener had given up on
// it. A claim must cost no more than a short wait, whatever the frame store's
// timeout.
func TestDedupClaim_StalledBackendDoesNotDelayRepairs(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	sp := startStallProxy(t, mr.Addr())

	cfg := &config.Config{
		CacheBackend:     "memory",
		RedisAddr:        sp.addr(),
		CacheDialTimeout: time.Second,
		CacheOpTimeout:   time.Second,
	}
	retransDedup, proxyDedup, closeDedup := newDedup(cfg, nil)
	defer closeDedup()
	if retransDedup == nil || proxyDedup == nil {
		t.Fatal("no dedup claims built for a reachable -redis-addr")
	}
	r := retransmit.New(shard.New(0xFF05, shard.DefaultGroupID, 2), nil, 9001, time.Minute, retransDedup, nil, false)

	// While the backend answers, the claim holds: a repeat is suppressed.
	if err := r.Retransmit(claimFrame(1), [32]byte{}); err != nil {
		t.Fatalf("first retransmit: %v", err)
	}
	if err := r.Retransmit(claimFrame(1), [32]byte{}); !errors.Is(err, retransmit.ErrDedupSuppressed) {
		t.Fatalf("repeat retransmit: err = %v, want ErrDedupSuppressed", err)
	}

	sp.stall(true)
	const bound = 250 * time.Millisecond
	var worst time.Duration
	for seq := byte(2); seq < 6; seq++ {
		start := time.Now()
		if err := r.Retransmit(claimFrame(seq), [32]byte{}); err != nil {
			t.Fatalf("retransmit %d with the dedup backend stalled: err = %v, want the frame sent", seq, err)
		}
		d := time.Since(start)
		worst = max(worst, d)
		if d > bound {
			t.Errorf("retransmit %d waited %v on a stalled dedup backend, want under %v", seq, d, bound)
		}
	}
	for seq := uint64(2); seq < 4; seq++ {
		key := make([]byte, 16)
		binary.BigEndian.PutUint64(key[8:], seq)
		start := time.Now()
		_, _ = proxyDedup.SetNX(key, []byte("1"), time.Minute)
		d := time.Since(start)
		worst = max(worst, d)
		if d > bound {
			t.Errorf("proxy claim %d waited %v on a stalled dedup backend, want under %v", seq, d, bound)
		}
	}
	t.Logf("longest claim against the stalled backend: %v", worst)
}

// With the breaker on, the first claim against the stalled backend pays the
// claim timeout, and the claims after it, on both paths (one breaker), go
// out at once until the window has passed. Then one probe finds the backend
// back, and claims are made, and hold, again.
func TestDedupClaim_BreakerSkipsThenRecovers(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	sp := startStallProxy(t, mr.Addr())

	cfg := &config.Config{
		CacheBackend:       "memory",
		RedisAddr:          sp.addr(),
		CacheDialTimeout:   time.Second,
		CacheOpTimeout:     time.Second,
		DedupClaimTimeout:  50 * time.Millisecond,
		DedupBreakerWindow: 300 * time.Millisecond,
	}
	retransDedup, proxyDedup, closeDedup := newDedup(cfg, nil)
	defer closeDedup()
	r := retransmit.New(shard.New(0xFF05, shard.DefaultGroupID, 2), nil, 9001, time.Minute, retransDedup, nil, false)
	if err := r.Retransmit(claimFrame(1), [32]byte{}); err != nil {
		t.Fatalf("retransmit with the backend answering: %v", err)
	}

	sp.stall(true)
	start := time.Now()
	if err := r.Retransmit(claimFrame(2), [32]byte{}); err != nil {
		t.Fatalf("first retransmit after the stall: %v, want the frame sent", err)
	}
	if d := time.Since(start); d < cfg.DedupClaimTimeout/2 || d > 250*time.Millisecond {
		t.Errorf("first claim after the stall took %v, want about the claim timeout (%v)", d, cfg.DedupClaimTimeout)
	}
	start = time.Now()
	for seq := byte(3); seq < 13; seq++ {
		if err := r.Retransmit(claimFrame(seq), [32]byte{}); err != nil {
			t.Fatalf("retransmit %d while the breaker is open: %v, want the frame sent", seq, err)
		}
	}
	if _, err := proxyDedup.SetNX([]byte("gap-1"), []byte("1"), time.Minute); !errors.Is(err, cache.ErrClaimSkipped) {
		t.Errorf("proxy claim after a failed retransmit claim: err = %v, want ErrClaimSkipped (one breaker for both)", err)
	}
	// Under the claim timeout: had one claim gone to the backend, it alone
	// would have waited that long.
	if d := time.Since(start); d > 40*time.Millisecond {
		t.Errorf("10 retransmits and a proxy claim took %v with the breaker open, want no claim to wait", d)
	}

	// The backend is back; once the window has passed, the next claim is
	// the probe, it is answered, and claims hold again.
	sp.stall(false)
	time.Sleep(cfg.DedupBreakerWindow + 50*time.Millisecond)
	if err := r.Retransmit(claimFrame(20), [32]byte{}); err != nil {
		t.Fatalf("probe retransmit: %v", err)
	}
	if err := r.Retransmit(claimFrame(20), [32]byte{}); !errors.Is(err, retransmit.ErrDedupSuppressed) {
		t.Errorf("repeat after the backend came back: err = %v, want ErrDedupSuppressed (claims resumed)", err)
	}
}

// With a redis frame backend the claims used the frame client, and so its
// -cache-op-timeout. They get a client of their own at the claim timeout.
func TestDedupClaim_RedisFrameBackendClaimsOwnClient(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	sp := startStallProxy(t, mr.Addr())

	cfg := &config.Config{
		CacheBackend:     "redis",
		RedisAddr:        sp.addr(),
		CacheDialTimeout: time.Second,
		CacheOpTimeout:   time.Second,
	}
	frames, err := scache.Open(context.Background(), scache.Config{
		Backend:     scache.BackendRedis,
		RedisAddr:   cfg.RedisAddr,
		DialTimeout: cfg.CacheDialTimeout,
		OpTimeout:   cfg.CacheOpTimeout,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = frames.Close() }()
	retransDedup, _, closeDedup := newDedup(cfg, frames)
	defer closeDedup()
	r := retransmit.New(shard.New(0xFF05, shard.DefaultGroupID, 2), nil, 9001, time.Minute, retransDedup, nil, false)
	if err := r.Retransmit(claimFrame(1), [32]byte{}); err != nil {
		t.Fatalf("retransmit with the backend answering: %v", err)
	}

	sp.stall(true)
	start := time.Now()
	if err := r.Retransmit(claimFrame(2), [32]byte{}); err != nil {
		t.Fatalf("retransmit with the backend stalled: %v, want the frame sent", err)
	}
	if d := time.Since(start); d > 250*time.Millisecond {
		t.Errorf("claim on the redis frame backend waited %v, want under 250ms (not -cache-op-timeout)", d)
	}
}

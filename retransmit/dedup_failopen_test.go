package retransmit

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	scache "github.com/lightwebinc/shard-common/cache"
	"github.com/lightwebinc/shard-common/shard"

	"github.com/lightwebinc/retry-endpoint/cache"
	"github.com/lightwebinc/retry-endpoint/metrics"
)

// failingDedup is a dedup backend that cannot answer: every SET NX errors,
// with the false "not set" a real client returns alongside the error.
type failingDedup struct{ calls atomic.Int64 }

func (d *failingDedup) SetNX(_, _ []byte, _ time.Duration) (bool, error) {
	d.calls.Add(1)
	return false, errors.New("dedup backend unreachable")
}

// serveMetrics starts rec's HTTP endpoint on a free loopback port and returns
// its address.
func serveMetrics(t *testing.T, rec *metrics.Recorder) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	done := make(chan struct{})
	go rec.Serve(addr, done)
	t.Cleanup(func() { close(done) })
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if resp, err := http.Get("http://" + addr + "/healthz"); err == nil {
			_ = resp.Body.Close()
			return addr
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("metrics endpoint did not come up")
	return ""
}

// scrape returns the value of the first series of metric name whose labels
// contain label (any series when label is empty), 0 when there is none.
func scrape(t *testing.T, addr, name, label string) float64 {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, name+"{") && strings.Contains(line, label) {
			f := strings.Fields(line)
			v, _ := strconv.ParseFloat(f[len(f)-1], 64)
			return v
		}
	}
	return 0
}

// warnLines counts the warn and error lines in text-handler output.
func warnLines(logs string) int {
	return strings.Count(logs, "level=WARN") + strings.Count(logs, "level=ERROR")
}

// Regression: a dedup claim the backend could not answer was read as a claim
// a sibling holds. The false "not set" that comes back with the error
// suppressed the send (ErrDedupSuppressed), so while the shared backend was
// down no endpoint put a multicast repair on the wire, and with no ACK the
// requester escalated through every endpoint to the same result. A failed
// claim must fail open: the frame is sent, the error is counted apart from
// real dedup drops, and the log gets one line per window, not one per NACK.
func TestRetransmit_DedupBackendErrorFailsOpen(t *testing.T) {
	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(prev)

	rec, err := metrics.New("retransmit-test", 1, "", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	addr := serveMetrics(t, rec)

	dd := &failingDedup{}
	r := New(shard.New(0xFF05, shard.DefaultGroupID, 2), nil, 9001, time.Minute, dd, rec, false)
	raw := make([]byte, 100)
	raw[55] = 1 // SeqNum 1

	const n = 50
	for i := 0; i < n; i++ {
		if err := r.Retransmit(raw, [32]byte{}); err != nil {
			t.Fatalf("retransmit %d with the dedup backend down: err = %v, want the frame sent (fail open)", i, err)
		}
	}
	if got := dd.calls.Load(); got != n {
		t.Errorf("SET NX attempts = %d, want %d (one claim per retransmit)", got, n)
	}
	if got := scrape(t, addr, "bre_dedup_errors_total", `claim="retransmit"`); got != n {
		t.Errorf(`bre_dedup_errors_total{claim="retransmit"} = %v, want %d`, got, n)
	}
	if got := scrape(t, addr, "bre_retransmit_dedup_total", ""); got != 0 {
		t.Errorf("bre_retransmit_dedup_total = %v, want 0: a failed claim is not a sibling's claim", got)
	}
	if got := warnLines(logs.String()); got != 1 {
		t.Errorf("%d warn/error log lines for %d failed claims, want 1:\n%s", got, n, logs.String())
	}
}

// The same through a real backend that went away after startup: the client
// returns its error with a false "not set", the shape that suppressed sends.
func TestRetransmit_RedisGoneFailsOpen(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	backend, err := scache.Open(context.Background(), scache.Config{Backend: scache.BackendRedis, RedisAddr: mr.Addr()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = backend.Close() }()
	mr.Close()

	r := New(shard.New(0xFF05, shard.DefaultGroupID, 2), nil, 9001, time.Minute,
		cache.NewStore(backend, "test:", 200*time.Millisecond), nil, false)
	raw := make([]byte, 100)
	raw[55] = 1
	if err := r.Retransmit(raw, [32]byte{}); err != nil {
		t.Fatalf("retransmit with redis gone: err = %v, want the frame sent (fail open)", err)
	}
}

// skippingDedup is a claim breaker that is open: every claim is skipped
// without reaching the backend.
type skippingDedup struct{ calls atomic.Int64 }

func (d *skippingDedup) SetNX(_, _ []byte, _ time.Duration) (bool, error) {
	d.calls.Add(1)
	return false, cache.ErrClaimSkipped
}

// A claim the breaker skipped (a recent claim failed) fails open at once, like
// a failed claim, but it is counted apart, as a skip, and not logged: the
// failure that opened the breaker was.
func TestRetransmit_ClaimSkippedFailsOpen(t *testing.T) {
	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(prev)

	rec, err := metrics.New("retransmit-test", 1, "", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	addr := serveMetrics(t, rec)

	r := New(shard.New(0xFF05, shard.DefaultGroupID, 2), nil, 9001, time.Minute, &skippingDedup{}, rec, false)
	raw := make([]byte, 100)
	raw[55] = 1
	const n = 20
	for i := 0; i < n; i++ {
		if err := r.Retransmit(raw, [32]byte{}); err != nil {
			t.Fatalf("retransmit %d with the claim skipped: err = %v, want the frame sent", i, err)
		}
	}
	if got := scrape(t, addr, "bre_dedup_skipped_total", `claim="retransmit"`); got != n {
		t.Errorf(`bre_dedup_skipped_total{claim="retransmit"} = %v, want %d`, got, n)
	}
	if got := scrape(t, addr, "bre_dedup_errors_total", ""); got != 0 {
		t.Errorf("bre_dedup_errors_total = %v, want 0: a skip is not a backend error", got)
	}
	if got := scrape(t, addr, "bre_retransmit_dedup_total", ""); got != 0 {
		t.Errorf("bre_retransmit_dedup_total = %v, want 0: a skip is not a sibling's claim", got)
	}
	if got := warnLines(logs.String()); got != 0 {
		t.Errorf("%d warn/error lines for skipped claims, want none:\n%s", got, logs.String())
	}
}

// redisDedup is a dedup store on a fresh miniredis, as siblings share one.
func redisDedup(t *testing.T) cache.Deduper {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	backend, err := scache.Open(context.Background(), scache.Config{Backend: scache.BackendRedis, RedisAddr: mr.Addr()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	return cache.NewStore(backend, "test:", time.Second)
}

// The first NACK of each requester shares the frame's one claim; a requester
// asking again inside the window is sent the frame past the claim and counted
// as a repeat. With no requester (a proxied recovery) the claim alone rules.
func TestRetransmitFor_RepeatFromRequesterIsSent(t *testing.T) {
	rec, err := metrics.New("retransmit-test", 1, "", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	addr := serveMetrics(t, rec)
	dd := redisDedup(t)
	r := New(shard.New(0xFF05, shard.DefaultGroupID, 2), nil, 9001, time.Minute, dd, rec, false)
	raw := make([]byte, 100)
	raw[55] = 3
	a, b := net.ParseIP("2001:db8::a"), net.ParseIP("2001:db8::b")

	if err := r.RetransmitFor(raw, [32]byte{}, a); err != nil {
		t.Fatalf("A's first NACK: %v, want the frame sent", err)
	}
	if err := r.RetransmitFor(raw, [32]byte{}, b); !errors.Is(err, ErrDedupSuppressed) {
		t.Fatalf("B's first NACK: err = %v, want ErrDedupSuppressed (A's copy serves B)", err)
	}
	for _, ip := range []net.IP{a, b, a} {
		if err := r.RetransmitFor(raw, [32]byte{}, ip); err != nil {
			t.Fatalf("repeat NACK from %s: %v, want the frame sent", ip, err)
		}
	}
	if got := scrape(t, addr, "bre_retransmit_repeats_total", ""); got != 3 {
		t.Errorf("bre_retransmit_repeats_total = %v, want 3", got)
	}
	if got := scrape(t, addr, "bre_retransmit_dedup_total", ""); got != 1 {
		t.Errorf("bre_retransmit_dedup_total = %v, want 1 (only B's first NACK)", got)
	}

	// No requester: the frame's claim decides alone.
	other := make([]byte, 100)
	other[55] = 4
	if err := r.Retransmit(other, [32]byte{}); err != nil {
		t.Fatalf("first claim without a requester: %v", err)
	}
	if err := r.Retransmit(other, [32]byte{}); !errors.Is(err, ErrDedupSuppressed) {
		t.Fatalf("second claim without a requester: err = %v, want ErrDedupSuppressed", err)
	}
}

// With the backend down a requester's NACK makes one failed call, not two:
// the mark's failure fails the NACK open without trying the frame's claim.
func TestRetransmitFor_BackendErrorCostsOneCall(t *testing.T) {
	dd := &failingDedup{}
	r := New(shard.New(0xFF05, shard.DefaultGroupID, 2), nil, 9001, time.Minute, dd, nil, false)
	raw := make([]byte, 100)
	raw[55] = 5
	for i := 0; i < 3; i++ {
		if err := r.RetransmitFor(raw, [32]byte{}, net.ParseIP("2001:db8::a")); err != nil {
			t.Fatalf("retransmit %d with the backend down: %v, want the frame sent", i, err)
		}
	}
	if got := dd.calls.Load(); got != 3 {
		t.Errorf("backend asked %d times for 3 NACKs, want 3", got)
	}
}

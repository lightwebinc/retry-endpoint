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

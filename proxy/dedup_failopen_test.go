package proxy

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
// contain label, 0 when there is none.
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

// An in-flight claim the backend cannot answer fails open, as the retransmit
// claim does: every gap is still recovered, each failed claim is counted, and
// the log gets one line per window instead of none (the error used to be
// logged at debug only, so a dead backend was invisible at the default level).
func TestRecover_DedupBackendErrorFailsOpen(t *testing.T) {
	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(prev)

	rec, err := metrics.New("proxy-test", 1, "", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	addr := serveMetrics(t, rec)

	fr := makeFrame(9, 9)
	up := startUpstream(t, "serve", fr)
	defer up.close()

	dd := &failingDedup{}
	frt := &fakeRetrans{}
	c := newTestClient(Config{
		Upstreams:   []string{up.addr()},
		Cache:       newFakeCache(),
		Retrans:     frt,
		Multicast:   true,
		Dedup:       dd,
		DedupWindow: time.Minute,
		Rec:         rec,
	})

	// The same gap twice: with no claim possible, both recoveries run.
	c.recover(context.Background(), job{hashKey: 9, seq: 9})
	c.recover(context.Background(), job{hashKey: 9, seq: 9})

	if got := frt.count(); got != 2 {
		t.Errorf("retransmit count = %d, want 2 (no claim could be made, so neither recovery is skipped)", got)
	}
	if got := dd.calls.Load(); got != 2 {
		t.Errorf("SET NX attempts = %d, want 2", got)
	}
	if got := scrape(t, addr, "bre_dedup_errors_total", `claim="proxy"`); got != 2 {
		t.Errorf(`bre_dedup_errors_total{claim="proxy"} = %v, want 2`, got)
	}
	if got := scrape(t, addr, "bre_proxy_inflight_dedup_total", ""); got != 0 {
		t.Errorf("bre_proxy_inflight_dedup_total = %v, want 0: a failed claim is not a sibling's claim", got)
	}
	if got := strings.Count(logs.String(), "level=WARN"); got != 1 {
		t.Errorf("%d warn log lines for 2 failed claims, want 1:\n%s", got, logs.String())
	}
}

// skippingDedup is a claim breaker that is open: every claim is skipped
// without reaching the backend.
type skippingDedup struct{ calls atomic.Int64 }

func (d *skippingDedup) SetNX(_, _ []byte, _ time.Duration) (bool, error) {
	d.calls.Add(1)
	return false, cache.ErrClaimSkipped
}

// A skipped in-flight claim fails open at once like a failed one, counted as
// a skip, not an error, and not logged.
func TestRecover_ClaimSkippedFailsOpen(t *testing.T) {
	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(prev)

	rec, err := metrics.New("proxy-test", 1, "", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	addr := serveMetrics(t, rec)

	up := startUpstream(t, "serve", makeFrame(11, 11))
	defer up.close()
	frt := &fakeRetrans{}
	c := newTestClient(Config{
		Upstreams:   []string{up.addr()},
		Cache:       newFakeCache(),
		Retrans:     frt,
		Multicast:   true,
		Dedup:       &skippingDedup{},
		DedupWindow: time.Minute,
		Rec:         rec,
	})
	c.recover(context.Background(), job{hashKey: 11, seq: 11})
	c.recover(context.Background(), job{hashKey: 11, seq: 11})

	if got := frt.count(); got != 2 {
		t.Errorf("retransmit count = %d, want 2 (both recoveries run without a claim)", got)
	}
	if got := scrape(t, addr, "bre_dedup_skipped_total", `claim="proxy"`); got != 2 {
		t.Errorf(`bre_dedup_skipped_total{claim="proxy"} = %v, want 2`, got)
	}
	if got := scrape(t, addr, "bre_dedup_errors_total", ""); got != 0 {
		t.Errorf("bre_dedup_errors_total = %v, want 0", got)
	}
	if got := strings.Count(logs.String(), "level=WARN"); got != 0 {
		t.Errorf("%d warn lines for skipped claims, want none:\n%s", got, logs.String())
	}
}

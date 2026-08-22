package metrics

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"
)

func newRecorder(t *testing.T) *Recorder {
	t.Helper()
	r, err := New("test", 2, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		r.Shutdown(ctx)
	})
	return r
}

func TestNew(t *testing.T) {
	newRecorder(t)
}

func TestNew_EmptyInstanceID(t *testing.T) {
	r, err := New("", 1, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, c := context.WithTimeout(context.Background(), time.Second)
		defer c()
		r.Shutdown(ctx)
	}()
}

func TestAllCountersSafeToCall(t *testing.T) {
	r := newRecorder(t)
	r.CacheHit()
	r.CacheMiss()
	r.CacheSize(42)
	r.CacheError()
	r.NACKRequest()
	r.Retransmit()
	r.RetransmitDedup()
	r.ResponseSent("ack")
	r.ResponseSent("miss")
	r.ResponseSendError("ack")
	r.RateLimitDrop("ip")
	r.FrameReceived()
	r.FrameCached("fd00:5::a8")
	r.FrameDropped("decode")
	r.BeaconAdvertSent()
	r.WorkerReady()
	r.WorkerDone()
}

func TestHealthz(t *testing.T) {
	r := newRecorder(t)
	w := httptest.NewRecorder()
	r.handleHealthz(w, httptest.NewRequest("GET", "/", nil))
	if w.Code != http.StatusOK {
		t.Errorf("code=%d", w.Code)
	}
	if !strings.Contains(w.Body.String(), `"ok"`) {
		t.Errorf("body: %q", w.Body.String())
	}
}

func TestReadyz_Starting(t *testing.T) {
	r := newRecorder(t)
	w := httptest.NewRecorder()
	r.handleReadyz(w, httptest.NewRequest("GET", "/", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("starting code=%d", w.Code)
	}
}

func TestReadyz_Ready(t *testing.T) {
	r := newRecorder(t)
	r.WorkerReady()
	r.WorkerReady()
	w := httptest.NewRecorder()
	r.handleReadyz(w, httptest.NewRequest("GET", "/", nil))
	if w.Code != http.StatusOK {
		t.Errorf("ready code=%d", w.Code)
	}
}

func TestReadyz_Draining(t *testing.T) {
	r := newRecorder(t)
	r.WorkerReady()
	r.WorkerReady()
	r.SetDraining()
	w := httptest.NewRecorder()
	r.handleReadyz(w, httptest.NewRequest("GET", "/", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("draining code=%d", w.Code)
	}
}

func TestServe(t *testing.T) {
	r := newRecorder(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	done := make(chan struct{})
	served := make(chan struct{})
	go func() { r.Serve(addr, done); close(served) }()

	var resp *http.Response
	for i := 0; i < 20; i++ {
		time.Sleep(50 * time.Millisecond)
		resp, err = http.Get("http://" + addr + "/healthz")
		if err == nil {
			break
		}
	}
	if err != nil {
		t.Fatalf("healthz: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	close(done)
	select {
	case <-served:
	case <-time.After(10 * time.Second):
		t.Fatal("Serve hung")
	}
}

// findSeries returns the label value of `label` on the first sample of metric
// `name` in the recorder's Prometheus registry, and whether it was found.
func findSeries(t *testing.T, r *Recorder, name, label string) (string, bool) {
	t.Helper()
	mfs, err := r.promReg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == label {
					return lp.GetValue(), true
				}
			}
		}
	}
	return "", false
}

func TestSetOwnSource(t *testing.T) {
	r := newRecorder(t)
	r.SetOwnSource("fd00:51::1")

	got, ok := findSeries(t, r, "bre_own_source_info", "source")
	if !ok {
		t.Fatal("bre_own_source_info not exported")
	}
	if got != "fd00:51::1" {
		t.Fatalf("source label = %q, want %q", got, "fd00:51::1")
	}
}

// The alert joins bre_own_source_info to bre_cache_stored_total on the
// `source` label, so a non-canonical BIND_SOURCE literal must still render
// byte-identically to the spelling the ingress path produces from the wire
// address. Divergence makes `unless on(site, source)` match nothing and the
// own-source false positive returns silently.
func TestSetOwnSource_MatchesDataPathSpelling(t *testing.T) {
	const wire = "fd00:51:0:0:0:0:0:1" // same address, expanded

	r := newRecorder(t)
	r.SetOwnSource(wire)
	info, ok := findSeries(t, r, "bre_own_source_info", "source")
	if !ok {
		t.Fatal("bre_own_source_info not exported")
	}

	// Reproduce exactly what ingress.Worker does with a recvmsg source addr.
	dataPath := netip.AddrFrom16(netip.MustParseAddr(wire).As16()).String()

	if info != dataPath {
		t.Fatalf("label mismatch: info=%q data-path=%q — the alert join would break", info, dataPath)
	}
}

func TestSetOwnSource_UnsetIsNoop(t *testing.T) {
	for _, in := range []string{"", "not-an-address"} {
		r := newRecorder(t)
		r.SetOwnSource(in)
		if _, ok := findSeries(t, r, "bre_own_source_info", "source"); ok {
			t.Fatalf("SetOwnSource(%q) exported a series; want none", in)
		}
	}
}

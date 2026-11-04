package ingress

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/teewire"

	"github.com/lightwebinc/retry-endpoint/metrics"
)

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

// droppedTotal scrapes bre_frames_dropped_total{reason} from addr.
func droppedTotal(t *testing.T, addr, reason string) int64 {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	want := `reason="` + reason + `"`
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "bre_frames_dropped_total{") && strings.Contains(line, want) {
			f := strings.Fields(line)
			v, _ := strconv.ParseFloat(f[len(f)-1], 64)
			return int64(v)
		}
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return 0
}

func newRecorder(t *testing.T) *metrics.Recorder {
	t.Helper()
	rec, err := metrics.New("ingress-test", 1, "", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return rec
}

// orderCache records, per key, the sequence of values stored.
type orderCache struct {
	mockCache
	mu    sync.Mutex
	byKey map[string][]byte // key -> marker byte of each store, in order
	n     int
}

func (c *orderCache) Store(key, value []byte, _ time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.byKey == nil {
		c.byKey = make(map[string][]byte)
	}
	c.byKey[string(key)] = append(c.byKey[string(key)], value[len(value)-1])
	c.n++
	return nil
}

func (c *orderCache) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

func TestPipeline_EveryDatagramInOrderPerKey(t *testing.T) {
	// Several processors, many keys, each key written several times with a
	// marker that counts up: every datagram must arrive, and each key's
	// writes must land in send order, raw and enveloped alike.
	oc := &orderCache{}
	w := newTestWorker(nil)
	w.cache = oc
	w.SetPipeline(64, 4)
	listen, _ := startTee(t, w)

	c, err := net.Dial("udp6", listen.String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	const keys, rounds = 256, 8
	src := netip.MustParseAddrPort("[fd00::7]:9001")
	sent := 0
	for r := range rounds {
		for k := range keys {
			raw := buildRaw(t, uint64(k%16+1), uint64(k+1), []byte{0, byte(r)})
			pkt := raw
			if k%2 == 1 {
				pkt = teewire.AppendEncap(nil, src, raw)
			}
			if _, err := c.Write(pkt); err != nil {
				t.Fatal(err)
			}
			sent++
			if sent%64 == 0 {
				time.Sleep(time.Millisecond) // stay inside the test socket buffer
			}
		}
	}

	deadline := time.Now().Add(5 * time.Second)
	for oc.count() < sent && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := oc.count(); got != sent {
		t.Fatalf("stored %d datagrams, sent %d", got, sent)
	}
	oc.mu.Lock()
	defer oc.mu.Unlock()
	if len(oc.byKey) != keys {
		t.Fatalf("%d keys stored, want %d", len(oc.byKey), keys)
	}
	for k, seq := range oc.byKey {
		for i, m := range seq {
			if int(m) != i {
				t.Fatalf("key %x: writes %v, want 0..%d in order", k, seq, rounds-1)
			}
		}
	}
}

// blockingCache holds every Store until release is closed.
type blockingCache struct {
	mockCache
	release chan struct{}
	entered chan struct{}
}

func (c *blockingCache) Store(key, value []byte, ttl time.Duration) error {
	select {
	case c.entered <- struct{}{}:
	default:
	}
	<-c.release
	return c.mockCache.Store(key, value, ttl)
}

func TestPipeline_FullQueueCountsDrop(t *testing.T) {
	bc := &blockingCache{release: make(chan struct{}), entered: make(chan struct{}, 1)}
	rec := newRecorder(t)
	addr := serveMetrics(t, rec)
	w := newTestWorker(nil)
	w.cache = bc
	w.rec = rec
	w.SetPipeline(1, 1)
	listen, _ := startTee(t, w)

	// One datagram parks the processor in Store, one fills the queue, and
	// the rest find the queue full.
	const n = 5
	sendTo(t, listen, buildRaw(t, 1, 1, nil))
	select {
	case <-bc.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("processor never reached the cache")
	}
	for i := 2; i <= n; i++ {
		sendTo(t, listen, buildRaw(t, 1, uint64(i), nil))
	}

	want := int64(n - 2)
	deadline := time.Now().Add(time.Duration(n) * time.Second)
	for droppedTotal(t, addr, "queue_full") < want && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := droppedTotal(t, addr, "queue_full"); got != want {
		t.Fatalf("queue_full drops = %d, want %d", got, want)
	}
	close(bc.release)
	waitStores(t, &bc.mockCache, 2)
}

func TestPipeline_ShutdownLeavesNothingBlocked(t *testing.T) {
	before := runtime.NumGoroutine()

	// Shut down with the queue full and the processor parked in the cache:
	// the receiver must leave its wait, and once the store returns every
	// pipeline goroutine must exit.
	bc := &blockingCache{release: make(chan struct{}), entered: make(chan struct{}, 1)}
	w := newTestWorker(nil)
	w.cache = bc
	w.log = slog.Default()
	w.SetPipeline(1, 2)
	ctx, cancel := context.WithCancel(context.Background())
	listen := netip.AddrPortFrom(netip.MustParseAddr("::1"), freeLoopbackPort(t))
	exited := make(chan error, 1)
	go func() { exited <- w.RunTee(ctx, listen) }()
	time.Sleep(50 * time.Millisecond)
	for i := 1; i <= 64; i++ {
		sendTo(t, listen, buildRaw(t, uint64(i), uint64(i), nil))
	}
	select {
	case <-bc.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("processor never reached the cache")
	}
	cancel()
	time.Sleep(50 * time.Millisecond)
	close(bc.release)
	select {
	case err := <-exited:
		if err != nil {
			t.Fatalf("RunTee: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("RunTee did not exit after cancel")
	}

	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before {
		buf := make([]byte, 1<<16)
		t.Fatalf("goroutines %d -> %d after shutdown:\n%s", before, after, buf[:runtime.Stack(buf, true)])
	}
}

func TestShardOf_SameKeySameShardInBothForms(t *testing.T) {
	src := netip.MustParseAddrPort("[fd00::1]:9001")
	for i := range 64 {
		raw := buildRaw(t, uint64(i)*7919, uint64(i)+1, []byte("x"))
		enc := teewire.AppendEncap(nil, src, raw)
		if shardOf(raw, true) != shardOf(enc, true) {
			t.Fatalf("frame %d: raw and enveloped forms shard apart", i)
		}
		if shardOf(raw, false) != shardOf(raw, true) {
			t.Fatalf("frame %d: multicast and tee shard apart", i)
		}
	}
	if shardOf([]byte{1, 2, 3}, true) != 0 || shardOf(nil, false) != 0 {
		t.Fatal("short datagrams must shard to 0")
	}
	// One flow (fixed HashKey) must still spread across processors.
	seen := map[uint64]bool{}
	var pkt [56]byte
	binary.BigEndian.PutUint64(pkt[40:48], 42)
	for s := range 64 {
		binary.BigEndian.PutUint64(pkt[48:56], uint64(s+1))
		seen[shardOf(pkt[:], false)%4] = true
	}
	if len(seen) != 4 {
		t.Fatalf("one flow reached %d of 4 processors", len(seen))
	}
}

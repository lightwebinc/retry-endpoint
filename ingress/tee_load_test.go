package ingress

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/cache/memory"
	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/teewire"

	"github.com/lightwebinc/retry-endpoint/cache"
	"github.com/lightwebinc/retry-endpoint/metrics"
)

// The tee load test is a loopback benchmark, not a unit test: it runs only
// when TEE_LOAD_RATES is set, for example
//
//	TEE_LOAD_RATES=50000,100000,150000 GOMAXPROCS=4 \
//	  go test ./ingress -run '^TestTeeLoad$' -v -count=1
//
// For each rate (datagrams/s) a separate sender process (this test binary,
// re-executed) blasts 9000-byte BRC-130 fragments at a tee socket backed by
// the real in-memory cache and metrics recorder, half raw and half
// teewire-enveloped. It reports datagrams sent, datagrams stored, and the
// socket's kernel drop count from /proc/net/udp6.
//
// Optional: TEE_LOAD_SECONDS (default 5), TEE_LOAD_RCVBUF (default 256 MiB,
// clamped by net.core.rmem_max), TEE_LOAD_KEYS (cache key cap, default
// 262144, about 2.4 GB of 9000-byte frames), TEE_LOAD_DEPTH and
// TEE_LOAD_PROCS (receive pipeline, default the package defaults).

const teeLoadDatagram = 9000

type countingCache struct {
	cache.Cache
	n atomic.Int64
}

func (c *countingCache) Store(key, value []byte, ttl time.Duration) error {
	err := c.Cache.Store(key, value, ttl)
	if err == nil {
		c.n.Add(1)
	}
	return err
}

func TestTeeLoad(t *testing.T) {
	rates := os.Getenv("TEE_LOAD_RATES")
	if rates == "" {
		t.Skip("set TEE_LOAD_RATES to run the tee load benchmark")
	}
	secs := envIntDefault("TEE_LOAD_SECONDS", 5)
	rcvbuf := envIntDefault("TEE_LOAD_RCVBUF", 256<<20)
	t.Logf("%-8s %-10s %-10s %-10s %-10s %-8s", "rate", "sent", "stored", "sockdrop", "qdrop", "loss%")
	for _, rs := range strings.Split(rates, ",") {
		rate, err := strconv.Atoi(strings.TrimSpace(rs))
		if err != nil || rate <= 0 {
			t.Fatalf("bad rate %q", rs)
		}
		sent, stored, sockDrops, qDrops := runTeeLoad(t, rate, secs, rcvbuf)
		loss := 0.0
		if sent > 0 {
			loss = 100 * float64(sent-stored) / float64(sent)
		}
		t.Logf("%-8d %-10d %-10d %-10d %-10d %-8.3f", rate, sent, stored, sockDrops, qDrops, loss)
	}
}

func runTeeLoad(t *testing.T, rate, secs, rcvbuf int) (sent, stored, sockDrops, qDrops int64) {
	t.Helper()
	rec, err := metrics.New("tee-load", 1, "", time.Second)
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}
	metricsAddr := serveMetrics(t, rec)
	be := memory.NewWithOptions(memory.Options{MaxKeys: envIntDefault("TEE_LOAD_KEYS", 1<<18)})
	defer func() { _ = be.Close() }()
	cc := &countingCache{Cache: cache.NewStore(be, "f:", 0)}
	w := New(nil, 0, nil, cc, rec, TTLConfig{Tx: time.Minute, Block: time.Minute, Subtree: time.Minute, Anchor: time.Minute, BEEF: time.Minute}, false)
	w.log = slog.New(slog.DiscardHandler)
	w.SetRecvBuf(rcvbuf)
	w.SetPipeline(envIntDefault("TEE_LOAD_DEPTH", 0), envIntDefault("TEE_LOAD_PROCS", 0))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listen, errCh := startTeeLoad(t, ctx, w)

	cmd := exec.Command(os.Args[0], "-test.run=^TestTeeLoadSender$", "-test.count=1")
	cmd.Env = append(os.Environ(),
		"TEE_LOAD_SENDER="+fmt.Sprintf("%s,%d,%d", listen, rate, secs))
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	sc := bufio.NewScanner(out)
	for sc.Scan() {
		if v, ok := strings.CutPrefix(sc.Text(), "SENT "); ok {
			sent, _ = strconv.ParseInt(v, 10, 64)
		}
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("sender: %v", err)
	}

	// Let the receiver drain what the socket still holds.
	last := int64(-1)
	for i := 0; i < 100; i++ {
		cur := cc.n.Load()
		if cur == last {
			break
		}
		last = cur
		time.Sleep(200 * time.Millisecond)
	}
	stored = cc.n.Load()
	sockDrops = udp6Drops(t, listen.Port())
	qDrops = droppedTotal(t, metricsAddr, "queue_full")
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("RunTee: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("RunTee did not exit")
	}
	return sent, stored, sockDrops, qDrops
}

func startTeeLoad(t *testing.T, ctx context.Context, w *Worker) (netip.AddrPort, chan error) {
	t.Helper()
	errCh := make(chan error, 1)
	for attempt := 0; attempt < 5; attempt++ {
		listen := netip.AddrPortFrom(netip.MustParseAddr("::1"), freeLoopbackPort(t))
		go func() { errCh <- w.RunTee(ctx, listen) }()
		select {
		case err := <-errCh:
			t.Logf("bind %s: %v", listen, err)
			continue
		case <-time.After(100 * time.Millisecond):
			return listen, errCh
		}
	}
	t.Fatal("could not bind a tee socket")
	return netip.AddrPort{}, nil
}

// udp6Drops reads the drops column of the socket bound to port on ::1.
func udp6Drops(t *testing.T, port uint16) int64 {
	t.Helper()
	b, err := os.ReadFile("/proc/net/udp6")
	if err != nil {
		return -1
	}
	want := fmt.Sprintf(":%04X", port)
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) < 13 || !strings.HasSuffix(f[1], want) {
			continue
		}
		n, _ := strconv.ParseInt(f[len(f)-1], 10, 64)
		return n
	}
	return -1
}

func envIntDefault(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

// TestTeeLoadSender is the sender half of TestTeeLoad, run in its own
// process so it does not share the receiver's scheduler.
func TestTeeLoadSender(t *testing.T) {
	spec := os.Getenv("TEE_LOAD_SENDER")
	if spec == "" {
		t.Skip("sender half of TestTeeLoad")
	}
	parts := strings.Split(spec, ",")
	dst := netip.MustParseAddrPort(parts[0])
	rate, _ := strconv.Atoi(parts[1])
	secs, _ := strconv.Atoi(parts[2])

	// Four sockets, each paced to a quarter of the rate. Even senders emit
	// raw frames (the proxy's form), odd ones teewire envelopes (the
	// listener's form).
	const senders = 4
	var total atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	end := start.Add(time.Duration(secs) * time.Second)
	for s := 0; s < senders; s++ {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			c, err := net.DialUDP("udp6", nil, net.UDPAddrFromAddrPort(dst))
			if err != nil {
				t.Error(err)
				return
			}
			defer func() { _ = c.Close() }()
			_ = c.SetWriteBuffer(16 << 20)
			encap := s%2 == 1
			plen := teeLoadDatagram - frame.HeaderSizeV3
			if encap {
				plen -= teewire.HeaderSize
			}
			fr := make([]byte, frame.HeaderSizeV3+plen)
			var txid, stid [32]byte
			if _, err := frame.EncodeFragment(fr, txid, stid, 1, 1, 1<<20, 0, 1, frame.FrameVerV2, make([]byte, plen)); err != nil {
				t.Error(err)
				return
			}
			pkt := fr
			off := 0
			if encap {
				pkt = teewire.AppendEncap(nil, netip.MustParseAddrPort("[fd00::1]:9001"), fr)
				off = teewire.HeaderSize
			}
			per := float64(rate) / senders
			var n int64
			for {
				now := time.Now()
				if now.After(end) {
					break
				}
				due := int64(per * now.Sub(start).Seconds())
				if n >= due {
					time.Sleep(50 * time.Microsecond)
					continue
				}
				for ; n < due; n++ {
					// Eight flows per sender; SeqNum unique per datagram.
					binary.BigEndian.PutUint64(pkt[off+40:off+48], uint64(s)<<8|uint64(n%8))
					binary.BigEndian.PutUint64(pkt[off+48:off+56], uint64(n+1))
					if _, err := c.Write(pkt); err != nil {
						continue
					}
					total.Add(1)
				}
			}
		}(s)
	}
	wg.Wait()
	fmt.Printf("SENT %d\n", total.Load())
}

package proxy

import (
	"context"
	"encoding/binary"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
)

// --- test doubles ---

type fakeCache struct {
	mu     sync.Mutex
	stored map[string][]byte
}

func newFakeCache() *fakeCache { return &fakeCache{stored: map[string][]byte{}} }

func (c *fakeCache) Store(key, value []byte, _ time.Duration) error {
	c.mu.Lock()
	c.stored[string(key)] = append([]byte{}, value...)
	c.mu.Unlock()
	return nil
}

func (c *fakeCache) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.stored)
}

type fakeRetrans struct {
	mu      sync.Mutex
	frames  [][]byte
	unicast []string
}

func (r *fakeRetrans) Retransmit(raw []byte, _ [32]byte) error {
	r.mu.Lock()
	r.frames = append(r.frames, append([]byte{}, raw...))
	r.mu.Unlock()
	return nil
}

func (r *fakeRetrans) RetransmitUnicast(raw []byte, dst *net.UDPAddr) error {
	r.mu.Lock()
	r.unicast = append(r.unicast, dst.String())
	r.mu.Unlock()
	return nil
}

func (r *fakeRetrans) unicastDsts() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string{}, r.unicast...)
}

func (r *fakeRetrans) unicastCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.unicast)
}

func (r *fakeRetrans) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.frames)
}

type fakeDedup struct {
	mu   sync.Mutex
	seen map[string]bool
}

func newFakeDedup() *fakeDedup { return &fakeDedup{seen: map[string]bool{}} }

func (d *fakeDedup) SetNX(key, _ []byte, _ time.Duration) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.seen[string(key)] {
		return false, nil
	}
	d.seen[string(key)] = true
	return true, nil
}

// fakeUpstream is a UDP server emulating an upstream retry-endpoint. It either
// returns ACK + frame, just MISS, or nothing (timeout).
type fakeUpstream struct {
	conn     *net.UDPConn
	mode     string // "serve" | "miss" | "silent"
	frame    []byte
	gotProxy chan bool // receives the FlagProxied bit of the last NACK
}

func startUpstream(t *testing.T, mode string, fr []byte) *fakeUpstream {
	t.Helper()
	conn, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.IPv6loopback})
	if err != nil {
		t.Fatalf("upstream listen: %v", err)
	}
	u := &fakeUpstream{conn: conn, mode: mode, frame: fr, gotProxy: make(chan bool, 4)}
	go u.serve()
	return u
}

func (u *fakeUpstream) addr() string { return u.conn.LocalAddr().String() }
func (u *fakeUpstream) close()       { _ = u.conn.Close() }

func (u *fakeUpstream) serve() {
	buf := make([]byte, 1500)
	for {
		n, src, err := u.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		if n >= nackSize {
			u.gotProxy <- (buf[7]&FlagProxied != 0)
		}
		switch u.mode {
		case "silent":
			// no response
		case "miss":
			var resp [respSize]byte
			binary.BigEndian.PutUint32(resp[0:4], frame.MagicBSV)
			binary.BigEndian.PutUint16(resp[4:6], frame.ProtoVer)
			resp[6] = msgTypeMISS
			_, _ = u.conn.WriteToUDP(resp[:], src)
		case "serve":
			// ACK first, then the frame (separate datagrams).
			var ack [respSize]byte
			binary.BigEndian.PutUint32(ack[0:4], frame.MagicBSV)
			binary.BigEndian.PutUint16(ack[4:6], frame.ProtoVer)
			ack[6] = msgTypeACK
			_, _ = u.conn.WriteToUDP(ack[:], src)
			_, _ = u.conn.WriteToUDP(u.frame, src)
		}
	}
}

// makeFrame builds a minimal valid BRC-124 (V2) frame with the given hashKey/seq.
func makeFrame(hashKey, seq uint64) []byte {
	raw := make([]byte, frame.HeaderSize)
	binary.BigEndian.PutUint32(raw[0:4], frame.MagicBSV)
	binary.BigEndian.PutUint16(raw[4:6], frame.ProtoVer)
	raw[6] = frame.FrameVerV2
	binary.BigEndian.PutUint64(raw[40:48], hashKey)
	binary.BigEndian.PutUint64(raw[48:56], seq)
	return raw
}

func newTestClient(cfg Config) *Client {
	if cfg.TTLs.Tx == 0 {
		cfg.TTLs = TTLConfig{Tx: time.Minute, Block: time.Minute, Subtree: time.Minute, Anchor: time.Minute}
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 500 * time.Millisecond
	}
	cfg.Workers = 1
	return New(cfg)
}

func TestRecover_Serve(t *testing.T) {
	fr := makeFrame(0x1122, 7)
	up := startUpstream(t, "serve", fr)
	defer up.close()

	fc := newFakeCache()
	frt := &fakeRetrans{}
	c := newTestClient(Config{
		Upstreams: []string{up.addr()},
		Cache:     fc,
		Retrans:   frt,
		Multicast: true,
	})

	c.recover(context.Background(), job{hashKey: 0x1122, seq: 7})

	// The NACK must carry the Proxied flag.
	select {
	case proxied := <-up.gotProxy:
		if !proxied {
			t.Error("upstream did not see FlagProxied set")
		}
	case <-time.After(time.Second):
		t.Fatal("upstream received no NACK")
	}
	if fc.count() != 1 {
		t.Errorf("re-cache count = %d, want 1", fc.count())
	}
	if frt.count() != 1 {
		t.Errorf("retransmit count = %d, want 1", frt.count())
	}
}

func TestRecover_MissThenExhausted(t *testing.T) {
	up := startUpstream(t, "miss", nil)
	defer up.close()

	fc := newFakeCache()
	frt := &fakeRetrans{}
	c := newTestClient(Config{
		Upstreams: []string{up.addr()},
		Cache:     fc,
		Retrans:   frt,
		Multicast: true,
	})

	c.recover(context.Background(), job{hashKey: 1, seq: 1})

	if fc.count() != 0 || frt.count() != 0 {
		t.Errorf("expected no recovery on MISS: cache=%d retrans=%d", fc.count(), frt.count())
	}
}

func TestRecover_Timeout(t *testing.T) {
	up := startUpstream(t, "silent", nil)
	defer up.close()

	fc := newFakeCache()
	frt := &fakeRetrans{}
	c := newTestClient(Config{
		Upstreams: []string{up.addr()},
		Cache:     fc,
		Retrans:   frt,
		Multicast: true,
		Timeout:   150 * time.Millisecond,
	})

	start := time.Now()
	c.recover(context.Background(), job{hashKey: 1, seq: 1})
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("recover blocked too long: %s", elapsed)
	}
	if fc.count() != 0 {
		t.Errorf("expected no recovery on timeout")
	}
}

func TestRecover_InflightDedup(t *testing.T) {
	fr := makeFrame(5, 5)
	up := startUpstream(t, "serve", fr)
	defer up.close()

	dd := newFakeDedup()
	fc := newFakeCache()
	frt := &fakeRetrans{}
	c := newTestClient(Config{
		Upstreams:   []string{up.addr()},
		Cache:       fc,
		Retrans:     frt,
		Multicast:   true,
		Dedup:       dd,
		DedupWindow: time.Minute,
	})

	// First claim wins and recovers.
	c.recover(context.Background(), job{hashKey: 5, seq: 5})
	// Second claim for same key is skipped (no second recovery).
	c.recover(context.Background(), job{hashKey: 5, seq: 5})

	if frt.count() != 1 {
		t.Errorf("retransmit count = %d, want 1 (dedup should skip the second)", frt.count())
	}
}

func TestEnqueue_DropsWhenFull(t *testing.T) {
	c := New(Config{
		Upstreams:  []string{"[::1]:9300"},
		Cache:      newFakeCache(),
		Retrans:    &fakeRetrans{},
		QueueDepth: 1,
		Workers:    1,
	})
	// Fill the queue without starting workers.
	if !c.Enqueue(1, 1, [32]byte{}, nil) {
		t.Fatal("first enqueue should succeed")
	}
	if c.Enqueue(2, 2, [32]byte{}, nil) {
		t.Error("second enqueue should drop (queue full)")
	}
}

// A recovered frame goes to the listener that asked for it, and only there,
// when the modes say unicast: no multicast into the segment (that is what put
// unsolicited, out-of-order frames in front of every local listener under this
// node's source address), and the requester need not be on this node.
func TestRecover_UnicastToRequester(t *testing.T) {
	fr := makeFrame(0x3344, 9)
	up := startUpstream(t, "serve", fr)
	defer up.close()

	fc := newFakeCache()
	frt := &fakeRetrans{}
	c := newTestClient(Config{
		Upstreams: []string{up.addr()},
		Cache:     fc,
		Retrans:   frt,
		Unicast:   true,
	})
	req := &net.UDPAddr{IP: net.ParseIP("2001:db8::9"), Port: 9001}
	c.recover(context.Background(), job{hashKey: 0x3344, seq: 9, requester: req})

	if fc.count() != 1 {
		t.Errorf("re-cache count = %d, want 1", fc.count())
	}
	if frt.count() != 0 {
		t.Errorf("multicast retransmits = %d, want 0 with multicast mode off", frt.count())
	}
	if got := frt.unicastDsts(); len(got) != 1 || got[0] != req.String() {
		t.Fatalf("unicast dsts = %v, want exactly [%s]", got, req.String())
	}
}

// No requester and no multicast mode: the recovery is cache-warm only, so the
// requester's next NACK hits locally. Nothing is put on the wire.
func TestRecover_NoRequester_CacheWarmOnly(t *testing.T) {
	fr := makeFrame(0x5566, 3)
	up := startUpstream(t, "serve", fr)
	defer up.close()

	fc := newFakeCache()
	frt := &fakeRetrans{}
	c := newTestClient(Config{
		Upstreams: []string{up.addr()},
		Cache:     fc,
		Retrans:   frt,
		Unicast:   true,
	})
	c.recover(context.Background(), job{hashKey: 0x5566, seq: 3})

	if fc.count() != 1 {
		t.Errorf("re-cache count = %d, want 1", fc.count())
	}
	if frt.count() != 0 || frt.unicastCount() != 0 {
		t.Errorf("sends = %d multicast / %d unicast, want 0 / 0", frt.count(), frt.unicastCount())
	}
}

// Both modes on: both deliveries happen (the operator asked for both).
func TestRecover_BothModes(t *testing.T) {
	fr := makeFrame(0x7788, 5)
	up := startUpstream(t, "serve", fr)
	defer up.close()

	fc := newFakeCache()
	frt := &fakeRetrans{}
	c := newTestClient(Config{
		Upstreams: []string{up.addr()},
		Cache:     fc,
		Retrans:   frt,
		Multicast: true,
		Unicast:   true,
	})
	req := &net.UDPAddr{IP: net.ParseIP("2001:db8::a"), Port: 9001}
	c.recover(context.Background(), job{hashKey: 0x7788, seq: 5, requester: req})

	if frt.count() != 1 || frt.unicastCount() != 1 {
		t.Errorf("sends = %d multicast / %d unicast, want 1 / 1", frt.count(), frt.unicastCount())
	}
}

// Two DIFFERENT requesters NACKing the SAME gap must BOTH be served in unicast mode.
//
// Regression test. The in-flight claim keys the gap (hashKey||seq) so sibling endpoints
// do not all fetch it from upstream. That is correct while recovery MULTICASTS — one
// winner's retransmit reaches everyone. Once recovery started unicasting to the
// requester, a gap-only key meant the loser of the race received NOTHING and had to wait
// out its own NACK timeout. Devnet masked this by running CACHE_BACKEND=memory, where the
// deduper is nil and the claim never runs at all; it only bites on a shared backend.
func TestRecover_UnicastDedupIsPerRequester(t *testing.T) {
	fr := makeFrame(7, 7)
	up := startUpstream(t, "serve", fr)
	defer up.close()

	dd := newFakeDedup()
	fc := newFakeCache()
	frt := &fakeRetrans{}
	c := newTestClient(Config{
		Upstreams:   []string{up.addr()},
		Cache:       fc,
		Retrans:     frt,
		Unicast:     true,
		Dedup:       dd,
		DedupWindow: time.Minute,
	})

	a := &net.UDPAddr{IP: net.ParseIP("fd00::a"), Port: 9300}
	b := &net.UDPAddr{IP: net.ParseIP("fd00::b"), Port: 9300}

	c.recover(context.Background(), job{hashKey: 7, seq: 7, requester: a})
	c.recover(context.Background(), job{hashKey: 7, seq: 7, requester: b})

	if got := frt.unicastCount(); got != 2 {
		t.Errorf("unicast count = %d, want 2 — the second requester lost the in-flight "+
			"claim and was never served", got)
	}

	// The SAME requester repeating inside the window must still dedup: that is the
	// upstream-protection this claim exists for.
	c.recover(context.Background(), job{hashKey: 7, seq: 7, requester: a})
	if got := frt.unicastCount(); got != 2 {
		t.Errorf("unicast count = %d after a repeat from the same requester, want 2", got)
	}
}

// Multicast mode must keep the gap-only key: one retransmit serves every requester, so
// widening the key there would multiply identical upstream fetches for no benefit.
func TestRecover_MulticastDedupStaysGapOnly(t *testing.T) {
	fr := makeFrame(8, 8)
	up := startUpstream(t, "serve", fr)
	defer up.close()

	dd := newFakeDedup()
	c := newTestClient(Config{
		Upstreams:   []string{up.addr()},
		Cache:       newFakeCache(),
		Retrans:     &fakeRetrans{},
		Multicast:   true,
		Dedup:       dd,
		DedupWindow: time.Minute,
	})
	frt := c.cfg.Retrans.(*fakeRetrans)

	c.recover(context.Background(), job{hashKey: 8, seq: 8, requester: &net.UDPAddr{IP: net.ParseIP("fd00::a"), Port: 9300}})
	c.recover(context.Background(), job{hashKey: 8, seq: 8, requester: &net.UDPAddr{IP: net.ParseIP("fd00::b"), Port: 9300}})

	if got := frt.count(); got != 1 {
		t.Errorf("multicast retransmit count = %d, want 1 (gap-only claim must still dedup)", got)
	}
}

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
	mu     sync.Mutex
	frames [][]byte
}

func (r *fakeRetrans) Retransmit(raw []byte, _ [32]byte) error {
	r.mu.Lock()
	r.frames = append(r.frames, append([]byte{}, raw...))
	r.mu.Unlock()
	return nil
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
	if !c.Enqueue(1, 1, [32]byte{}) {
		t.Fatal("first enqueue should succeed")
	}
	if c.Enqueue(2, 2, [32]byte{}) {
		t.Error("second enqueue should drop (queue full)")
	}
}

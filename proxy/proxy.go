// Package proxy implements cross-domain NACK proxying for retry-endpoint.
//
// When a retry-endpoint serving a downstream multicast domain misses a frame
// in its cache, it recovers the frame from an upstream retry-endpoint that
// received the frame directly from the proxy (independent of the bridge that
// dropped it), re-caches it, and multicast-retransmits it into the downstream
// domain. The downstream consumer's gap then auto-fills.
//
// Recovery is asynchronous ("cache-warm"): the server returns MISS immediately
// and enqueues a job here; no NACK worker is held. Because the downstream
// proxy is not joined to the upstream shard groups, the recovered frame must
// return by unicast — the upstream endpoint must run with unicast retransmit
// enabled (proxied NACKs are always served a unicast copy; see the server).
//
// Proxied NACKs carry FlagProxied so an upstream endpoint never re-proxies
// them, bounding any chain to a single hop.
package proxy

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/lightwebinc/shard-common/frame"

	"github.com/lightwebinc/retry-endpoint/metrics"
)

const (
	nackSize    = 64
	respSize    = 16
	msgTypeNACK = 0x10
	msgTypeMISS = 0x11
	msgTypeACK  = 0x12

	// FlagProxied marks a NACK (Flags byte, offset 7) as recovered on behalf
	// of a downstream domain. An endpoint receiving a NACK with this bit set
	// MUST NOT re-proxy it, bounding the chain to one hop (BRC-126).
	FlagProxied byte = 0x01
)

// Cache is the re-cache surface the proxy needs (subset of cache.Cache).
type Cache interface {
	Store(key, value []byte, ttl time.Duration) error
}

// Deduper is the cross-instance in-flight-claim surface (subset of cache.Deduper).
type Deduper interface {
	SetNX(key, value []byte, ttl time.Duration) (bool, error)
}

// Retransmitter multicasts a recovered frame into the downstream domain.
type Retransmitter interface {
	Retransmit(raw []byte, txID [32]byte) error
}

// TTLConfig mirrors the per-FrameVer cache TTLs used by the ingress worker so
// recovered frames are re-cached with the same lifetime as freshly received ones.
type TTLConfig struct {
	Tx      time.Duration
	Block   time.Duration
	Subtree time.Duration
	Anchor  time.Duration
	BEEF    time.Duration // BRC-148 BEEF object (FrameVer V9)
}

// Config holds the proxy client's dependencies and tuning.
type Config struct {
	Upstreams    []string // upstream NACK targets (host:port)
	Timeout      time.Duration
	MaxEndpoints int // upstream endpoints tried per gap; <=0 = all
	DedupWindow  time.Duration
	Workers      int
	QueueDepth   int
	Dedup        Deduper // may be nil (no cross-instance in-flight dedup)
	Cache        Cache
	Retrans      Retransmitter
	TTLs         TTLConfig
	Rec          *metrics.Recorder
}

type job struct {
	hashKey uint64
	seq     uint64
	subtree [32]byte
}

// Client recovers missed frames from upstream retry-endpoints.
type Client struct {
	cfg   Config
	log   *slog.Logger
	queue chan job
}

// New constructs a Client. Cache and Retrans must be non-nil; Upstreams must
// be non-empty (validated by config).
func New(cfg Config) *Client {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 300 * time.Millisecond
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	if cfg.QueueDepth <= 0 {
		cfg.QueueDepth = 1024
	}
	if cfg.MaxEndpoints <= 0 || cfg.MaxEndpoints > len(cfg.Upstreams) {
		cfg.MaxEndpoints = len(cfg.Upstreams)
	}
	if cfg.DedupWindow <= 0 {
		cfg.DedupWindow = 60 * time.Second
	}
	return &Client{
		cfg:   cfg,
		log:   slog.Default().With("component", "proxy"),
		queue: make(chan job, cfg.QueueDepth),
	}
}

// Enqueue submits a recovery job for the (hashKey, seq) gap. Non-blocking: if
// the queue is full the job is dropped and counted. Returns false on drop.
func (c *Client) Enqueue(hashKey, seq uint64, subtree [32]byte) bool {
	select {
	case c.queue <- job{hashKey: hashKey, seq: seq, subtree: subtree}:
		return true
	default:
		if c.cfg.Rec != nil {
			c.cfg.Rec.ProxyQueueDropped()
		}
		return false
	}
}

// Start launches the worker pool. Workers run until ctx is cancelled.
func (c *Client) Start(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < c.cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case j := <-c.queue:
					c.recover(ctx, j)
				}
			}
		}()
	}
	c.log.Info("proxy client started", "upstreams", len(c.cfg.Upstreams), "workers", c.cfg.Workers)
	go func() { <-ctx.Done(); wg.Wait() }()
}

// recover claims the gap, fetches the frame from upstream, re-caches it, and
// multicast-retransmits it into the downstream domain.
func (c *Client) recover(ctx context.Context, j job) {
	if c.cfg.Rec != nil {
		c.cfg.Rec.ProxyRequest()
	}

	// In-flight claim dedups sibling downstream endpoints when a shared cache
	// backend is configured. nil deduper (memory backend) => per-process only.
	if c.cfg.Dedup != nil {
		var claim [16]byte
		binary.BigEndian.PutUint64(claim[0:8], j.hashKey)
		binary.BigEndian.PutUint64(claim[8:16], j.seq)
		set, err := c.cfg.Dedup.SetNX(claim[:], []byte("1"), c.cfg.DedupWindow)
		if err != nil {
			c.log.Debug("proxy in-flight SETNX error", "err", err)
		} else if !set {
			if c.cfg.Rec != nil {
				c.cfg.Rec.ProxyInflightDedup()
			}
			return
		}
	}

	for i := 0; i < c.cfg.MaxEndpoints; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}
		raw, ok := c.fetch(c.cfg.Upstreams[i], j)
		if !ok {
			continue
		}
		if err := c.store(raw); err != nil {
			c.log.Warn("proxy re-cache error", "err", err)
		}
		var txID [32]byte
		copy(txID[:], raw[8:40])
		if err := c.cfg.Retrans.Retransmit(raw, txID); err != nil {
			c.log.Warn("proxy retransmit error", "err", err)
		}
		if c.cfg.Rec != nil {
			c.cfg.Rec.ProxyRecovered()
		}
		c.log.Debug("proxy recovered frame", "hash_key", j.hashKey, "seq_num", j.seq, "upstream", c.cfg.Upstreams[i])
		return
	}

	if c.cfg.Rec != nil {
		c.cfg.Rec.ProxyFailed("exhausted")
	}
	c.log.Debug("proxy recovery failed", "hash_key", j.hashKey, "seq_num", j.seq)
}

// fetch sends a proxied NACK to one upstream and waits for the unicast frame
// return (tolerating the 16-byte ACK arriving separately). Returns the raw
// frame bytes on success.
func (c *Client) fetch(endpoint string, j job) ([]byte, bool) {
	addr, err := net.ResolveUDPAddr("udp6", endpoint)
	if err != nil {
		c.log.Warn("proxy: cannot resolve upstream", "endpoint", endpoint, "err", err)
		return nil, false
	}
	conn, err := net.ListenPacket("udp6", "[::]:0")
	if err != nil {
		c.log.Warn("proxy: listen failed", "err", err)
		return nil, false
	}
	defer func() { _ = conn.Close() }()

	var nack [nackSize]byte
	binary.BigEndian.PutUint32(nack[0:4], frame.MagicBSV)
	binary.BigEndian.PutUint16(nack[4:6], frame.ProtoVer)
	nack[6] = msgTypeNACK
	nack[7] = FlagProxied
	binary.BigEndian.PutUint64(nack[8:16], j.hashKey)
	binary.BigEndian.PutUint64(nack[16:24], j.seq)
	binary.BigEndian.PutUint64(nack[24:32], j.seq)
	copy(nack[32:64], j.subtree[:])
	if _, err := conn.WriteTo(nack[:], addr); err != nil {
		c.log.Warn("proxy: NACK send failed", "endpoint", endpoint, "err", err)
		return nil, false
	}

	deadline := time.Now().Add(c.cfg.Timeout)
	_ = conn.SetReadDeadline(deadline)
	buf := make([]byte, 64*1024)
	for {
		n, _, err := conn.ReadFrom(buf)
		if err != nil {
			return nil, false // timeout or error
		}
		// A frame return: BRC-12x header, magic matches, >= header size.
		if n >= frame.HeaderSize && binary.BigEndian.Uint32(buf[0:4]) == frame.MagicBSV && buf[6] != msgTypeACK && buf[6] != msgTypeMISS {
			out := make([]byte, n)
			copy(out, buf[:n])
			return out, true
		}
		// A 16-byte control response: MISS => give up on this upstream now;
		// ACK => keep waiting for the frame until the deadline.
		if n == respSize && len(buf) >= 7 && buf[6] == msgTypeMISS {
			return nil, false
		}
		// otherwise (ACK or junk) loop until deadline.
	}
}

// store re-caches a recovered frame under HashKey ∥ SeqNum with the per-FrameVer TTL.
func (c *Client) store(raw []byte) error {
	if len(raw) < frame.HeaderSize {
		return fmt.Errorf("proxy: short frame (%d bytes)", len(raw))
	}
	var key [16]byte
	copy(key[0:8], raw[40:48])  // HashKey
	copy(key[8:16], raw[48:56]) // SeqNum
	return c.cfg.Cache.Store(key[:], raw, c.ttlFor(raw[6]))
}

// ttlFor selects the re-cache TTL by FrameVer, mirroring the ingress worker.
func (c *Client) ttlFor(ver byte) time.Duration {
	switch ver {
	case frame.FrameVerV4:
		return c.cfg.TTLs.Block
	case frame.FrameVerV5:
		return c.cfg.TTLs.Subtree
	case frame.FrameVerV6:
		return c.cfg.TTLs.Anchor
	case frame.FrameVerV9:
		return c.cfg.TTLs.BEEF
	default:
		return c.cfg.TTLs.Tx
	}
}

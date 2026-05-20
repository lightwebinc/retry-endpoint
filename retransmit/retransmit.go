// Package retransmit handles retransmitting cached frames to the multicast network.
package retransmit

import (
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	"github.com/lightwebinc/bitcoin-shard-common/frame"
	"github.com/lightwebinc/bitcoin-shard-common/shard"

	"github.com/lightwebinc/bitcoin-retry-endpoint/cache/redis"
	"github.com/lightwebinc/bitcoin-retry-endpoint/metrics"
)

// Retransmitter handles retransmitting cached frames.
type Retransmitter struct {
	engine      *shard.Engine
	ifaces      []*net.Interface
	egressPort  int
	dedupWindow time.Duration
	redisCache  *redis.Cache // nil if using in-memory cache
	rec         *metrics.Recorder
	debug       bool
	log         *slog.Logger

	mu      sync.Mutex
	sockets map[string]*net.UDPConn // iface name -> socket
}

// New constructs a Retransmitter.
func New(
	engine *shard.Engine,
	ifaces []*net.Interface,
	egressPort int,
	dedupWindow time.Duration,
	redisCache *redis.Cache,
	rec *metrics.Recorder,
	debug bool,
) *Retransmitter {
	return &Retransmitter{
		engine:      engine,
		ifaces:      ifaces,
		egressPort:  egressPort,
		dedupWindow: dedupWindow,
		redisCache:  redisCache,
		rec:         rec,
		debug:       debug,
		log:         slog.Default().With("component", "retransmit"),
		sockets:     make(map[string]*net.UDPConn),
	}
}

// Open opens egress sockets for all interfaces.
func (r *Retransmitter) Open() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, iface := range r.ifaces {
		conn, err := r.openEgressSocket(iface)
		if err != nil {
			return fmt.Errorf("open egress socket on %s: %w", iface.Name, err)
		}
		r.sockets[iface.Name] = conn
		r.log.Info("egress socket opened", "iface", iface.Name)
	}
	return nil
}

// Close closes all egress sockets.
func (r *Retransmitter) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var lastErr error
	for name, conn := range r.sockets {
		if err := conn.Close(); err != nil {
			r.log.Warn("close egress socket error", "iface", name, "err", err)
			lastErr = err
		}
	}
	return lastErr
}

// Retransmit sends a cached frame to the multicast network.
func (r *Retransmitter) Retransmit(raw []byte, txID [32]byte) error {
	// Cross-instance deduplication via Redis SET NX.
	if r.redisCache != nil {
		dedupKey := r.buildDedupKey(raw)
		if len(dedupKey) > 0 {
			set, err := r.redisCache.SetNX(dedupKey, []byte("1"), r.dedupWindow)
			if err != nil {
				r.log.Error("dedup SET NX error", "err", err)
			}
			if !set {
				// Another instance already retransmitted this frame.
				if r.rec != nil {
					r.rec.RetransmitDedup()
				}
				if r.debug {
					r.log.Debug("retransmit dropped by dedup", "txid", fmt.Sprintf("%x", txID[:8]))
				}
				return nil
			}
		}
	}

	// Derive multicast group based on frame version:
	// - V4 (FrameVerV4): BRC-131 block control → CtrlGroupControl (0xFFFE)
	// - V5 (FrameVerV5): BRC-132 subtree data  → CtrlGroupSubtreeAnnounce (0xFFFB)
	// - All others:      shard group derived from TxID
	var groupAddr *net.UDPAddr
	if len(raw) >= 7 {
		switch raw[6] {
		case frame.FrameVerV4:
			ctrlIP := shard.ControlGroupAddr(r.engine.Prefix(), r.engine.GroupID(), shard.CtrlGroupControl)
			groupAddr = &net.UDPAddr{IP: ctrlIP, Port: r.egressPort}
		case frame.FrameVerV5:
			subtreeIP := shard.ControlGroupAddr(r.engine.Prefix(), r.engine.GroupID(), shard.CtrlGroupSubtreeAnnounce)
			groupAddr = &net.UDPAddr{IP: subtreeIP, Port: r.egressPort}
		case frame.FrameVerV6:
			ctrlIP := shard.ControlGroupAddr(r.engine.Prefix(), r.engine.GroupID(), shard.CtrlGroupControl)
			groupAddr = &net.UDPAddr{IP: ctrlIP, Port: r.egressPort}
		default:
			groupIdx := r.engine.GroupIndex(&txID)
			groupAddr = r.engine.Addr(groupIdx, r.egressPort)
		}
	} else {
		groupIdx := r.engine.GroupIndex(&txID)
		groupAddr = r.engine.Addr(groupIdx, r.egressPort)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Send to all egress interfaces.
	for name, conn := range r.sockets {
		if _, err := conn.WriteTo(raw, groupAddr); err != nil {
			r.log.Error("egress write error", "iface", name, "err", err)
			if r.rec != nil {
				r.rec.CacheError()
			}
			return err
		}
	}

	if r.debug {
		r.log.Debug("frame retransmitted",
			"txid", fmt.Sprintf("%x", txID[:8]),
			"group_addr", groupAddr.String(),
		)
	}

	return nil
}

// openEgressSocket opens a multicast egress socket on the given interface.
func (r *Retransmitter) openEgressSocket(iface *net.Interface) (*net.UDPConn, error) {
	conn, err := net.ListenPacket("udp6", "[::]:0")
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}
	udpConn, ok := conn.(*net.UDPConn)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("not a UDP connection")
	}

	// Set multicast interface.
	file, err := udpConn.File()
	if err != nil {
		_ = udpConn.Close()
		return nil, fmt.Errorf("get file descriptor: %w", err)
	}
	defer func() { _ = file.Close() }()

	fd := int(file.Fd())
	if err := unix.SetsockoptInt(fd, unix.IPPROTO_IPV6, unix.IPV6_MULTICAST_IF, iface.Index); err != nil {
		_ = udpConn.Close()
		return nil, fmt.Errorf("set multicast interface: %w", err)
	}

	return udpConn, nil
}

// buildDedupKey builds a deduplication key from the frame.
// Key: SeqNum (bytes 48–55), the monotonic per-flow counter for this frame.
func (r *Retransmitter) buildDedupKey(raw []byte) []byte {
	if len(raw) < 56 {
		return nil
	}
	key := make([]byte, 8)
	copy(key, raw[48:56]) // SeqNum
	return key
}

// Package retransmit handles retransmitting cached frames to the multicast network.
package retransmit

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/shard"

	"github.com/lightwebinc/retry-endpoint/cache"
	"github.com/lightwebinc/retry-endpoint/metrics"
)

// Retransmitter handles retransmitting cached frames.
type Retransmitter struct {
	engine      *shard.Engine
	beefEngine  *shard.PlaneEngine // nil = BRC-148 plane disabled (V9 retransmits error)
	ifaces      []*net.Interface
	egressPort  int
	dedupWindow time.Duration
	dedup       cache.Deduper // nil = no cross-instance dedup
	rec         *metrics.Recorder
	debug       bool
	log         *slog.Logger

	unicastSrc    net.IP       // optional source IP for the unicast retransmit socket
	unicastConn   *net.UDPConn // opened in Open(); used by RetransmitUnicast
	multicastLoop bool         // IPV6_MULTICAST_LOOP on egress sockets (collapsed/mesh node)

	mu      sync.Mutex
	sockets map[string]*net.UDPConn // iface name -> socket
}

// New constructs a Retransmitter.
func New(
	engine *shard.Engine,
	ifaces []*net.Interface,
	egressPort int,
	dedupWindow time.Duration,
	dedup cache.Deduper,
	rec *metrics.Recorder,
	debug bool,
) *Retransmitter {
	return &Retransmitter{
		engine:      engine,
		ifaces:      ifaces,
		egressPort:  egressPort,
		dedupWindow: dedupWindow,
		dedup:       dedup,
		rec:         rec,
		debug:       debug,
		log:         slog.Default().With("component", "retransmit"),
		sockets:     make(map[string]*net.UDPConn),
	}
}

// SetUnicastSource sets the source IP bound on the unicast retransmit socket.
// Set it to the advertised NACK address so the frame returned to a requester
// is sourced from the address its registry expects. Must be called before Open.
func (r *Retransmitter) SetUnicastSource(ip net.IP) { r.unicastSrc = ip }

// SetMulticastLoop enables IPV6_MULTICAST_LOOP on the egress socket(s). Required on a
// collapsed/mesh router node whose egress iface is a dummy (mc-local): the kernel only
// submits locally-originated multicast to the MFC for forwarding onto the fabric
// tunnels when loop is on (mirrors shard-proxy EGRESS_MULTICAST_LOOP). Call before Open.
func (r *Retransmitter) SetMulticastLoop(b bool) { r.multicastLoop = b }

// Open opens egress sockets for all interfaces plus the unicast retransmit socket.
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

	// Unicast retransmit socket (port 0; source-bound to unicastSrc when set).
	uc, err := net.ListenUDP("udp6", &net.UDPAddr{IP: r.unicastSrc})
	if err != nil {
		return fmt.Errorf("open unicast retransmit socket: %w", err)
	}
	r.unicastConn = uc
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
	if r.unicastConn != nil {
		if err := r.unicastConn.Close(); err != nil {
			r.log.Warn("close unicast retransmit socket error", "err", err)
			lastErr = err
		}
	}
	return lastErr
}

// RetransmitUnicast sends a cached frame verbatim back to a single requester
// (dst), the source address of an incoming NACK. Unlike the multicast path it
// does NOT apply cross-instance dedup — each requester needs its own copy, and
// the cross-domain NACK proxy depends on this as its only return channel. dst
// must be non-nil.
func (r *Retransmitter) RetransmitUnicast(raw []byte, dst *net.UDPAddr) error {
	if dst == nil {
		return fmt.Errorf("retransmit unicast: nil dst")
	}
	if r.unicastConn == nil {
		return fmt.Errorf("retransmit unicast: socket not open")
	}
	if _, err := r.unicastConn.WriteToUDP(raw, dst); err != nil {
		return err
	}
	if r.debug {
		r.log.Debug("frame retransmitted (unicast)", "dst", dst.String())
	}
	return nil
}

// SetBEEF wires the BRC-148 plane engine used to derive a cached BEEF
// frame's domain-tagged retransmit group from its TopicID. Call before Open.
func (r *Retransmitter) SetBEEF(pe *shard.PlaneEngine) { r.beefEngine = pe }


// targetGroup derives the retransmission destination for a cached frame,
// re-computed from the frame's own fields per FrameVer:
//   - V4/V6: GroupBlockBroadcast (0xFFFE)
//   - V5:    GroupSubtreeDataAnnounce (0xFFFB)
//   - V8:    the group index carried in the bundle header (offset 56)
//   - V9:    BRC-148 domain-tagged group from the TopicID at offset 56 at
//     the BEEF plane's width — never from offset 8 (the ContentID)
//   - else:  shard group derived from the TxID
func (r *Retransmitter) targetGroup(raw []byte, txID [32]byte) (*net.UDPAddr, error) {
	if len(raw) >= 7 {
		switch raw[6] {
		case frame.FrameVerV4, frame.FrameVerV6:
			ctrlIP := shard.GroupAddr(r.engine.Prefix(), r.engine.GroupID(), shard.GroupBlockBroadcast)
			return &net.UDPAddr{IP: ctrlIP, Port: r.egressPort}, nil
		case frame.FrameVerV5:
			subtreeIP := shard.GroupAddr(r.engine.Prefix(), r.engine.GroupID(), shard.GroupSubtreeDataAnnounce)
			return &net.UDPAddr{IP: subtreeIP, Port: r.egressPort}, nil
		case frame.FrameVerV9:
			if r.beefEngine == nil {
				return nil, fmt.Errorf("retransmit: BEEF frame cached but plane disabled")
			}
			if len(raw) < 88 {
				return nil, fmt.Errorf("retransmit: BEEF frame too short for TopicID")
			}
			var topicID [32]byte
			copy(topicID[:], raw[56:88])
			return r.engine.Addr(r.beefEngine.GroupIndex(&topicID), r.egressPort), nil
		case frame.FrameVerV8:
			// BRC-142 bundle: the group is carried in the header (offset 56),
			// not derived from a TxID (a bundle has none).
			if len(raw) >= 58 {
				groupIdx := uint32(binary.BigEndian.Uint16(raw[56:58]))
				return r.engine.Addr(groupIdx, r.egressPort), nil
			}
		}
	}
	groupIdx := r.engine.GroupIndex(&txID)
	return r.engine.Addr(groupIdx, r.egressPort), nil
}

// Retransmit sends a cached frame to the multicast network.
func (r *Retransmitter) Retransmit(raw []byte, txID [32]byte) error {
	// Cross-instance deduplication via backend SETNX.
	if r.dedup != nil {
		dedupKey := r.buildDedupKey(raw)
		if len(dedupKey) > 0 {
			set, err := r.dedup.SetNX(dedupKey, []byte("1"), r.dedupWindow)
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

	groupAddr, gerr := r.targetGroup(raw, txID)
	if gerr != nil {
		return gerr
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
	if r.multicastLoop {
		// Collapsed/mesh node: locally-originated multicast on a dummy egress iface is
		// only handed to the MFC (for forwarding onto the fabric tunnels) when loop is on.
		if err := unix.SetsockoptInt(fd, unix.IPPROTO_IPV6, unix.IPV6_MULTICAST_LOOP, 1); err != nil {
			_ = udpConn.Close()
			return nil, fmt.Errorf("set multicast loop: %w", err)
		}
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

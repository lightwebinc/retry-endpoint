// Package ingress implements the multicast receive worker for
// retry-endpoint.
//
// # Worker model
//
// Exactly one worker binds a UDP socket with SO_REUSEADDR on the configured
// port and joins all configured multicast groups on the configured interface.
// SO_REUSEADDR (not SO_REUSEPORT) is the cross-EUID co-bind path: it lets this
// socket co-exist with a co-resident shard-listener under a DIFFERENT user on a
// collapsed node — both receive the multicast group. Linux delivers multicast
// to ALL such sockets, so a second retry-endpoint worker would store each frame
// twice; hence exactly one worker.
//
// # Hot path per frame
//
//  1. Recvfrom (64 MiB receive buffer)
//  2. frame.Decode — extract HashKey, SeqNum
//  3. Drop if SeqNum == 0 (proxy has not stamped the frame)
//  4. Store: key = HashKey(8B) || SeqNum(8B) → raw frame
//
// The single 16-byte key (HashKey ∥ SeqNum) uniquely identifies every frame
// within a flow. The NACK server performs lookups using the same key.
package ingress

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"time"

	"golang.org/x/sys/unix"

	"github.com/lightwebinc/shard-common/bundle"
	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/netjoin"

	"github.com/lightwebinc/retry-endpoint/cache"
	"github.com/lightwebinc/retry-endpoint/metrics"
)

const (
	recvBufSize   = 4 * 1024 * 1024  // per-worker UDP receive buffer
	socketRecvBuf = 64 * 1024 * 1024 // 64 MiB
)

// TTLConfig configures per-FrameVer cache TTLs applied by the ingress
// worker. Each field corresponds to a frame type the retry endpoint may
// cache; values must be strictly positive.
type TTLConfig struct {
	Tx      time.Duration // FrameVer V2 (BRC-124/128 regular tx)
	Block   time.Duration // FrameVer V4 (BRC-131 block control)
	Subtree time.Duration // FrameVer V5 (BRC-132 subtree data)
	Anchor  time.Duration // FrameVer V6 (BRC-134 anchor tx)
	BEEF    time.Duration // FrameVer V9 (BRC-148 BEEF object)
}

// GroupSources maps a multicast group address (16-byte IPv6 in dotted
// string form per netip.Addr) to its SSM source list. Groups not present
// in the map (or whose source list is empty) are joined ASM-style with
// IPV6_JOIN_GROUP. Each control group's source list is the matching
// sources.bootstrap.<group> bucket; the data-plane source list is the
// manifest-derived publisher union.
type GroupSources map[netip.Addr][]netip.Addr

// Worker is the single multicast receive goroutine.
type Worker struct {
	iface   *net.Interface
	port    int
	groups  []*net.UDPAddr
	sources GroupSources // optional; non-nil entries trigger SSM joins
	cache   cache.Cache
	rec     *metrics.Recorder
	ttls    TTLConfig
	debug   bool
	log     *slog.Logger
}

// New constructs a Worker.
func New(
	iface *net.Interface,
	port int,
	groups []*net.UDPAddr,
	cache cache.Cache,
	rec *metrics.Recorder,
	ttls TTLConfig,
	debug bool,
) *Worker {
	return &Worker{
		iface:  iface,
		port:   port,
		groups: groups,
		cache:  cache,
		rec:    rec,
		ttls:   ttls,
		debug:  debug,
		log:    slog.Default().With("component", "ingress"),
	}
}

// SetGroupSources configures per-group SSM source lists. Must be called
// before [Worker.Run]. Groups absent from src (or with empty source
// lists) are joined ASM-style.
func (w *Worker) SetGroupSources(src GroupSources) {
	w.sources = src
}

// Run opens a SO_REUSEADDR socket, joins all multicast groups, and processes
// frames until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) error {
	fd, err := w.openRawSocket()
	if err != nil {
		return fmt.Errorf("ingress: open socket: %w", err)
	}

	for _, grp := range w.groups {
		ga, ok := netip.AddrFromSlice(grp.IP.To16())
		if !ok {
			_ = unix.Close(fd)
			return fmt.Errorf("ingress: bad group address %s", grp.IP)
		}
		// SSM sources for this group, if any. ASM join when nil/empty.
		var srcs []netip.Addr
		if w.sources != nil {
			srcs = w.sources[ga]
		}
		if err := netjoin.Join(fd, w.iface.Index, ga, srcs); err != nil {
			_ = unix.Close(fd)
			return fmt.Errorf("ingress: join group %s (%d sources): %w", grp.IP, len(srcs), err)
		}
	}

	if w.rec != nil {
		w.rec.WorkerReady()
		defer w.rec.WorkerDone()
	}

	w.log.Info("ingress worker ready", "iface", w.iface.Name, "port", w.port, "groups", len(w.groups))

	tv := unix.NsecToTimeval((200 * time.Millisecond).Nanoseconds())
	_ = unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &tv)

	go func() {
		<-ctx.Done()
		_ = unix.Close(fd)
	}()

	buf := make([]byte, recvBufSize)
	for {
		n, from, err := unix.Recvfrom(fd, buf, 0)
		if err != nil {
			if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
				if ctx.Err() != nil {
					return nil
				}
				continue
			}
			if err == unix.EBADF || err == unix.EINVAL {
				return nil
			}
			if err == unix.EINTR {
				continue
			}
			if ctx.Err() != nil {
				return nil
			}
			w.log.Error("recvfrom error", "err", err)
			continue
		}
		if n > 0 {
			// The datagram's source address IS the fabric source of the frame
			// (SSM: one (S,G) per publisher) — it labels the per-source cache
			// counters so a retry starved of ONE source's frames is a zero
			// rate, not an invisible absence.
			src := ""
			if sa, ok := from.(*unix.SockaddrInet6); ok {
				src = netip.AddrFrom16(sa.Addr).String()
			}
			w.processFrame(buf[:n], src)
		}
	}
}

func (w *Worker) processFrame(raw []byte, src string) {
	// BRC-131 block control frames (FrameVer 0x04) are handled separately
	// because frame.Decode rejects V4 with ErrBadVer.
	if frame.IsBlockFrame(raw) {
		w.processBlockFrame(raw, src)
		return
	}

	// BRC-132 subtree data frames (FrameVer 0x05) are handled separately
	// because frame.Decode rejects V5 with ErrBadVer.
	if frame.IsSubtreeDataFrame(raw) {
		w.processSubtreeDataFrame(raw, src)
		return
	}

	// BRC-134 anchor transaction frames (FrameVer 0x06) are handled
	// separately because frame.Decode rejects V6 with ErrBadVer.
	if frame.IsAnchorFrame(raw) {
		w.processAnchorFrame(raw, src)
		return
	}

	// BRC-142 bundle frames (FrameVer 0x08) are handled separately because
	// frame.Decode rejects V8 with ErrBadVer. A bundle is cached opaquely by
	// its (HashKey, SeqNum) flow key, exactly like a BRC-124 frame.
	if frame.IsBundle(raw) {
		w.processBundleFrame(raw, src)
		return
	}

	// BRC-148 BEEF object frames (FrameVer 0x09) are handled separately
	// because frame.Decode rejects V9 with ErrBadVer. Like every cached
	// class, the flow key is (HashKey, SeqNum) at fixed offsets.
	if frame.IsBEEFFrame(raw) {
		w.processBEEFFrame(raw, src)
		return
	}

	// BRC-130 fragments (FrameVer 0x03) are cached individually: each
	// fragment carries its own HashKey/SeqNum at the standard offsets, so
	// per-fragment NACK recovery works exactly like whole frames. The TTL
	// follows the original frame class (OrigFrameVer).
	if frame.IsFragment(raw) {
		w.processFragmentFrame(raw, src)
		return
	}

	f, err := frame.Decode(raw)
	if err != nil {
		if w.rec != nil {
			w.rec.FrameDropped("decode_error")
		}
		if w.debug {
			w.log.Debug("decode error", "err", err, "len", len(raw))
		}
		return
	}

	if w.rec != nil {
		w.rec.FrameReceived()
	}

	if f.SeqNum == 0 {
		return // proxy has not stamped this frame
	}

	// Single index: HashKey (8B) || SeqNum (8B) → raw frame
	var key [16]byte
	binary.BigEndian.PutUint64(key[0:8], f.HashKey)
	binary.BigEndian.PutUint64(key[8:16], f.SeqNum)
	if err := w.cache.Store(key[:], raw, w.ttls.Tx); err != nil {
		if w.rec != nil {
			w.rec.CacheError()
		}
		w.log.Error("cache store error", "err", err)
		return
	}

	if w.rec != nil {
		w.rec.FrameCached(src)
	}

	if w.debug {
		w.log.Debug("frame cached",
			"txid", fmt.Sprintf("%x", f.TxID[:8]),
			"hash_key", f.HashKey,
			"seq_num", f.SeqNum,
		)
	}
}

// processBEEFFrame handles BRC-148 BEEF object frames (FrameVer 0x09),
// cached by their (HashKey, SeqNum) flow key with the BEEF TTL. The
// retransmit path re-derives the domain-tagged group from the frame's
// TopicID at offset 56 — never from offset 8, which carries the ContentID.
func (w *Worker) processBEEFFrame(raw []byte, src string) {
	bf, err := frame.DecodeBEEF(raw)
	if err != nil {
		if w.rec != nil {
			w.rec.FrameDropped("decode_error")
		}
		if w.debug {
			w.log.Debug("beef decode error", "err", err, "len", len(raw))
		}
		return
	}
	if w.rec != nil {
		w.rec.FrameReceived()
	}
	if bf.SeqNum == 0 {
		return // not stamped at ingress
	}
	var key [16]byte
	binary.BigEndian.PutUint64(key[0:8], bf.HashKey)
	binary.BigEndian.PutUint64(key[8:16], bf.SeqNum)
	if err := w.cache.Store(key[:], raw, w.ttls.BEEF); err != nil {
		if w.rec != nil {
			w.rec.CacheError()
		}
		w.log.Error("cache store error", "err", err)
		return
	}
	if w.rec != nil {
		w.rec.FrameCached(src)
	}
}

// processFragmentFrame caches one BRC-130 fragment under its
// (HashKey, SeqNum) flow key. The cache TTL follows the fragmented class
// (OrigFrameVer); the retransmit path re-derives the group from the
// fragment's own header fields per that class.
func (w *Worker) processFragmentFrame(raw []byte, src string) {
	ff, err := frame.DecodeFragment(raw)
	if err != nil {
		if w.rec != nil {
			w.rec.FrameDropped("decode_error")
		}
		if w.debug {
			w.log.Debug("fragment decode error", "err", err, "len", len(raw))
		}
		return
	}
	if w.rec != nil {
		w.rec.FrameReceived()
	}
	if ff.SeqNum == 0 {
		return // not stamped at ingress
	}
	ttl := w.ttls.Tx
	switch ff.OrigFrameVer {
	case frame.FrameVerV4:
		ttl = w.ttls.Block
	case frame.FrameVerV5:
		ttl = w.ttls.Subtree
	case frame.FrameVerV6:
		ttl = w.ttls.Anchor
	case frame.FrameVerV9:
		ttl = w.ttls.BEEF
	}
	var key [16]byte
	binary.BigEndian.PutUint64(key[0:8], ff.HashKey)
	binary.BigEndian.PutUint64(key[8:16], ff.SeqNum)
	if err := w.cache.Store(key[:], raw, ttl); err != nil {
		if w.rec != nil {
			w.rec.CacheError()
		}
		w.log.Error("cache store error", "err", err)
		return
	}
	if w.rec != nil {
		w.rec.FrameCached(src)
	}
}

// processBundleFrame handles BRC-142 bundle frames (FrameVer 0x08). A bundle is
// cached opaquely by its (HashKey, SeqNum) flow key — identical to a BRC-124
// frame — and retransmitted whole on NACK; the retry endpoint never parses
// members.
func (w *Worker) processBundleFrame(raw []byte, src string) {
	b, err := bundle.Decode(raw)
	if err != nil {
		if w.rec != nil {
			w.rec.FrameDropped("decode_error")
		}
		if w.debug {
			w.log.Debug("bundle decode error", "err", err, "len", len(raw))
		}
		return
	}

	if w.rec != nil {
		w.rec.FrameReceived()
	}

	if b.SeqNum == 0 {
		return // proxy has not stamped this bundle
	}

	var key [16]byte
	binary.BigEndian.PutUint64(key[0:8], b.HashKey)
	binary.BigEndian.PutUint64(key[8:16], b.SeqNum)
	if err := w.cache.Store(key[:], raw, w.ttls.Tx); err != nil {
		if w.rec != nil {
			w.rec.CacheError()
		}
		w.log.Error("cache store error", "err", err)
		return
	}

	if w.rec != nil {
		w.rec.FrameCached(src)
	}

	if w.debug {
		w.log.Debug("bundle cached",
			"hash_key", b.HashKey,
			"seq_num", b.SeqNum,
			"members", len(b.Members),
		)
	}
}

// processBlockFrame handles BRC-131 block control frames (FrameVer 0x04).
// Uses the same HashKey ∥ SeqNum cache key as regular frames.
func (w *Worker) processBlockFrame(raw []byte, src string) {
	bf, err := frame.DecodeBlock(raw)
	if err != nil {
		if w.rec != nil {
			w.rec.FrameDropped("decode_error")
		}
		if w.debug {
			w.log.Debug("block frame decode error", "err", err, "len", len(raw))
		}
		return
	}

	if w.rec != nil {
		w.rec.FrameReceived()
	}

	if bf.SeqNum == 0 {
		return // proxy has not stamped this frame
	}

	var key [16]byte
	binary.BigEndian.PutUint64(key[0:8], bf.HashKey)
	binary.BigEndian.PutUint64(key[8:16], bf.SeqNum)
	if err := w.cache.Store(key[:], raw, w.ttls.Block); err != nil {
		if w.rec != nil {
			w.rec.CacheError()
		}
		w.log.Error("cache store error", "err", err)
		return
	}

	if w.rec != nil {
		w.rec.FrameCached(src)
	}

	if w.debug {
		w.log.Debug("block frame cached",
			"content_id", fmt.Sprintf("%x", bf.ContentID[:8]),
			"msg_type", bf.MsgType,
			"hash_key", bf.HashKey,
			"seq_num", bf.SeqNum,
		)
	}
}

// processSubtreeDataFrame handles BRC-132 subtree data frames (FrameVer 0x05).
// Uses the same HashKey ∥ SeqNum cache key as regular and block frames.
func (w *Worker) processSubtreeDataFrame(raw []byte, src string) {
	sf, err := frame.DecodeSubtreeData(raw)
	if err != nil {
		if w.rec != nil {
			w.rec.FrameDropped("decode_error")
		}
		if w.debug {
			w.log.Debug("subtree data frame decode error", "err", err, "len", len(raw))
		}
		return
	}

	if w.rec != nil {
		w.rec.FrameReceived()
	}

	if sf.SeqNum == 0 {
		return // proxy has not stamped this frame
	}

	var key [16]byte
	binary.BigEndian.PutUint64(key[0:8], sf.HashKey)
	binary.BigEndian.PutUint64(key[8:16], sf.SeqNum)
	if err := w.cache.Store(key[:], raw, w.ttls.Subtree); err != nil {
		if w.rec != nil {
			w.rec.CacheError()
		}
		w.log.Error("cache store error", "err", err)
		return
	}

	if w.rec != nil {
		w.rec.FrameCached(src)
	}

	if w.debug {
		w.log.Debug("subtree data frame cached",
			"subtree_id", fmt.Sprintf("%x", sf.SubtreeID[:8]),
			"msg_type", sf.MsgType,
			"hash_key", sf.HashKey,
			"seq_num", sf.SeqNum,
		)
	}
}

// processAnchorFrame handles BRC-134 anchor transaction frames (FrameVer 0x06).
// Uses the same HashKey ∥ SeqNum cache key as other frame types.
func (w *Worker) processAnchorFrame(raw []byte, src string) {
	af, err := frame.DecodeAnchor(raw)
	if err != nil {
		if w.rec != nil {
			w.rec.FrameDropped("decode_error")
		}
		if w.debug {
			w.log.Debug("anchor frame decode error", "err", err, "len", len(raw))
		}
		return
	}

	if w.rec != nil {
		w.rec.FrameReceived()
	}

	if af.SeqNum == 0 {
		return // proxy has not stamped this frame
	}

	var key [16]byte
	binary.BigEndian.PutUint64(key[0:8], af.HashKey)
	binary.BigEndian.PutUint64(key[8:16], af.SeqNum)
	if err := w.cache.Store(key[:], raw, w.ttls.Anchor); err != nil {
		if w.rec != nil {
			w.rec.CacheError()
		}
		w.log.Error("cache store error", "err", err)
		return
	}

	if w.rec != nil {
		w.rec.FrameCached(src)
	}

	if w.debug {
		w.log.Debug("anchor frame cached",
			"txid", fmt.Sprintf("%x", af.TxID[:8]),
			"hash_key", af.HashKey,
			"seq_num", af.SeqNum,
		)
	}
}

func (w *Worker) openRawSocket() (int, error) {
	fd, err := unix.Socket(unix.AF_INET6, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return -1, fmt.Errorf("socket: %w", err)
	}
	// The ingress is single-worker, so it does NOT need SO_REUSEPORT (which would only matter
	// for same-EUID load-balancing). It uses SO_REUSEADDR alone: that is the cross-EUID
	// bind path, so this socket can co-exist with a co-resident shard-listener (a DIFFERENT
	// user, which keeps SO_REUSEPORT for its own workers) on a collapsed node — both receive
	// the multicast group. NB: declaring SO_REUSEPORT here would force the same-EUID check and
	// defeat the SO_REUSEADDR cross-user share.
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
		_ = unix.Close(fd)
		return -1, fmt.Errorf("SO_REUSEADDR: %w", err)
	}
	_ = unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_RCVBUF, socketRecvBuf)
	// Scope this socket to the groups it actually joined. Linux-only knob; on other
	// platforms the default behaviour already matches. See mcastall_linux.go.
	disableMulticastAll(fd)
	sa := &unix.SockaddrInet6{Port: w.port}
	if err := unix.Bind(fd, sa); err != nil {
		_ = unix.Close(fd)
		return -1, fmt.Errorf("bind [::]::%d: %w", w.port, err)
	}
	return fd, nil
}

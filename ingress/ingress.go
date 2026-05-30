// Package ingress implements the multicast receive worker for
// retry-endpoint.
//
// # Worker model
//
// Exactly one worker binds a UDP socket with SO_REUSEPORT on the configured
// port and joins all configured multicast groups on the configured interface.
// This is critical: Linux delivers multicast to ALL sockets in a reuseport
// group, so multiple workers would store each frame multiple times.
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

// Run opens a SO_REUSEPORT socket, joins all multicast groups, and processes
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
		n, _, err := unix.Recvfrom(fd, buf, 0)
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
			w.processFrame(buf[:n])
		}
	}
}

func (w *Worker) processFrame(raw []byte) {
	// BRC-131 block control frames (FrameVer 0x04) are handled separately
	// because frame.Decode rejects V4 with ErrBadVer.
	if frame.IsBlockFrame(raw) {
		w.processBlockFrame(raw)
		return
	}

	// BRC-132 subtree data frames (FrameVer 0x05) are handled separately
	// because frame.Decode rejects V5 with ErrBadVer.
	if frame.IsSubtreeDataFrame(raw) {
		w.processSubtreeDataFrame(raw)
		return
	}

	// BRC-134 anchor transaction frames (FrameVer 0x06) are handled
	// separately because frame.Decode rejects V6 with ErrBadVer.
	if frame.IsAnchorFrame(raw) {
		w.processAnchorFrame(raw)
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
		w.rec.FrameCached()
	}

	if w.debug {
		w.log.Debug("frame cached",
			"txid", fmt.Sprintf("%x", f.TxID[:8]),
			"hash_key", f.HashKey,
			"seq_num", f.SeqNum,
		)
	}
}

// processBlockFrame handles BRC-131 block control frames (FrameVer 0x04).
// Uses the same HashKey ∥ SeqNum cache key as regular frames.
func (w *Worker) processBlockFrame(raw []byte) {
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
		w.rec.FrameCached()
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
func (w *Worker) processSubtreeDataFrame(raw []byte) {
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
		w.rec.FrameCached()
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
func (w *Worker) processAnchorFrame(raw []byte) {
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
		w.rec.FrameCached()
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
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEPORT, 1); err != nil {
		_ = unix.Close(fd)
		return -1, fmt.Errorf("SO_REUSEPORT: %w", err)
	}
	_ = unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_RCVBUF, socketRecvBuf)
	sa := &unix.SockaddrInet6{Port: w.port}
	if err := unix.Bind(fd, sa); err != nil {
		_ = unix.Close(fd)
		return -1, fmt.Errorf("bind [::]::%d: %w", w.port, err)
	}
	return fd, nil
}

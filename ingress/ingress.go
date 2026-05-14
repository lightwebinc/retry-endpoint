// Package ingress implements the multicast receive worker for
// bitcoin-retry-endpoint.
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
//  2. frame.Decode — extract PrevSeq, CurSeq
//  3. Drop if CurSeq == 0 (proxy has not stamped the frame)
//  4. Store primary index:   key = 0x01 || CurSeq  (8+1 = 9 bytes) → raw frame
//  5. Store secondary index: key = 0x00 || PrevSeq (8+1 = 9 bytes) → CurSeq (8 bytes)
//
// The dual index lets the retry endpoint serve both LookupByCurSeq and
// LookupByPrevSeq NACK requests without scanning all cached frames.
package ingress

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"time"

	"golang.org/x/sys/unix"

	"github.com/lightwebinc/bitcoin-shard-common/frame"

	"github.com/lightwebinc/bitcoin-retry-endpoint/cache"
	"github.com/lightwebinc/bitcoin-retry-endpoint/metrics"
)

const (
	recvBufSize   = 4 * 1024 * 1024  // per-worker UDP receive buffer
	socketRecvBuf = 64 * 1024 * 1024 // 64 MiB
)

// Worker is the single multicast receive goroutine.
type Worker struct {
	iface  *net.Interface
	port   int
	groups []*net.UDPAddr
	cache  cache.Cache
	rec    *metrics.Recorder
	ttl    time.Duration
	debug  bool
	log    *slog.Logger
}

// New constructs a Worker.
func New(
	iface *net.Interface,
	port int,
	groups []*net.UDPAddr,
	cache cache.Cache,
	rec *metrics.Recorder,
	ttl time.Duration,
	debug bool,
) *Worker {
	return &Worker{
		iface:  iface,
		port:   port,
		groups: groups,
		cache:  cache,
		rec:    rec,
		ttl:    ttl,
		debug:  debug,
		log:    slog.Default().With("component", "ingress"),
	}
}

// Run opens a SO_REUSEPORT socket, joins all multicast groups, and processes
// frames until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) error {
	fd, err := w.openRawSocket()
	if err != nil {
		return fmt.Errorf("ingress: open socket: %w", err)
	}

	for _, grp := range w.groups {
		mreq := &unix.IPv6Mreq{Interface: uint32(w.iface.Index)}
		copy(mreq.Multiaddr[:], grp.IP.To16())
		if err := unix.SetsockoptIPv6Mreq(fd, unix.IPPROTO_IPV6, unix.IPV6_JOIN_GROUP, mreq); err != nil {
			_ = unix.Close(fd)
			return fmt.Errorf("ingress: join group %s: %w", grp.IP, err)
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

	if f.CurSeq == 0 {
		return // proxy has not stamped this frame
	}

	// Primary index: 0x01 || SubtreeID || CurSeq → raw frame (LookupByCurSeq)
	var primaryKey [41]byte
	primaryKey[0] = 0x01
	copy(primaryKey[1:33], f.SubtreeID[:])
	binary.BigEndian.PutUint64(primaryKey[33:41], f.CurSeq)
	if err := w.cache.Store(primaryKey[:], raw, w.ttl); err != nil {
		if w.rec != nil {
			w.rec.CacheError()
		}
		w.log.Error("cache store error (primary)", "err", err)
		return
	}

	// Secondary index: 0x00 || SubtreeID || PrevSeq → CurSeq pointer (LookupByPrevSeq)
	if f.PrevSeq != 0 {
		var secondaryKey [41]byte
		secondaryKey[0] = 0x00
		copy(secondaryKey[1:33], f.SubtreeID[:])
		binary.BigEndian.PutUint64(secondaryKey[33:41], f.PrevSeq)
		var curSeqVal [8]byte
		binary.BigEndian.PutUint64(curSeqVal[:], f.CurSeq)
		if err := w.cache.Store(secondaryKey[:], curSeqVal[:], w.ttl); err != nil {
			w.log.Error("cache store error (secondary)", "err", err)
		}
	}

	if w.rec != nil {
		w.rec.FrameCached()
	}

	if w.debug {
		w.log.Debug("frame cached",
			"txid", fmt.Sprintf("%x", f.TxID[:8]),
			"prev_seq", f.PrevSeq,
			"cur_seq", f.CurSeq,
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

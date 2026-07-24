// Package server implements the UDP NACK receiver for retry-endpoint.
package server

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/shard"

	"github.com/lightwebinc/retry-endpoint/cache"
	"github.com/lightwebinc/retry-endpoint/metrics"
	"github.com/lightwebinc/retry-endpoint/proxy"
	"github.com/lightwebinc/retry-endpoint/ratelimit"
)

// NACKSize is the fixed size of a BRC-126 NACK datagram (64 bytes).
const NACKSize = 64

// ResponseSize is the fixed size of a BRC-126 ACK or MISS response (16 bytes).
const ResponseSize = 16

// MsgType constants for BRC-126 protocol messages.
const (
	msgTypeNACK      byte = 0x10
	msgTypeMISS      byte = 0x11
	msgTypeACK       byte = 0x12
	msgTypeThrottled byte = 0x13 // congestion signal: back off this gap, do not escalate
)

// Throttle backoff hint buckets, encoded in the low nibble of the THROTTLED
// response Flags byte. The listener computes a suggested hold of
// ThrottleHintBase << bucket (see BRC-126). Sequence-level throttling implies a
// retransmit for this exact gap was just served (and is propagating via
// multicast), so the hold is short; chain-level throttling means the source is
// NACKing a whole flow too hard, so it backs off a little longer; group-level
// throttling means the retransmit budget for a whole shard group is exhausted
// (the broadest scope), so it backs off longest.
const (
	throttleBucketSequence byte = 2 // ~500ms at the 125ms base
	throttleBucketChain    byte = 3 // ~1s
	throttleBucketGroup    byte = 4 // ~2s
)

// ACK response Flags bits (mirrored in shard-listener/nack/wire.go). Each bit
// is set only when the corresponding retransmit path actually dispatched.
const (
	ackFlagMulticastSent byte = 0x01
	ackFlagUnicastSent   byte = 0x02
)

// Server receives NACK requests and coordinates retransmissions.
type Server struct {
	port         int
	bindAddr     string // specific IPv6 address to bind; empty = [::]
	cache        cache.Cache
	rateLimiter  *ratelimit.Limiter
	rec          *metrics.Recorder
	retransmit   Retransmitter
	workers      int
	debug        bool
	suppressACK  bool          // if true, do not send ACK responses
	suppressMISS bool          // if true, do not send MISS responses
	throttleResp bool          // if true, answer honest-congestion throttles with a THROTTLED hint
	shardEngine  *shard.Engine // for post-lookup group index derivation; nil = skip group limiter
	beefEngine  *shard.PlaneEngine // nil = derive BEEF rate-limit group from offset 8 (stable fallback)
	retransmitMC bool          // multicast retransmit on cache hit (default true)
	retransmitUC bool          // unicast retransmit (frame back to requester) on cache hit
	proxy        ProxyEnqueuer // nil = NACK proxying disabled
	log          *slog.Logger
}

// Retransmitter is the interface for retransmitting cached frames.
type Retransmitter interface {
	Retransmit(raw []byte, txID [32]byte) error
	RetransmitUnicast(raw []byte, dst *net.UDPAddr) error
}

// ProxyEnqueuer submits a cross-domain recovery job on a cache miss.
// *proxy.Client satisfies it.
type ProxyEnqueuer interface {
	Enqueue(hashKey, seq uint64, subtree [32]byte) bool
}

// New constructs a Server.
func New(
	port int,
	cache cache.Cache,
	rateLimiter *ratelimit.Limiter,
	rec *metrics.Recorder,
	retransmit Retransmitter,
	workers int,
	debug bool,
) *Server {
	return &Server{
		port:         port,
		cache:        cache,
		rateLimiter:  rateLimiter,
		rec:          rec,
		retransmit:   retransmit,
		workers:      workers,
		debug:        debug,
		retransmitMC: true, // preserve default behaviour until SetRetransmitModes is called
		log:          slog.Default().With("component", "server"),
	}
}

// SetRetransmitModes selects which retransmit paths fire on a cache hit. These
// mirror the advertised beacon flags. Multicast defaults to true; unicast to
// false. A proxied NACK is always served a unicast copy regardless of uc.
func (s *Server) SetRetransmitModes(multicast, unicast bool) {
	s.retransmitMC = multicast
	s.retransmitUC = unicast
}

// SetProxy enables cross-domain NACK proxying: on a local cache miss the server
// enqueues a recovery job. nil (the default) disables proxying.
func (s *Server) SetProxy(p ProxyEnqueuer) { s.proxy = p }

// SetSuppressACK disables ACK responses (for high-volume deployments).
func (s *Server) SetSuppressACK(v bool) { s.suppressACK = v }

// SetSuppressMISS disables MISS responses.
func (s *Server) SetSuppressMISS(v bool) { s.suppressMISS = v }

// SetThrottleResponse enables THROTTLED replies on honest-congestion throttles
// (sequence, chain, and group tiers). The IP flood tier stays silent
// regardless, so a spoofed-source flood is never answered. Off by default.
func (s *Server) SetThrottleResponse(v bool) { s.throttleResp = v }

// SetBindAddr sets the specific IPv6 address the NACK socket binds to.
// When set, ACK/MISS responses are sourced from this address, avoiding
// kernel source-address selection (which may pick a SLAAC-derived address
// that does not match what listeners expect).
func (s *Server) SetBindAddr(addr string) { s.bindAddr = addr }

// SetBEEFEngine wires the BRC-148 plane engine used to derive a BEEF
// frame's rate-limit group from its TopicID. Optional; without it BEEF
// frames rate-limit under the (meaningless but stable) ContentID-derived
// group.
func (s *Server) SetBEEFEngine(pe *shard.PlaneEngine) { s.beefEngine = pe }

// SetShardEngine wires the shard engine used to derive groupIdx from TxID
// for the post-lookup group rate limiter. Must be called before Run.
func (s *Server) SetShardEngine(e *shard.Engine) { s.shardEngine = e }

// Run starts the UDP server with a worker pool.
func (s *Server) Run(ctx context.Context) error {
	host := "::"
	if s.bindAddr != "" {
		host = s.bindAddr
	}
	// SO_REUSEADDR so the NACK port can be shared with a co-resident shard-listener's beacon
	// on the same port (collapsed node, different EUID) — same rationale as the ingress socket.
	lc := net.ListenConfig{Control: func(_, _ string, c syscall.RawConn) error {
		var cerr error
		if err := c.Control(func(fd uintptr) {
			cerr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
		}); err != nil {
			return err
		}
		return cerr
	}}
	conn, err := lc.ListenPacket(ctx, "udp6", fmt.Sprintf("[%s]:%d", host, s.port))
	if err != nil {
		return fmt.Errorf("server: listen: %w", err)
	}
	defer func() { _ = conn.Close() }()

	s.log.Info("NACK server listening", "port", s.port, "workers", s.workers)

	if s.rec != nil {
		s.rec.WorkerReady()
		defer s.rec.WorkerDone()
	}

	type nackRequest struct {
		data []byte
		src  *net.UDPAddr // full source address for response sending
	}

	// Worker pool for parallel request handling.
	requests := make(chan nackRequest, 100)
	var wg sync.WaitGroup

	// Start workers.
	for i := 0; i < s.workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case req, ok := <-requests:
					if !ok {
						return
					}
					s.processNACK(conn, workerID, req.data, req.src)
				}
			}
		}(i)
	}

	buf := make([]byte, NACKSize)
	for {
		select {
		case <-ctx.Done():
			close(requests)
			wg.Wait()
			return nil
		default:
			_ = conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			n, src, err := conn.ReadFrom(buf)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				if ctx.Err() != nil {
					close(requests)
					wg.Wait()
					return nil
				}
				s.log.Error("read error", "err", err, "src", src)
				continue
			}
			if n != NACKSize {
				s.log.Warn("invalid NACK size", "len", n, "src", src)
				continue
			}

			// Extract source UDPAddr.
			var srcAddr *net.UDPAddr
			if udpAddr, ok := src.(*net.UDPAddr); ok {
				srcAddr = udpAddr
			}

			// Copy the datagram for the worker.
			datagram := make([]byte, NACKSize)
			copy(datagram, buf[:n])

			select {
			case requests <- nackRequest{data: datagram, src: srcAddr}:
			case <-ctx.Done():
				close(requests)
				wg.Wait()
				return nil
			}
		}
	}
}

func (s *Server) processNACK(conn net.PacketConn, workerID int, datagram []byte, src *net.UDPAddr) {
	if s.rec != nil {
		s.rec.NACKRequest()
	}

	// Validate 64-byte NACK format.
	if err := validateNACK(datagram); err != nil {
		s.log.Debug("invalid NACK", "err", err)
		return
	}

	// datagram[7] = Flags. FlagProxied marks a NACK already recovered on behalf
	// of a downstream domain; such requests are served locally but never
	// re-proxied (bounds the chain to one hop, BRC-126).
	proxied := datagram[7]&proxy.FlagProxied != 0
	hashKey := binary.BigEndian.Uint64(datagram[8:16])
	startSeq := binary.BigEndian.Uint64(datagram[16:24])
	endSeq := binary.BigEndian.Uint64(datagram[24:32])
	var subtreeID [32]byte
	copy(subtreeID[:], datagram[32:64])

	// For now only single-frame retrieval is implemented (StartSeq == EndSeq).
	// Range requests (StartSeq < EndSeq) are reserved for future use.
	if endSeq != startSeq {
		s.log.Debug("range NACK not supported", "start_seq", startSeq, "end_seq", endSeq)
		return
	}

	// Rate limiting: tier 1 (IP) + tier 3 (sequence), pre-lookup.
	var srcIP net.IP
	if src != nil {
		srcIP = src.IP
	} else {
		srcIP = net.IPv6unspecified
	}
	allowed, level := s.rateLimiter.Allow(srcIP, startSeq)
	if !allowed {
		if s.rec != nil {
			s.rec.RateLimitDrop(string(level))
		}
		if s.debug {
			s.log.Debug("rate limited", "level", level)
		}
		// Honest-congestion signal for the sequence tier: a retransmit for this
		// exact gap was just served and is propagating via multicast, so tell the
		// listener to hold rather than time out and escalate. The IP tier is the
		// flood/abuse tier — stay silent there (never answer a possible spoof).
		if level == ratelimit.LevelSequence && s.throttleResp && src != nil {
			s.sendThrottled(conn, src, throttleBucketSequence, startSeq)
		}
		return
	}

	// Rate limiting: tier 2 (flow/hashKey), pre-lookup.
	if !s.rateLimiter.AllowChain(srcIP, hashKey) {
		if s.rec != nil {
			s.rec.RateLimitDrop(string(ratelimit.LevelChain))
		}
		if s.debug {
			s.log.Debug("rate limited", "level", ratelimit.LevelChain, "hash_key", hashKey)
		}
		if s.throttleResp && src != nil {
			s.sendThrottled(conn, src, throttleBucketChain, startSeq)
		}
		return
	}

	// Retrieve raw frame from cache using single 16-byte key: HashKey || SeqNum.
	var key [16]byte
	binary.BigEndian.PutUint64(key[0:8], hashKey)
	binary.BigEndian.PutUint64(key[8:16], startSeq)
	raw, err := s.cache.Retrieve(key[:])
	if err != nil {
		if s.rec != nil {
			s.rec.CacheError()
		}
		s.log.Error("cache retrieve error", "err", err)
		return
	}
	seqNum := startSeq

	if raw == nil {
		if s.rec != nil {
			s.rec.CacheMiss()
		}
		if s.debug {
			s.log.Debug("cache miss", "hash_key", hashKey, "start_seq", startSeq)
		}
		if !s.suppressMISS && src != nil {
			s.sendResponse(conn, src, msgTypeMISS, 0, 0)
		}
		// Cross-domain recovery: enqueue an async upstream fetch (cache-warm).
		// Never re-proxy an already-proxied request (one-hop bound).
		if s.proxy != nil && !proxied {
			s.proxy.Enqueue(hashKey, startSeq, subtreeID)
		}
		return
	}

	if s.rec != nil {
		s.rec.CacheHit()
	}

	// Extract TxID from the raw frame header (bytes 8..39).
	var txID [32]byte
	if len(raw) >= 40 {
		copy(txID[:], raw[8:40])
	}

	// Rate limiting: tier 4 (group), post-lookup. Like the other honest-
	// congestion tiers: THROTTLED (group bucket) when enabled, silence
	// otherwise — never ACK, which would cancel the listener gap with no
	// retransmit dispatched (silent loss).
	if s.shardEngine != nil {
		groupIdx := s.shardEngine.GroupIndex(&txID)
		// A BRC-142 bundle has no single TxID; its group is in the header
		// (offset 56), so derive the rate-limit group from there.
		if frame.IsBundle(raw) && len(raw) >= 58 {
			groupIdx = uint32(binary.BigEndian.Uint16(raw[56:58]))
		}
		// A BRC-148 BEEF frame's offset-8 field is the ContentID, not a
		// shard key; its rate-limit group derives from the TopicID at
		// offset 56 (domain-tagged when the plane engine is wired).
		if frame.IsBEEFFrame(raw) && len(raw) >= 88 && s.beefEngine != nil {
			var topicID [32]byte
			copy(topicID[:], raw[56:88])
			groupIdx = s.beefEngine.GroupIndex(&topicID)
		}
		if !s.rateLimiter.AllowGroup(srcIP, groupIdx) {
			if s.rec != nil {
				s.rec.RateLimitDrop(string(ratelimit.LevelGroup))
			}
			if s.debug {
				s.log.Debug("rate limited", "level", ratelimit.LevelGroup, "group_idx", groupIdx)
			}
			if s.throttleResp && src != nil {
				s.sendThrottled(conn, src, throttleBucketGroup, seqNum)
			}
			return
		}
	}

	var ackFlags byte
	if s.retransmitMC {
		if err := s.retransmit.Retransmit(raw, txID); err != nil {
			s.log.Error("retransmit error", "err", err)
			return
		}
		ackFlags |= ackFlagMulticastSent
		if s.rec != nil {
			s.rec.Retransmit()
		}
	}
	// Unicast the frame back to the requester when unicast mode is on, or
	// always for a proxied NACK — a cross-domain proxy receives the frame
	// only via this return channel (it is not joined to our shard groups).
	if (s.retransmitUC || proxied) && src != nil {
		if err := s.retransmit.RetransmitUnicast(raw, src); err != nil {
			s.log.Error("unicast retransmit error", "err", err)
		} else {
			ackFlags |= ackFlagUnicastSent
			if s.rec != nil {
				s.rec.UnicastRetransmit()
			}
		}
	}

	if !s.suppressACK && src != nil {
		s.sendResponse(conn, src, msgTypeACK, ackFlags, seqNum)
	}

	if s.debug {
		s.log.Debug("retransmitted frame", "txid", fmt.Sprintf("%x", txID[:8]), "seq_num", seqNum)
	}
}

// sendThrottled sends a 16-byte THROTTLED response carrying a backoff-bucket
// hint in the Flags byte. It echoes the requested SeqNum. The response (16B) is
// smaller than the NACK (64B), so it cannot be used for bandwidth amplification.
func (s *Server) sendThrottled(conn net.PacketConn, src *net.UDPAddr, bucket byte, seqNum uint64) {
	var buf [ResponseSize]byte
	binary.BigEndian.PutUint32(buf[0:4], frame.MagicBSV)
	binary.BigEndian.PutUint16(buf[4:6], frame.ProtoVer)
	buf[6] = msgTypeThrottled
	buf[7] = bucket & 0x0F
	binary.BigEndian.PutUint64(buf[8:16], seqNum)

	if _, err := conn.WriteTo(buf[:], src); err != nil {
		if s.rec != nil {
			s.rec.ResponseSendError("throttled")
		}
		s.log.Warn("failed to send THROTTLED", "dst", src.String(), "err", err)
		return
	}
	if s.rec != nil {
		s.rec.ResponseSent("throttled")
	}
}

// sendResponse sends a 16-byte ACK or MISS response to src.
func (s *Server) sendResponse(conn net.PacketConn, src *net.UDPAddr, msgType byte, flags byte, seqNum uint64) {
	var buf [ResponseSize]byte
	binary.BigEndian.PutUint32(buf[0:4], frame.MagicBSV)
	binary.BigEndian.PutUint16(buf[4:6], frame.ProtoVer)
	buf[6] = msgType
	buf[7] = flags
	binary.BigEndian.PutUint64(buf[8:16], seqNum)

	label := "ack"
	if msgType == msgTypeMISS {
		label = "miss"
	}

	if _, err := conn.WriteTo(buf[:], src); err != nil {
		if s.rec != nil {
			s.rec.ResponseSendError(label)
		}
		s.log.Warn("failed to send response", "type", fmt.Sprintf("0x%02X", msgType), "dst", src.String(), "err", err)
		return
	}
	if s.rec != nil {
		s.rec.ResponseSent(label)
	}
}

// validateNACK checks the NACK datagram format (64 bytes).
func validateNACK(datagram []byte) error {
	if len(datagram) < NACKSize {
		return fmt.Errorf("invalid NACK size: %d", len(datagram))
	}
	if binary.BigEndian.Uint32(datagram[0:4]) != frame.MagicBSV {
		return fmt.Errorf("invalid magic: 0x%08X", binary.BigEndian.Uint32(datagram[0:4]))
	}
	if datagram[6] != msgTypeNACK {
		return fmt.Errorf("invalid message type: 0x%02X", datagram[6])
	}
	return nil
}

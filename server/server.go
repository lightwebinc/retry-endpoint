// Package server implements the UDP NACK receiver for bitcoin-retry-endpoint.
package server

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/lightwebinc/bitcoin-shard-common/frame"
	"github.com/lightwebinc/bitcoin-shard-common/shard"

	"github.com/lightwebinc/bitcoin-retry-endpoint/cache"
	"github.com/lightwebinc/bitcoin-retry-endpoint/metrics"
	"github.com/lightwebinc/bitcoin-retry-endpoint/ratelimit"
)

// NACKSize is the fixed size of a BRC-126 NACK datagram (64 bytes).
const NACKSize = 64

// ResponseSize is the fixed size of a BRC-126 ACK or MISS response (16 bytes).
const ResponseSize = 16

// MsgType constants for BRC-126 protocol messages.
const (
	msgTypeNACK byte = 0x10
	msgTypeMISS byte = 0x11
	msgTypeACK  byte = 0x12
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
	shardEngine  *shard.Engine // for post-lookup group index derivation; nil = skip group limiter
	log          *slog.Logger
}

// Retransmitter is the interface for retransmitting cached frames.
type Retransmitter interface {
	Retransmit(raw []byte, txID [32]byte) error
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
		port:        port,
		cache:       cache,
		rateLimiter: rateLimiter,
		rec:         rec,
		retransmit:  retransmit,
		workers:     workers,
		debug:       debug,
		log:         slog.Default().With("component", "server"),
	}
}

// SetSuppressACK disables ACK responses (for high-volume deployments).
func (s *Server) SetSuppressACK(v bool) { s.suppressACK = v }

// SetSuppressMISS disables MISS responses.
func (s *Server) SetSuppressMISS(v bool) { s.suppressMISS = v }

// SetBindAddr sets the specific IPv6 address the NACK socket binds to.
// When set, ACK/MISS responses are sourced from this address, avoiding
// kernel source-address selection (which may pick a SLAAC-derived address
// that does not match what listeners expect).
func (s *Server) SetBindAddr(addr string) { s.bindAddr = addr }

// SetShardEngine wires the shard engine used to derive groupIdx from TxID
// for the post-lookup group rate limiter. Must be called before Run.
func (s *Server) SetShardEngine(e *shard.Engine) { s.shardEngine = e }

// Run starts the UDP server with a worker pool.
func (s *Server) Run(ctx context.Context) error {
	host := "::"
	if s.bindAddr != "" {
		host = s.bindAddr
	}
	conn, err := net.ListenPacket("udp6", fmt.Sprintf("[%s]:%d", host, s.port))
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

	// datagram[7] = Flags (reserved, ignored)
	hashKey := binary.BigEndian.Uint64(datagram[8:16])
	startSeq := binary.BigEndian.Uint64(datagram[16:24])
	endSeq := binary.BigEndian.Uint64(datagram[24:32])

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

	// Rate limiting: tier 4 (group), post-lookup.
	// On throttle: skip retransmit but still send ACK (frame exists; listener
	// must not escalate to the next endpoint on an honest ACK).
	groupThrottled := false
	if s.shardEngine != nil {
		groupIdx := s.shardEngine.GroupIndex(&txID)
		if !s.rateLimiter.AllowGroup(srcIP, groupIdx) {
			groupThrottled = true
			if s.rec != nil {
				s.rec.RateLimitDrop(string(ratelimit.LevelGroup))
			}
			if s.debug {
				s.log.Debug("rate limited", "level", ratelimit.LevelGroup, "group_idx", groupIdx)
			}
		}
	}

	if !groupThrottled {
		if err := s.retransmit.Retransmit(raw, txID); err != nil {
			s.log.Error("retransmit error", "err", err)
			return
		}
		if s.rec != nil {
			s.rec.Retransmit()
		}
	}

	if !s.suppressACK && src != nil {
		s.sendResponse(conn, src, msgTypeACK, 0x01, seqNum)
	}

	if s.debug {
		s.log.Debug("retransmitted frame", "txid", fmt.Sprintf("%x", txID[:8]), "seq_num", seqNum)
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

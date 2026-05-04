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

	"github.com/lightwebinc/bitcoin-retry-endpoint/cache"
	"github.com/lightwebinc/bitcoin-retry-endpoint/metrics"
	"github.com/lightwebinc/bitcoin-retry-endpoint/ratelimit"
)

// NACKSize is the fixed size of a BRC-TBD-retransmission NACK datagram (56 bytes).
// See docs/brc-tbd-retransmission-protocol.md for the wire format.
const NACKSize = 56

// ResponseSize is the fixed size of a BRC-TBD-retransmission ACK or MISS response (24 bytes).
const ResponseSize = 24

// MsgType constants for BRC-TBD-retransmission protocol messages.
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
	suppressACK  bool // if true, do not send ACK responses
	suppressMISS bool // if true, do not send MISS responses
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

	// Validate NACK format.
	if err := validateNACK(datagram); err != nil {
		s.log.Debug("invalid NACK", "err", err)
		return
	}

	// Extract fields (BRC-TBD-retransmission 56-byte format).
	txID := extractTxID(datagram)
	senderID := extractSenderID(datagram)
	sequenceID := extractSequenceID(datagram)
	seqNum := extractSeqNum(datagram)

	// Rate limiting.
	var srcIP net.IP
	if src != nil {
		srcIP = src.IP
	} else {
		srcIP = net.IPv6unspecified
	}
	allowed, level := s.rateLimiter.Allow(srcIP, senderID, sequenceID)
	if !allowed {
		if s.rec != nil {
			s.rec.RateLimitDrop(string(level))
		}
		if s.debug {
			s.log.Debug("rate limited", "level", level)
		}
		return
	}

	// Build cache key from uint32 fields.
	key := make([]byte, 12)
	binary.BigEndian.PutUint32(key[0:4], senderID)
	binary.BigEndian.PutUint32(key[4:8], sequenceID)
	binary.BigEndian.PutUint32(key[8:12], seqNum)

	// Retrieve from cache.
	raw, err := s.cache.Retrieve(key)
	if err != nil {
		if s.rec != nil {
			s.rec.CacheError()
		}
		s.log.Error("cache retrieve error", "err", err)
		return
	}
	if raw == nil {
		if s.rec != nil {
			s.rec.CacheMiss()
		}
		if s.debug {
			s.log.Debug("cache miss", "seq", seqNum)
		}
		// Send MISS response.
		if !s.suppressMISS && src != nil {
			s.sendResponse(conn, src, msgTypeMISS, 0, senderID, sequenceID, seqNum)
		}
		return
	}

	if s.rec != nil {
		s.rec.CacheHit()
	}

	// Retransmit.
	if err := s.retransmit.Retransmit(raw, txID); err != nil {
		s.log.Error("retransmit error", "err", err)
		return
	}

	if s.rec != nil {
		s.rec.Retransmit()
	}

	// Send ACK response.
	if !s.suppressACK && src != nil {
		var flags byte = 0x01 // multicast_sent (default)
		s.sendResponse(conn, src, msgTypeACK, flags, senderID, sequenceID, seqNum)
	}

	if s.debug {
		s.log.Debug("retransmitted frame", "txid", fmt.Sprintf("%x", txID[:8]), "seq", seqNum)
	}
}

// sendResponse sends a 24-byte ACK or MISS response to src.
func (s *Server) sendResponse(conn net.PacketConn, src *net.UDPAddr, msgType byte, flags byte, senderID, sequenceID, seqNum uint32) {
	var buf [ResponseSize]byte
	binary.BigEndian.PutUint32(buf[0:4], frame.MagicBSV)
	binary.BigEndian.PutUint16(buf[4:6], frame.ProtoVer)
	buf[6] = msgType
	buf[7] = flags
	binary.BigEndian.PutUint32(buf[8:12], senderID)
	binary.BigEndian.PutUint32(buf[12:16], sequenceID)
	binary.BigEndian.PutUint32(buf[16:20], seqNum)
	// buf[20:24] reserved (zero)

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

// validateNACK checks the NACK datagram format (BRC-TBD-retransmission, 56 bytes).
func validateNACK(datagram []byte) error {
	if len(datagram) != NACKSize {
		return fmt.Errorf("invalid NACK size: %d", len(datagram))
	}

	// Check magic (bytes 0-3).
	magic := binary.BigEndian.Uint32(datagram[0:4])
	if magic != frame.MagicBSV {
		return fmt.Errorf("invalid magic: 0x%08X", magic)
	}

	// Check protocol version (bytes 4-5).
	proto := binary.BigEndian.Uint16(datagram[4:6])
	if proto != frame.ProtoVer {
		return fmt.Errorf("invalid protocol version: %d", proto)
	}

	// Check message type (byte 6) - should be 0x10 for NACK.
	if datagram[6] != msgTypeNACK {
		return fmt.Errorf("invalid message type: 0x%02X", datagram[6])
	}

	return nil
}

// extractTxID extracts the TxID from a NACK datagram (bytes 8-39).
func extractTxID(datagram []byte) [32]byte {
	var txID [32]byte
	copy(txID[:], datagram[8:40])
	return txID
}

// extractSenderID extracts the SenderID (uint32) from a NACK datagram (bytes 40-43).
func extractSenderID(datagram []byte) uint32 {
	return binary.BigEndian.Uint32(datagram[40:44])
}

// extractSequenceID extracts the SequenceID (uint32) from a NACK datagram (bytes 44-47).
func extractSequenceID(datagram []byte) uint32 {
	return binary.BigEndian.Uint32(datagram[44:48])
}

// extractSeqNum extracts the SeqNum (uint32) from a NACK datagram (bytes 48-51).
func extractSeqNum(datagram []byte) uint32 {
	return binary.BigEndian.Uint32(datagram[48:52])
}

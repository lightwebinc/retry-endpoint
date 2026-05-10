// Package beacon implements the ADVERT beacon sender for bitcoin-retry-endpoint.
// It periodically multicasts 56-byte ADVERT datagrams to site-local and/or
// global beacon groups so that listeners can discover this endpoint dynamically.
package beacon

import (
	"context"
	"encoding/binary"
	"log/slog"
	"net"
	"syscall"
	"time"

	"github.com/lightwebinc/bitcoin-shard-common/frame"
	"github.com/lightwebinc/bitcoin-shard-common/shard"
)

// ADVERTSize is the fixed size of an ADVERT beacon datagram.
const ADVERTSize = 56

// MsgTypeADVERT is the message type for ADVERT beacons.
const MsgTypeADVERT byte = 0x20

// Flag bits (BRC-126).
const (
	FlagHasParent           uint16 = 0x0002
	FlagDraining            uint16 = 0x0004
	FlagUnicastRetransmit   uint16 = 0x0008
	FlagMulticastRetransmit uint16 = 0x0010
)

// Config holds beacon sender parameters.
type Config struct {
	NACKAddr    net.IP         // our IPv6 address for NACK reception
	NACKPort    uint16         // our NACK listen port
	Tier        uint8          // 0 = closest to source
	Preference  uint8          // weighting within tier; higher = more preferred
	Interval    time.Duration  // beacon interval (default 60s)
	Scope       byte           // 0x05=site, 0x08=org, 0x0E=global, 0xFF=all
	Flags       uint16         // ADVERT flags (see constants above)
	InstanceID  uint32         // unique instance identifier
	MiddleBytes [11]byte       // bytes 2-12 of the multicast prefix
	Iface       *net.Interface // outgoing multicast interface
}

// Sender periodically multicasts ADVERT beacons.
type Sender struct {
	cfg Config
	log *slog.Logger
}

// New creates a beacon Sender.
func New(cfg Config) *Sender {
	if cfg.Interval == 0 {
		cfg.Interval = 60 * time.Second
	}
	return &Sender{
		cfg: cfg,
		log: slog.Default().With("component", "beacon"),
	}
}

// Run starts the beacon loop. Blocks until ctx is cancelled.
func (s *Sender) Run(ctx context.Context) error {
	// Build the ADVERT payload once (only Draining flag might change).
	buf := s.buildADVERT()

	// Determine target beacon groups.
	groups := s.beaconGroups()
	if len(groups) == 0 {
		s.log.Warn("no beacon groups configured, beacon disabled")
		return nil
	}

	// Open multicast send sockets.
	// Set IPV6_MULTICAST_IF to force beacons out the fabric interface (enp6s0),
	// overriding the lower-metric management default route.
	conns := make([]*net.UDPConn, 0, len(groups))
	for _, grp := range groups {
		conn, err := net.DialUDP("udp6", nil, grp)
		if err != nil {
			s.log.Error("beacon: cannot dial beacon group", "group", grp, "err", err)
			continue
		}
		if s.cfg.Iface != nil {
			if rc, err := conn.SyscallConn(); err == nil {
				_ = rc.Control(func(fd uintptr) {
					_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IPV6,
						syscall.IPV6_MULTICAST_IF, s.cfg.Iface.Index)
				})
			}
		}
		conns = append(conns, conn)
	}
	if len(conns) == 0 {
		return nil
	}
	defer func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}()

	s.log.Info("beacon sender started",
		"interval", s.cfg.Interval,
		"scope", s.cfg.Scope,
		"tier", s.cfg.Tier,
		"preference", s.cfg.Preference,
		"groups", len(groups),
	)

	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()

	// Send immediately on startup.
	s.send(conns, buf)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.send(conns, buf)
		}
	}
}

func (s *Sender) send(conns []*net.UDPConn, buf []byte) {
	for _, conn := range conns {
		if _, err := conn.Write(buf); err != nil {
			s.log.Debug("beacon send error", "err", err)
		}
	}
}

func (s *Sender) buildADVERT() []byte {
	buf := make([]byte, ADVERTSize)
	binary.BigEndian.PutUint32(buf[0:4], frame.MagicBSV)
	binary.BigEndian.PutUint16(buf[4:6], frame.ProtoVer)
	buf[6] = MsgTypeADVERT
	buf[7] = s.cfg.Scope

	nackIP := s.cfg.NACKAddr.To16()
	if nackIP == nil {
		nackIP = net.IPv6unspecified
	}
	copy(buf[8:24], nackIP)

	binary.BigEndian.PutUint16(buf[24:26], s.cfg.NACKPort)
	buf[26] = s.cfg.Tier
	buf[27] = s.cfg.Preference

	intervalSec := uint16(s.cfg.Interval.Seconds())
	binary.BigEndian.PutUint16(buf[28:30], intervalSec)
	binary.BigEndian.PutUint16(buf[30:32], s.cfg.Flags)
	binary.BigEndian.PutUint32(buf[32:36], s.cfg.InstanceID)
	// bytes 36-55: reserved (already zero)
	return buf
}

func (s *Sender) beaconGroups() []*net.UDPAddr {
	beaconPort := 9300 // default beacon port
	var groups []*net.UDPAddr

	if s.cfg.Scope == 0x05 || s.cfg.Scope == 0xFF {
		ip := shard.ControlGroupAddr(0xFF05, s.cfg.MiddleBytes, shard.CtrlGroupBeacon)
		groups = append(groups, &net.UDPAddr{IP: ip, Port: beaconPort})
	}
	if s.cfg.Scope == 0x08 || s.cfg.Scope == 0xFF {
		ip := shard.ControlGroupAddr(0xFF08, s.cfg.MiddleBytes, shard.CtrlGroupBeacon)
		groups = append(groups, &net.UDPAddr{IP: ip, Port: beaconPort})
	}
	if s.cfg.Scope == 0x0E || s.cfg.Scope == 0xFF {
		ip := shard.ControlGroupAddr(0xFF0E, s.cfg.MiddleBytes, shard.CtrlGroupBeacon)
		groups = append(groups, &net.UDPAddr{IP: ip, Port: beaconPort})
	}

	return groups
}

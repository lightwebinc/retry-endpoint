// Package beacon implements the ADVERT beacon sender for retry-endpoint.
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

	"github.com/lightwebinc/retry-endpoint/metrics"
	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/shard"
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
	NACKAddr   net.IP         // our IPv6 address for NACK reception
	NACKPort   uint16         // our NACK listen port
	Tier       uint8          // 0 = closest to source
	Preference uint8          // weighting within tier; higher = more preferred
	Interval   time.Duration  // beacon interval (default 60s)
	Scope      byte           // 0x05=site, 0x08=org, 0x0E=global, 0xFF=all
	Flags      uint16         // ADVERT flags (see constants above)
	InstanceID uint32         // unique instance identifier
	GroupID    uint16         // IANA group-id occupying bytes 12–13 (default 0x000B)
	Iface      *net.Interface // outgoing multicast interface
	BindSource net.IP         // optional IPv6 to bind for beacon egress; required when SSM listeners pre-declare this retry-endpoint in sources.bootstrap.beacon

	// GroupPrefixes are the upper-16-bit multicast prefixes to advertise
	// into, already resolved from (-control-group-compat, -source-mode,
	// -beacon-scope) by config.Config.BeaconGroupPrefixes. BRC-126 and
	// BRC-129 require FF3x under SSM (FF35 site, FF3E global), not the
	// any-source FF0x form; carrying the resolved list here keeps that
	// decision in one place (config/controlgroup.go) instead of a scope
	// table in this package.
	//
	// Empty falls back to the pre-fix derivation — the any-source
	// prefixes implied by Scope — so a caller that has not been updated
	// keeps its old wire rather than silently changing group.
	GroupPrefixes []uint16
}

// Sender periodically multicasts ADVERT beacons.
type Sender struct {
	cfg Config
	log *slog.Logger
	rec *metrics.Recorder // nil = no metrics
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

// SetRecorder attaches a metrics recorder for counting sent ADVERTs.
func (s *Sender) SetRecorder(rec *metrics.Recorder) {
	s.rec = rec
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
	var laddr *net.UDPAddr
	if s.cfg.BindSource != nil {
		laddr = &net.UDPAddr{IP: s.cfg.BindSource}
	}
	conns := make([]*net.UDPConn, 0, len(groups))
	for _, grp := range groups {
		conn, err := net.DialUDP("udp6", laddr, grp)
		if err != nil {
			s.log.Error("beacon: cannot dial beacon group", "group", grp, "err", err, "bind_source", s.cfg.BindSource)
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

	// The group list is logged in full, not just counted: on the BRC-126/129
	// flag day "which group am I advertising into" is the question an
	// operator has to answer from the logs, because landing on the wrong one
	// produces no error at either end.
	dests := make([]string, 0, len(groups))
	for _, grp := range groups {
		dests = append(dests, grp.String())
	}
	s.log.Info("beacon sender started",
		"interval", s.cfg.Interval,
		"scope", s.cfg.Scope,
		"tier", s.cfg.Tier,
		"preference", s.cfg.Preference,
		"groups", dests,
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
		} else if s.rec != nil {
			s.rec.BeaconAdvertSent()
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

// beaconGroups returns the destinations this endpoint advertises into: one
// per resolved multicast prefix at the BRC-126 beacon port.
//
// The prefixes are NOT derived here. BRC-126 §Beacon Scopes and BRC-129
// §Source Mode and Address Range make the control-plane group address a
// function of the source mode as well as the scope — FF35::B:FFFD at site
// scope under SSM, not FF05::B:FFFD — and this package has no view of
// -source-mode. config.Config.BeaconGroupPrefixes resolves it (reusing
// shard.Prefix, the same helper the data plane uses) and hands the answer
// down in Config.GroupPrefixes.
func (s *Sender) beaconGroups() []*net.UDPAddr {
	beaconPort := 9300 // default beacon port

	prefixes := s.cfg.GroupPrefixes
	if len(prefixes) == 0 {
		prefixes = legacyASMPrefixes(s.cfg.Scope)
	}

	groups := make([]*net.UDPAddr, 0, len(prefixes))
	for _, prefix := range prefixes {
		ip := shard.GroupAddr(prefix, s.cfg.GroupID, shard.GroupBeacon)
		groups = append(groups, &net.UDPAddr{IP: ip, Port: beaconPort})
	}
	return groups
}

// legacyASMPrefixes is the pre-fix derivation: the any-source FF0x prefixes
// implied by the ADVERT scope byte, ignoring the source mode. It survives
// only as the fallback for a Config with no GroupPrefixes set, so that an
// un-updated caller keeps the exact wire it had.
func legacyASMPrefixes(scope byte) []uint16 {
	var out []uint16
	if scope == 0x05 || scope == 0xFF {
		out = append(out, 0xFF05)
	}
	if scope == 0x08 || scope == 0xFF {
		out = append(out, 0xFF08)
	}
	if scope == 0x0E || scope == 0xFF {
		out = append(out, 0xFF0E)
	}
	return out
}

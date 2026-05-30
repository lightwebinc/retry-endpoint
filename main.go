// Command retry-endpoint caches multicast BSV transaction frames
// and retransmits them on demand via NACK requests.
package main

import (
	"context"
	"fmt"
	"hash/crc32"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"net/netip"

	"github.com/lightwebinc/shard-common/bootstrap"
	"github.com/lightwebinc/shard-common/shard"

	"github.com/lightwebinc/retry-endpoint/beacon"
	"github.com/lightwebinc/retry-endpoint/cache"
	"github.com/lightwebinc/retry-endpoint/cache/memory"
	"github.com/lightwebinc/retry-endpoint/cache/redis"
	"github.com/lightwebinc/retry-endpoint/config"
	"github.com/lightwebinc/retry-endpoint/ingress"
	"github.com/lightwebinc/retry-endpoint/metrics"
	"github.com/lightwebinc/retry-endpoint/ratelimit"
	"github.com/lightwebinc/retry-endpoint/retransmit"
	"github.com/lightwebinc/retry-endpoint/server"
)

// buildSSMGroupSources resolves the per-control-group bootstrap source
// lists into an ingress.GroupSources map keyed by the group's IPv6 (S,G)
// receiver-side join target. Resolvers run for the lifetime of ctx; the
// initial resolution is synchronous and fail-closed (Start returns an
// error if any configured list resolves to zero addresses).
//
// Mapping (BRC-129 indices):
//
//	GroupBeacon            (0xFFFD) → SSMBootstrapBeacon (retry-endpoint pods)
//	GroupSubtreeAnnounce   (0xFFFB) → SSMBootstrapSubtreeAnn
//	GroupSubtreeGroupAnn   (0xFFFC) → SSMBootstrapSubtreeAnn (same source set)
//	GroupBlockBroadcast    (0xFFFE) → SSMBootstrapManifest (block-bcast follows manifest emitters)
//	data-plane shard groups        → SSMPublishersStatic (lab) or manifest
//	                                  union (production — wired by the
//	                                  manifest consumer in a later iteration)
func buildSSMGroupSources(ctx context.Context, cfg *config.Config) (ingress.GroupSources, error) {
	gs := make(ingress.GroupSources)

	resolve := func(entries []string, target ingress.GroupSources, groupIPs ...net.IP) error {
		if len(entries) == 0 {
			return nil
		}
		r := &bootstrap.Resolver{
			Entries: entries,
			Refresh: cfg.SSMBootstrapRefresh,
		}
		if err := r.Start(ctx); err != nil {
			return err
		}
		for _, gip := range groupIPs {
			ga, ok := netip.AddrFromSlice(gip.To16())
			if !ok {
				return fmt.Errorf("group %s: not IPv6", gip)
			}
			target[ga] = r.Current()
		}
		return nil
	}

	// Beacon group sources = retry-endpoint pods (self).
	beaconIP := shard.GroupAddr(cfg.MCPrefix, cfg.MCGroupID, shard.GroupBeacon)
	if err := resolve(cfg.SSMBootstrapBeacon, gs, beaconIP); err != nil {
		return nil, fmt.Errorf("beacon: %w", err)
	}
	// Subtree-announce + subtree-group-announce share the same emitter set.
	subAnnIP := shard.GroupAddr(cfg.MCPrefix, cfg.MCGroupID, shard.GroupSubtreeAnnounce)
	subGrpAnnIP := shard.GroupAddr(cfg.MCPrefix, cfg.MCGroupID, shard.GroupSubtreeGroupAnnounce)
	if err := resolve(cfg.SSMBootstrapSubtreeAnn, gs, subAnnIP, subGrpAnnIP); err != nil {
		return nil, fmt.Errorf("subtree-announce: %w", err)
	}
	// Manifest emitters drive block-broadcast too.
	blockBcastIP := shard.GroupAddr(cfg.MCPrefix, cfg.MCGroupID, shard.GroupBlockBroadcast)
	if err := resolve(cfg.SSMBootstrapManifest, gs, blockBcastIP); err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}

	// Data-plane: static publisher list (lab/CI). Production manifest-
	// driven discovery wires the same map after parsing each manifest.
	if len(cfg.SSMPublishersStatic) > 0 {
		r := &bootstrap.Resolver{
			Entries: cfg.SSMPublishersStatic,
			Refresh: cfg.SSMBootstrapRefresh,
		}
		if err := r.Start(ctx); err != nil {
			return nil, fmt.Errorf("publishers-static: %w", err)
		}
		srcs := r.Current()
		// Apply to every shard group address derived from MCPrefix/MCGroupID.
		// The shard.Engine.Addr helper isn't visible here (no engine in
		// main scope at this point), but iterating numGroups by index is
		// straightforward.
		for idx := uint32(0); idx < cfg.NumGroups; idx++ {
			ip := shard.GroupAddr(cfg.MCPrefix, cfg.MCGroupID, shard.GroupIdx(idx))
			ga, ok := netip.AddrFromSlice(ip.To16())
			if !ok {
				continue
			}
			gs[ga] = srcs
		}
	}
	return gs, nil
}

// hashInstanceID derives a 32-bit identifier for this endpoint from the
// instance name so the listener registry can key ADVERT entries stably
// across restarts. CRC32c is hardware-accelerated on x86 (SSE4.2) and
// ARM (ARMv8 CRC extensions).
func hashInstanceID(s string) uint32 {
	h := crc32.Checksum([]byte(s), crc32.MakeTable(crc32.Castagnoli))
	if h == 0 {
		h = 1 // 0 is reserved / ignored by some consumers
	}
	return h
}

// pickBeaconNACKAddr returns a suitable IPv6 unicast address for the ADVERT
// NACKAddr field. If an explicit address is configured it is returned.
// Otherwise the first global-unicast address on the given interface is used.
func pickBeaconNACKAddr(explicit string, iface *net.Interface) (net.IP, error) {
	if explicit != "" {
		ip := net.ParseIP(explicit)
		if ip == nil {
			return nil, fmt.Errorf("invalid nack-addr %q", explicit)
		}
		return ip.To16(), nil
	}
	addrs, err := iface.Addrs()
	if err != nil {
		return nil, fmt.Errorf("iface %s addrs: %w", iface.Name, err)
	}
	for _, a := range addrs {
		var ip net.IP
		switch v := a.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}
		if ip == nil || ip.To4() != nil {
			continue
		}
		if ip.IsGlobalUnicast() && !ip.IsLinkLocalUnicast() {
			return ip.To16(), nil
		}
	}
	return nil, fmt.Errorf("no global-unicast IPv6 address on %s; set -nack-addr explicitly", iface.Name)
}

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	logLevel := slog.LevelInfo
	if cfg.Debug {
		logLevel = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel})))

	slog.Info("retry-endpoint starting",
		"shard_bits", cfg.ShardBits,
		"num_groups", cfg.NumGroups,
		"scope", cfg.MCScope,
		"mc_port", cfg.ListenPort,
		"nack_port", cfg.NACKPort,
		"cache_backend", cfg.CacheBackend,
		"egress_port", cfg.EgressPort,
		"dedup_window", cfg.DedupWindow,
	)

	// Initialize metrics.
	rec, err := metrics.New(cfg.InstanceID, cfg.NumWorkers, cfg.OTLPEndpoint, cfg.OTLPInterval)
	if err != nil {
		return err
	}

	// Build shard engine.
	engine := shard.New(cfg.MCPrefix, cfg.MCGroupID, cfg.ShardBits)

	// Build cache backend.
	var c cache.Cache
	var redisCache *redis.Cache
	switch cfg.CacheBackend {
	case "redis":
		if cfg.RedisAddr == "" {
			return fmt.Errorf("REDIS_ADDR required when CACHE_BACKEND=redis")
		}
		redisCache, err = redis.New(cfg.RedisAddr, "bre:frame:")
		if err != nil {
			return err
		}
		c = redisCache
	case "memory":
		c = memory.New(cfg.CacheMaxKeys)
	default:
		slog.Warn("unknown cache backend, using memory", "backend", cfg.CacheBackend)
		c = memory.New(cfg.CacheMaxKeys)
	}
	defer func() { _ = c.Close() }()

	// Cross-instance dedup via Redis SET NX.
	// When CACHE_BACKEND=memory and REDIS_ADDR is set, frame storage stays per-instance
	// (freecache), and Redis is used only for the dedup gate in Retransmitter.Retransmit().
	if redisCache == nil && cfg.RedisAddr != "" {
		redisCache, err = redis.New(cfg.RedisAddr, "bre:dedup:")
		if err != nil {
			slog.Warn("redis dedup unavailable, running without cross-instance dedup", "addr", cfg.RedisAddr, "err", err)
			redisCache = nil
		} else {
			defer func() { _ = redisCache.Close() }()
			slog.Info("cross-instance dedup enabled", "addr", cfg.RedisAddr)
		}
	}

	// Build multicast groups for ingress.
	groups, err := buildGroups(cfg, engine)
	if err != nil {
		return err
	}
	slog.Info("multicast groups", "count", len(groups))

	// Resolve ingress interface.
	mcIface, err := net.InterfaceByName(cfg.MCIface)
	if err != nil {
		return err
	}

	// Resolve egress interfaces.
	egressIfaces := make([]*net.Interface, 0, len(cfg.EgressIfaces))
	for _, name := range cfg.EgressIfaces {
		iface, err := net.InterfaceByName(name)
		if err != nil {
			return err
		}
		egressIfaces = append(egressIfaces, iface)
	}

	// Build rate limiter.
	rl := ratelimit.New(ratelimit.Config{
		IPRate:         cfg.RLIPRate,
		IPBurst:        cfg.RLIPBurst,
		SenderRate:     cfg.RLSenderRate,
		SenderWindow:   cfg.RLSenderWindow,
		ChainRate:      cfg.RLChainRate,
		ChainWindow:    cfg.RLChainWindow,
		SequenceMax:    cfg.RLSequenceMax,
		SequenceWindow: cfg.RLSequenceWindow,
		GroupRate:      cfg.RLGroupRate,
		GroupBurst:     cfg.RLGroupBurst,
	})

	// Build retransmitter.
	retrans := retransmit.New(engine, egressIfaces, cfg.EgressPort, cfg.DedupWindow, redisCache, rec, cfg.Debug)
	if err := retrans.Open(); err != nil {
		return err
	}
	defer func() { _ = retrans.Close() }()

	// Resolve NACK bind address. This is used to bind the NACK listening
	// socket so ACK/MISS responses are sourced from the correct address
	// (avoids kernel SLAAC source-address selection mismatch).
	nackBindIP, err := pickBeaconNACKAddr(cfg.BeaconNACKAddr, egressIfaces[0])
	if err != nil {
		return fmt.Errorf("resolve nack bind address: %w", err)
	}

	// Build server.
	srv := server.New(cfg.NACKPort, c, rl, rec, retrans, cfg.NACKWorkers, cfg.Debug)
	srv.SetBindAddr(nackBindIP.String())
	srv.SetSuppressACK(cfg.SuppressACK)
	srv.SetSuppressMISS(cfg.SuppressMISS)
	srv.SetShardEngine(engine)

	// Build ingress worker.
	ing := ingress.New(mcIface, cfg.ListenPort, groups, c, rec, ingress.TTLConfig{
		Tx:      cfg.CacheTTLTx,
		Block:   cfg.CacheTTLBlock,
		Subtree: cfg.CacheTTLSubtree,
		Anchor:  cfg.CacheTTLAnchor,
	}, cfg.Debug)

	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// SSM: resolve the per-control-group bootstrap source lists. Resolvers
	// run for the lifetime of ctx; OnChange triggers will not propagate
	// to live joins until a future iteration adds dynamic membership
	// (the current ingress worker resolves once at start). For now,
	// fail-closed startup ensures every configured control group has at
	// least one resolved source.
	if cfg.SourceMode == "ssm" {
		gs, err := buildSSMGroupSources(ctx, cfg)
		if err != nil {
			return fmt.Errorf("ssm bootstrap: %w", err)
		}
		ing.SetGroupSources(gs)
	}

	var wg sync.WaitGroup

	// Start metrics server.
	wg.Add(1)
	go func() {
		defer wg.Done()
		rec.Serve(cfg.MetricsAddr, done)
	}()

	// Start cache size sampler (samples Len() every 15s if the backend supports it).
	if sizer, ok := c.(interface{ Len() int }); ok {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t := time.NewTicker(15 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					rec.CacheSize(int64(sizer.Len()))
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// Start ingress worker.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := ing.Run(ctx); err != nil {
			slog.Error("ingress exited with error", "err", err)
		}
	}()

	// Start server.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.Run(ctx); err != nil {
			slog.Error("server exited with error", "err", err)
		}
	}()

	// Start beacon sender.
	if cfg.BeaconEnabled {
		beaconIface := egressIfaces[0]
		nackIP, perr := pickBeaconNACKAddr(cfg.BeaconNACKAddr, beaconIface)
		if perr != nil {
			return fmt.Errorf("beacon: %w", perr)
		}
		var flags uint16
		if cfg.BeaconFlagsUnicast {
			flags |= beacon.FlagUnicastRetransmit
		}
		if cfg.BeaconFlagsMulticast {
			flags |= beacon.FlagMulticastRetransmit
		}
		if cfg.BeaconFlagsDraining {
			flags |= beacon.FlagDraining
		}
		host := cfg.InstanceID
		if host == "" {
			if h, herr := os.Hostname(); herr == nil {
				host = h
			}
		}
		var bindSrc net.IP
		if cfg.BindSource != "" {
			bindSrc = net.ParseIP(cfg.BindSource)
		}
		beaconCfg := beacon.Config{
			NACKAddr:   nackIP,
			NACKPort:   uint16(cfg.NACKPort),
			Tier:       uint8(cfg.BeaconTier),
			Preference: uint8(cfg.BeaconPreference),
			Interval:   cfg.BeaconInterval,
			Scope:      cfg.BeaconScopeByte,
			Flags:      flags,
			InstanceID: hashInstanceID(host),
			GroupID:    cfg.MCGroupID,
			Iface:      beaconIface,
			BindSource: bindSrc,
		}
		beaconSender := beacon.New(beaconCfg)
		beaconSender.SetRecorder(rec)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := beaconSender.Run(ctx); err != nil {
				slog.Error("beacon exited with error", "err", err)
			}
		}()
	}

	// Wait for signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	slog.Info("shutdown signal received", "signal", sig)

	if cfg.DrainTimeout > 0 {
		rec.SetDraining()
		slog.Info("draining", "timeout", cfg.DrainTimeout)
		time.Sleep(cfg.DrainTimeout)
	}

	cancel()
	close(done)
	wg.Wait()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	rec.Shutdown(ctx2)

	slog.Info("shutdown complete")
	return nil
}

func buildGroups(cfg *config.Config, engine *shard.Engine) ([]*net.UDPAddr, error) {
	groups := make([]*net.UDPAddr, cfg.NumGroups)
	for i := uint32(0); i < cfg.NumGroups; i++ {
		addr := engine.Addr(i, cfg.ListenPort)
		groups[i] = addr
	}

	// Join the block broadcast group (FF0E::B:FFFE) so we cache block
	// announcement and coinbase frames for retransmission.
	ctrlIP := shard.GroupAddr(cfg.MCPrefix, cfg.MCGroupID, shard.GroupBlockBroadcast)
	groups = append(groups, &net.UDPAddr{IP: ctrlIP, Port: cfg.ListenPort})

	// Join the subtree data group (FF0X::B:FFFB) when BRC-132 caching is enabled.
	if cfg.SubtreeDataEnabled {
		subtreeDataIP := shard.GroupAddr(cfg.MCPrefix, cfg.MCGroupID, shard.GroupSubtreeAnnounce)
		groups = append(groups, &net.UDPAddr{IP: subtreeDataIP, Port: cfg.ListenPort})
	}

	return groups, nil
}

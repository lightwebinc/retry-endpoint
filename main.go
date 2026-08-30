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
	scache "github.com/lightwebinc/shard-common/cache"
	"github.com/lightwebinc/shard-common/hostinfo"
	"github.com/lightwebinc/shard-common/logging"
	"github.com/lightwebinc/shard-common/shard"
	"github.com/lightwebinc/shard-common/tracing"

	"github.com/lightwebinc/retry-endpoint/beacon"
	"github.com/lightwebinc/retry-endpoint/cache"
	"github.com/lightwebinc/retry-endpoint/config"
	"github.com/lightwebinc/retry-endpoint/ingress"
	"github.com/lightwebinc/retry-endpoint/metrics"
	"github.com/lightwebinc/retry-endpoint/proxy"
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
//	GroupSubtreeDataAnnounce   (0xFFFB) → SSMBootstrapSubtreeAnn
//	GroupSubtreeGroupAnn   (0xFFFC) → SSMBootstrapSubtreeAnn (same source set)
//	GroupBlockBroadcast    (0xFFFE) → SSMBootstrapManifest (block-bcast follows manifest emitters)
//	data-plane shard groups        → SSMPublishersStatic (lab) or manifest
//	                                  union (production — wired by the
//	                                  manifest consumer in a later iteration)
//
// excludeOwnSource drops the node's own source address from a roster before
// it feeds (S,G) joins. A node must never join its own source on the PIM
// interface: the MLD state installs an iif==oif mroute on a collapsed edge
// and every originated frame re-enters the MFC until hop-limit death —
// measured ~60x egress amplification. Consequence: this process does not
// receive own-node frames via multicast; a local mirror is required where
// own-source completeness matters.
func excludeOwnSource(srcs []netip.Addr, own netip.Addr) []netip.Addr {
	if !own.IsValid() {
		return srcs
	}
	out := srcs[:0]
	for _, s := range srcs {
		if s != own {
			out = append(out, s)
		}
	}
	return out
}

func buildSSMGroupSources(ctx context.Context, cfg *config.Config) (ingress.GroupSources, error) {
	gs := make(ingress.GroupSources)

	var own netip.Addr
	if cfg.BindSource != "" {
		if a, err := netip.ParseAddr(cfg.BindSource); err == nil {
			own = a
		}
	}

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
			target[ga] = excludeOwnSource(r.Current(), own)
		}
		return nil
	}

	// Beacon group sources = retry-endpoint pods (self).
	beaconIP := shard.GroupAddr(cfg.MCPrefix, cfg.MCGroupID, shard.GroupBeacon)
	if err := resolve(cfg.SSMBootstrapBeacon, gs, beaconIP); err != nil {
		return nil, fmt.Errorf("beacon: %w", err)
	}
	// Subtree-announce + subtree-group-announce share the same emitter set.
	subAnnIP := shard.GroupAddr(cfg.MCPrefix, cfg.MCGroupID, shard.GroupSubtreeDataAnnounce)
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
		srcs := excludeOwnSource(r.Current(), own)
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
		// BRC-148 plane band groups inherit the same announced roster
		// (object planes publish from the transaction plane's sources).
		if cfg.BEEFEnabled {
			for i := uint32(0); i < 1<<cfg.BEEFShardBits; i++ {
				ip := shard.GroupAddr(cfg.MCPrefix, cfg.MCGroupID, shard.GroupIdx(0x1000+i))
				ga, ok := netip.AddrFromSlice(ip.To16())
				if !ok {
					continue
				}
				gs[ga] = srcs
			}
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

	logLevel := logging.ParseLevel(cfg.LogLevel)
	if cfg.Debug {
		logLevel = slog.LevelDebug
	}
	levelVar := logging.Init(logging.Options{
		Service:    metrics.ServiceName,
		InstanceID: cfg.InstanceID,
		Version:    metrics.Version,
		Level:      logLevel,
		Format:     logging.ParseFormat(cfg.LogFormat),
	})
	logging.InstallSIGHUPToggle(levelVar, logLevel)

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
	rec.SetLevelVar(levelVar)

	// One-shot host inventory: descriptive payload as a log event, slim
	// numerics mirrored as the bre_host_info gauge.
	inv := hostinfo.Gather(metrics.ServiceName, metrics.Version)
	rec.SetHostInfo(inv)
	slog.Info("host.inventory", "inventory", inv)

	// Publish this node's own source so the starvation rule can subtract the
	// (site, own source) pair, which excludeOwnSource makes structurally
	// uncacheable. No-op when unset.
	rec.SetOwnSource(cfg.BindSource)

	// Opt-in distributed tracing (no-op unless -trace-sampling > 0 with an OTLP
	// endpoint). Control-plane only (NACK -> retransmit); not the cache hot path.
	_, traceShutdown, terr := tracing.Init(context.Background(), tracing.Options{
		Service:      metrics.ServiceName,
		InstanceID:   cfg.InstanceID,
		Version:      metrics.Version,
		OTLPEndpoint: cfg.OTLPEndpoint,
		Sampling:     cfg.TraceSampling,
	})
	if terr != nil {
		slog.Warn("tracing init failed; continuing without traces", "err", terr)
	}
	defer func() {
		tctx, tcancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer tcancel()
		_ = traceShutdown(tctx)
	}()

	// Build shard engine.
	engine := shard.New(cfg.MCPrefix, cfg.MCGroupID, cfg.ShardBits)

	// BRC-148 BEEF plane engine (domain-tagged group derivation from
	// TopicIDs). Always constructed so cached V9 frames retransmit
	// correctly; the band is only JOINED when -beef-enabled is set.
	beefEngine, err := shard.NewPlane(cfg.MCPrefix, cfg.MCGroupID, cfg.BEEFShardBits, shard.DomainBEEF)
	if err != nil {
		return fmt.Errorf("beef plane: %w", err)
	}

	// Build the modular cache backend (memory | redis | aerospike). The
	// frame store and the cross-instance dedup gate share one backend via
	// independent key prefixes ("bre:frame:" and "bre:dedup:").
	frameBackend, err := scache.Open(context.Background(), scache.Config{
		Backend:       cfg.CacheBackend,
		MemoryMaxKeys: cfg.CacheMaxKeys,
		RedisAddr:     cfg.RedisAddr,
		AeroHosts:     cfg.AeroHosts,
		AeroNamespace: cfg.AeroNamespace,
		AeroSet:       cfg.AeroSet,
		DialTimeout:   cfg.CacheDialTimeout,
		OpTimeout:     cfg.CacheOpTimeout,
	})
	if err != nil {
		return fmt.Errorf("cache backend: %w", err)
	}
	defer func() { _ = frameBackend.Close() }()

	c := cache.NewStore(frameBackend, "bre:frame:", cfg.CacheOpTimeout)

	// Cross-instance retransmit dedup. A redis/aerospike frame backend is
	// itself cross-instance, so reuse it under the "bre:dedup:" prefix. With
	// CACHE_BACKEND=memory (per-instance frames) the dedup gate still needs a
	// shared store: build a separate redis backend from REDIS_ADDR if given.
	var dedup cache.Deduper
	var dedupB scache.Backend // backend used for cross-instance dedup; nil = none
	switch cfg.CacheBackend {
	case "redis", "aerospike":
		dedupB = frameBackend
		dedup = cache.NewStore(frameBackend, "bre:dedup:", cfg.CacheOpTimeout)
		slog.Info("cross-instance dedup enabled", "backend", cfg.CacheBackend)
	case "memory":
		if cfg.RedisAddr != "" {
			dedupBackend, derr := scache.Open(context.Background(), scache.Config{
				Backend:     scache.BackendRedis,
				RedisAddr:   cfg.RedisAddr,
				DialTimeout: cfg.CacheDialTimeout,
				OpTimeout:   cfg.CacheOpTimeout,
			})
			if derr != nil {
				slog.Warn("redis dedup unavailable, running without cross-instance dedup", "addr", cfg.RedisAddr, "err", derr)
			} else {
				defer func() { _ = dedupBackend.Close() }()
				dedupB = dedupBackend
				dedup = cache.NewStore(dedupBackend, "bre:dedup:", cfg.CacheOpTimeout)
				slog.Info("cross-instance dedup enabled", "addr", cfg.RedisAddr)
			}
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

	// Resolve NACK bind address. This is used to bind the NACK listening
	// socket so ACK/MISS responses are sourced from the correct address
	// (avoids kernel SLAAC source-address selection mismatch). It also sources
	// the unicast retransmit socket so frames returned to a requester match the
	// advertised address.
	nackBindIP, err := pickBeaconNACKAddr(cfg.BeaconNACKAddr, egressIfaces[0])
	if err != nil {
		return fmt.Errorf("resolve nack bind address: %w", err)
	}

	// Build retransmitter.
	retrans := retransmit.New(engine, egressIfaces, cfg.EgressPort, cfg.DedupWindow, dedup, rec, cfg.Debug)
	retrans.SetBEEF(beefEngine)
	retrans.SetUnicastSource(nackBindIP)
	retrans.SetMulticastLoop(cfg.EgressMulticastLoop)
	if err := retrans.Open(); err != nil {
		return err
	}
	defer func() { _ = retrans.Close() }()

	// Build server.
	srv := server.New(cfg.NACKPort, c, rl, rec, retrans, cfg.NACKWorkers, cfg.Debug)
	srv.SetBindAddr(nackBindIP.String())
	srv.SetSuppressACK(cfg.SuppressACK)
	srv.SetSuppressMISS(cfg.SuppressMISS)
	srv.SetThrottleResponse(cfg.ThrottleResponse)
	srv.SetShardEngine(engine)
	srv.SetBEEFEngine(beefEngine)
	srv.SetRetransmitModes(cfg.BeaconFlagsMulticast, cfg.BeaconFlagsUnicast)

	// NACK proxying: recover cache misses from an upstream retry-endpoint.
	// Constructed here (wired into the server before Run); workers started once
	// the root context exists, below.
	var proxyClient *proxy.Client
	if cfg.ProxyEnabled {
		var proxyDedup proxy.Deduper
		if dedupB != nil {
			proxyDedup = cache.NewStore(dedupB, "bre:proxy:", cfg.CacheOpTimeout)
		} else {
			slog.Warn("proxy in-flight dedup disabled: no shared cache backend; sibling endpoints may each proxy the same gap (use -cache-backend redis|aerospike)")
		}
		proxyClient = proxy.New(proxy.Config{
			Upstreams: cfg.UpstreamRetryEndpoints,
			// Same address the NACK socket binds and the unicast retransmit socket
			// sources from: the upstream must be able to reply to it, and fabric
			// firewalls only admit the retry-endpoint address set.
			SourceIP:     nackBindIP,
			Timeout:      cfg.ProxyTimeout,
			MaxEndpoints: cfg.ProxyMaxEndpoints,
			DedupWindow:  cfg.ProxyDedupWindow,
			Workers:      cfg.ProxyWorkers,
			QueueDepth:   cfg.ProxyQueue,
			Dedup:        proxyDedup,
			Cache:        c,
			Retrans:      retrans,
			TTLs: proxy.TTLConfig{
				Tx:      cfg.CacheTTLTx,
				Block:   cfg.CacheTTLBlock,
				Subtree: cfg.CacheTTLSubtree,
				Anchor:  cfg.CacheTTLAnchor,
				BEEF:    cfg.CacheTTLBEEF,
			},
			Rec: rec,
			// Same modes the direct cache-hit path honours. Before this the
			// recovery path multicast unconditionally, regardless of the beacon
			// flags, and a requester on another node never got its frame.
			Multicast: cfg.BeaconFlagsMulticast,
			Unicast:   cfg.BeaconFlagsUnicast,
		})
		srv.SetProxy(proxyClient)
	}

	// Build ingress worker.
	ing := ingress.New(mcIface, cfg.ListenPort, groups, c, rec, ingress.TTLConfig{
		Tx:      cfg.CacheTTLTx,
		Block:   cfg.CacheTTLBlock,
		Subtree: cfg.CacheTTLSubtree,
		Anchor:  cfg.CacheTTLAnchor,
		BEEF:    cfg.CacheTTLBEEF,
	}, cfg.Debug)

	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start NACK proxy workers (wired into the server above).
	if proxyClient != nil {
		proxyClient.Start(ctx)
		slog.Info("NACK proxying enabled", "upstreams", cfg.UpstreamRetryEndpoints, "workers", cfg.ProxyWorkers)
	}

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
		// Pre-create the per-source cache counters for every joined source
		// (metrics.PreCreateSources contract); the roster already excludes
		// this node's own source.
		uniq := map[string]struct{}{}
		for _, srcs := range gs {
			for _, s := range srcs {
				uniq[s.String()] = struct{}{}
			}
		}
		pre := make([]string, 0, len(uniq))
		for s := range uniq {
			pre = append(pre, s)
		}
		rec.PreCreateSources(pre)
	}

	var wg sync.WaitGroup

	// Start metrics server.
	wg.Add(1)
	go func() {
		defer wg.Done()
		rec.Serve(cfg.MetricsAddr, done)
	}()

	// Start cache size sampler (samples Len() every 15s). Len() is meaningful
	// only for the in-memory backend; remote backends report 0.
	if cfg.CacheBackend == scache.BackendMemory {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t := time.NewTicker(15 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					rec.CacheSize(int64(c.Len()))
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// Start ingress worker. Its death is fatal: without ingress the cache
	// never fills and every NACK answered from here is a miss, yet the
	// metrics/NACK/beacon goroutines would keep the process looking healthy.
	// Surface it as a non-zero exit so the supervisor restarts us and the
	// failure is visible where operators look.
	fatalCh := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := ing.Run(ctx); err != nil && ctx.Err() == nil {
			slog.Error("ingress exited with error", "err", err)
			select {
			case fatalCh <- fmt.Errorf("ingress: %w", err):
			default:
			}
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
		if cfg.ProxyEnabled {
			flags |= beacon.FlagHasParent
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

	// Wait for a signal or a fatal subsystem error.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	var fatalErr error
	select {
	case sig := <-sigCh:
		slog.Info("shutdown signal received", "signal", sig)
	case fatalErr = <-fatalCh:
		slog.Error("fatal subsystem error, shutting down", "err", fatalErr)
	}

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
	return fatalErr
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
		subtreeDataIP := shard.GroupAddr(cfg.MCPrefix, cfg.MCGroupID, shard.GroupSubtreeDataAnnounce)
		groups = append(groups, &net.UDPAddr{IP: subtreeDataIP, Port: cfg.ListenPort})
	}

	// Join the BRC-148 BEEF plane band when enabled so V9 frames are cached
	// for retransmission.
	if cfg.BEEFEnabled {
		for i := uint32(0); i < 1<<cfg.BEEFShardBits; i++ {
			groups = append(groups, engine.Addr(0x1000+i, cfg.ListenPort))
		}
	}

	return groups, nil
}

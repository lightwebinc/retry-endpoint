// Package config loads and validates runtime configuration for
// retry-endpoint. Parameters are accepted from CLI flags first;
// environment variables serve as fallbacks; hard-coded defaults apply when
// neither is present.
package config

import (
	"flag"
	"fmt"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/lightwebinc/shard-common/shard"
)

// splitCSV trims and returns the non-empty comma-separated tokens of s.
// Used by the SSM bootstrap-list flags.
func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Differentiated default TTLs per FrameVer. These mirror the typical
// retransmit-window expectations for each frame type and are overridden
// by either an explicit semantic flag/env (CACHE_TTL_TX, CACHE_TTL_BLOCK,
// CACHE_TTL_SUBTREE, CACHE_TTL_ANCHOR) or, as a fallback, by an explicit
// CACHE_TTL value (which collapses all four to the same TTL).
const (
	defaultCacheTTLTx      = 60 * time.Second // BRC-124/128 regular tx
	defaultCacheTTLBlock   = 10 * time.Minute // BRC-131 block control
	defaultCacheTTLSubtree = 5 * time.Minute  // BRC-132 subtree data
	defaultCacheTTLAnchor  = 2 * time.Minute  // BRC-134 anchor tx
)

// Scopes maps a human-readable scope name to the two-byte big-endian IPv6
// multicast prefix. See RFC 4291 §2.7.
var Scopes = map[string]uint16{
	"link":   0xFF02,
	"site":   0xFF05,
	"org":    0xFF08,
	"global": 0xFF0E,
}

// Config holds all runtime parameters for the retry endpoint.
type Config struct {
	// Ingress (multicast receive)
	MCIface    string // NIC for multicast ingress
	ListenPort int    // Multicast listen port
	ShardBits  uint   // Number of txid prefix bits used as the group key (1–15)
	NumGroups  uint32 // Derived: 1 << ShardBits
	MCScope    string // "site" | "global" — also accepts legacy "link"/"org" in ASM mode
	MCPrefix   uint16 // Derived from (SourceMode, MCScope) — upper 16 bits of the IPv6 group address
	MCGroupID  uint16 // IANA group-id occupying bytes 12–13 (default 0x000B)

	// SSM (RFC 4607)
	// SourceMode: "asm" (default) | "ssm"
	// BindSource: optional IPv6 literal for beacon emit; required when
	//   listeners pre-declare this retry-endpoint in sources.bootstrap.beacon
	// PublishersManifest: optional URL or local path for static manifest
	//   source-set bootstrap; defer to operator-supplied static list for now
	// SSMBootstrap{Beacon,Manifest,SubtreeAnnounce}: per-control-group
	//   bootstrap source lists (IPv6 literals or DNS names; resolved via
	//   the shared bootstrap.Resolver at startup and refreshed periodically).
	//   Used to (S,G)-join the matching control group.
	// PublishersStatic: lab/CI escape hatch — pre-declared data-plane
	//   publisher source list. Production must learn the data-plane source
	//   set from manifest discovery.
	SourceMode              string
	BindSource              string
	SSMBootstrapManifest    []string
	SSMBootstrapBeacon      []string
	SSMBootstrapSubtreeAnn  []string
	SSMPublishersStatic     []string
	SSMBootstrapRefresh     time.Duration

	// Cache
	CacheBackend string // "redis" or "memory"
	RedisAddr    string // Redis server address (e.g., "localhost:6379")
	CacheTTL     time.Duration
	CacheMaxKeys int // Maximum number of keys in cache (0 = no limit)

	// Per-FrameVer TTLs. Resolution order applied in Load():
	//   1. explicit semantic flag/env (e.g. CACHE_TTL_TX) — wins
	//   2. else, explicit CACHE_TTL — overrides differentiated default
	//   3. else, differentiated default below
	CacheTTLTx      time.Duration // FrameVer V2 (BRC-124/128 regular tx)
	CacheTTLBlock   time.Duration // FrameVer V4 (BRC-131 block control)
	CacheTTLSubtree time.Duration // FrameVer V5 (BRC-132 subtree data)
	CacheTTLAnchor  time.Duration // FrameVer V6 (BRC-134 anchor tx)

	// Server (NACK receive)
	NACKPort    int // NACK listen port (default 9300)
	NACKWorkers int // Worker goroutines for NACK processing

	// Retransmit
	EgressIfaces []string      // NIC names for multicast egress
	EgressPort   int           // Destination UDP port for retransmitted frames
	DedupWindow  time.Duration // Deduplication window (default 60s)

	// Rate limiting
	RLIPRate         float64       // IP rate limit (tokens per second)
	RLIPBurst        int           // IP burst size
	RLSenderRate     float64       // Alias for RLChainRate (backward-compat)
	RLSenderWindow   time.Duration // Alias for RLChainWindow (backward-compat)
	RLChainRate      float64       // Max NACKs per window per (srcIP, HashKey)
	RLChainWindow    time.Duration // Sliding window for per-HashKey limiter
	RLSequenceMax    int           // Max requests per SeqNum per SequenceWindow
	RLSequenceWindow time.Duration // SeqNum sliding window duration
	RLGroupRate      float64       // Retransmits per second per (srcIP, groupIdx)
	RLGroupBurst     int           // Burst size per (srcIP, groupIdx)

	// Runtime
	NumWorkers   int           // Worker goroutines for multicast ingress (always 1)
	Debug        bool          // Enable per-packet debug logging
	DrainTimeout time.Duration // Pre-drain delay before closing sockets

	// Observability
	MetricsAddr  string        // HTTP bind address for /metrics, /healthz, /readyz
	InstanceID   string        // OTel service.instance.id
	OTLPEndpoint string        // gRPC OTLP endpoint (empty = disabled)
	OTLPInterval time.Duration // OTLP push interval

	// Beacon (BRC-126 endpoint discovery)
	BeaconEnabled        bool
	BeaconTier           uint          // 0 = closest to source
	BeaconPreference     uint          // weighting within tier; higher = preferred
	BeaconInterval       time.Duration // ADVERT cadence
	BeaconScope          string        // "site" | "org" | "global" | "both" | "all"
	BeaconScopeByte      byte          // derived: 0x05 | 0x08 | 0x0E | 0xFF
	BeaconFlagsUnicast   bool
	BeaconFlagsMulticast bool
	BeaconFlagsDraining  bool
	BeaconNACKAddr       string // explicit IPv6 unicast NACK address for ADVERT; auto-detected if empty

	// BRC-132 subtree data
	SubtreeDataEnabled bool // join GroupSubtreeAnnounce (0xFFFB) for subtree data caching

	// Response suppression (BRC-126)
	SuppressACK  bool // do not emit ACK responses
	SuppressMISS bool // do not emit MISS responses
}

// Load parses flags and environment variables, validates all values, and
// returns a populated Config. It calls flag.Parse internally; callers
// must not call flag.Parse separately.
func Load() (*Config, error) {
	c := &Config{}

	flag.StringVar(&c.MCIface, "mc-iface", envStr("MC_IFACE", "eth0"),
		"NIC for multicast ingress")
	flag.IntVar(&c.ListenPort, "listen-port", envInt("LISTEN_PORT", 9001),
		"multicast listen port")

	flag.StringVar(&c.CacheBackend, "cache-backend", envStr("CACHE_BACKEND", "memory"),
		"cache backend: redis | memory")
	flag.StringVar(&c.RedisAddr, "redis-addr", envStr("REDIS_ADDR", ""),
		"Redis server address (required when cache-backend=redis; also enables cross-instance dedup when cache-backend=memory)")
	flag.DurationVar(&c.CacheTTL, "cache-ttl", envDuration("CACHE_TTL", 60*time.Second),
		"cache TTL for frames (fallback default for any per-FrameVer TTL not explicitly set)")
	flag.DurationVar(&c.CacheTTLTx, "cache-ttl-tx", envDuration("CACHE_TTL_TX", defaultCacheTTLTx),
		"cache TTL for regular tx frames (BRC-124/128, FrameVer V2)")
	flag.DurationVar(&c.CacheTTLBlock, "cache-ttl-block", envDuration("CACHE_TTL_BLOCK", defaultCacheTTLBlock),
		"cache TTL for block control frames (BRC-131, FrameVer V4)")
	flag.DurationVar(&c.CacheTTLSubtree, "cache-ttl-subtree", envDuration("CACHE_TTL_SUBTREE", defaultCacheTTLSubtree),
		"cache TTL for subtree data frames (BRC-132, FrameVer V5)")
	flag.DurationVar(&c.CacheTTLAnchor, "cache-ttl-anchor", envDuration("CACHE_TTL_ANCHOR", defaultCacheTTLAnchor),
		"cache TTL for anchor tx frames (BRC-134, FrameVer V6)")
	flag.IntVar(&c.CacheMaxKeys, "cache-max-keys", envInt("CACHE_MAX_KEYS", 0),
		"maximum number of keys in cache (0 = no limit)")

	flag.IntVar(&c.NACKPort, "nack-port", envInt("NACK_PORT", 9300),
		"NACK listen port")
	flag.IntVar(&c.NACKWorkers, "nack-workers", envInt("NACK_WORKERS", runtime.NumCPU()),
		"NACK worker goroutines")

	egressFlag := flag.String("egress-iface", envStr("EGRESS_IFACE", "eth0"),
		"comma-separated NIC names for multicast egress")
	flag.IntVar(&c.EgressPort, "egress-port", envInt("EGRESS_PORT", 9001),
		"destination UDP port for retransmitted frames")
	flag.DurationVar(&c.DedupWindow, "dedup-window", envDuration("DEDUP_WINDOW", 60*time.Second),
		"retransmission deduplication window")

	flag.Float64Var(&c.RLIPRate, "rl-ip-rate", envFloat("RL_IP_RATE", 100),
		"IP rate limit (tokens per second)")
	flag.IntVar(&c.RLIPBurst, "rl-ip-burst", envInt("RL_IP_BURST", 10),
		"IP rate limit burst size")
	flag.Float64Var(&c.RLSenderRate, "rl-sender-rate", envFloat("RL_SENDER_RATE", 0),
		"alias for rl-chain-rate (backward-compat)")
	flag.DurationVar(&c.RLSenderWindow, "rl-sender-window", envDuration("RL_SENDER_WINDOW", 0),
		"alias for rl-chain-window (backward-compat)")
	flag.Float64Var(&c.RLChainRate, "rl-chain-rate", envFloat("RL_CHAIN_RATE", 500),
		"max NACKs per window per (srcIP, HashKey)")
	flag.DurationVar(&c.RLChainWindow, "rl-chain-window", envDuration("RL_CHAIN_WINDOW", time.Minute),
		"sliding window for per-chain NACK limiter")
	flag.IntVar(&c.RLSequenceMax, "rl-sequence-max", envInt("RL_SEQUENCE_MAX", 100),
		"max requests per SequenceID per sliding window")
	flag.DurationVar(&c.RLSequenceWindow, "rl-sequence-window", envDuration("RL_SEQUENCE_WINDOW", time.Minute),
		"SequenceID sliding window duration")
	flag.Float64Var(&c.RLGroupRate, "rl-group-rate", envFloat("RL_GROUP_RATE", 200),
		"retransmits per second per (srcIP, groupIdx)")
	flag.IntVar(&c.RLGroupBurst, "rl-group-burst", envInt("RL_GROUP_BURST", 50),
		"burst size per (srcIP, groupIdx) for group retransmit limiter")

	flag.StringVar(&c.MCScope, "scope", envStr("MC_SCOPE", "site"),
		"multicast scope: link | site | org | global (site|global also accepted in SSM mode)")
	groupIDFlag := flag.String("mc-group-id", envStr("MC_GROUP_ID", "0x000B"),
		"IANA group-id (bytes 12–13 of the IPv6 multicast address); default 0x000B (IANA Bitcoin)")
	flag.StringVar(&c.SourceMode, "source-mode", envStr("SOURCE_MODE", "asm"),
		"multicast addressing model: asm | ssm")
	flag.StringVar(&c.BindSource, "bind-source", envStr("BIND_SOURCE", ""),
		"optional IPv6 literal to bind for beacon egress (so SSM listeners can pre-declare this retry-endpoint as a beacon source)")
	ssmBootstrapManifest := flag.String("ssm-bootstrap-manifest", envStr("SSM_BOOTSTRAP_MANIFEST", ""),
		"CSV of shard-manifest sources (IPv6 literals or DNS names); used to (S,G)-join the manifest group in Posture C")
	ssmBootstrapBeacon := flag.String("ssm-bootstrap-beacon", envStr("SSM_BOOTSTRAP_BEACON", ""),
		"CSV of retry-endpoint sources used to (S,G)-join the beacon group (typically a headless-Service name fronting retry-endpoint pods)")
	ssmBootstrapSubtreeAnn := flag.String("ssm-bootstrap-subtree-announce", envStr("SSM_BOOTSTRAP_SUBTREE_ANNOUNCE", ""),
		"CSV of subtree-announce emitter sources for SSM join of that control group")
	ssmPublishersStatic := flag.String("ssm-publishers-static", envStr("SSM_PUBLISHERS_STATIC", ""),
		"lab/CI escape hatch: CSV of data-plane publisher IPv6 sources (production uses manifest discovery)")
	flag.DurationVar(&c.SSMBootstrapRefresh, "ssm-bootstrap-refresh", envDuration("SSM_BOOTSTRAP_REFRESH", 30*time.Second),
		"DNS re-resolve interval for SSM bootstrap entries")

	flag.BoolVar(&c.SubtreeDataEnabled, "subtree-data-enabled", envBool("SUBTREE_DATA_ENABLED", false),
		"enable BRC-132 subtree data caching: join GroupSubtreeAnnounce (0xFFFB) group")
	flag.BoolVar(&c.Debug, "debug", envBool("DEBUG", false),
		"enable per-packet debug logging")
	flag.DurationVar(&c.DrainTimeout, "drain-timeout", envDuration("DRAIN_TIMEOUT", 0),
		"pre-drain delay before closing sockets")

	flag.StringVar(&c.MetricsAddr, "metrics-addr", envStr("METRICS_ADDR", ":9400"),
		"HTTP bind address for /metrics, /healthz, /readyz")
	flag.StringVar(&c.InstanceID, "instance", envStr("INSTANCE_ID", ""),
		"OTel service.instance.id (default: hostname)")
	flag.StringVar(&c.OTLPEndpoint, "otlp-endpoint", envStr("OTLP_ENDPOINT", ""),
		"OTLP gRPC endpoint for metric push (empty = disabled)")
	otlpInterval := flag.Duration("otlp-interval", envDuration("OTLP_INTERVAL", 30*time.Second),
		"OTLP push interval")

	shardBitsDefault := uint(envInt("SHARD_BITS", 2))
	bits := flag.Uint("shard-bits", shardBitsDefault,
		"txid prefix bit width used as the shard key (1–15)")

	// Beacon flags.
	flag.BoolVar(&c.BeaconEnabled, "beacon-enabled", envBool("BEACON_ENABLED", true),
		"enable ADVERT beacon multicasting")
	beaconTier := flag.Uint("beacon-tier", uint(envInt("BEACON_TIER", 0)),
		"beacon tier (0 = closest to source)")
	beaconPref := flag.Uint("beacon-preference", uint(envInt("BEACON_PREFERENCE", 128)),
		"beacon preference within tier (higher = preferred)")
	flag.DurationVar(&c.BeaconInterval, "beacon-interval", envDuration("BEACON_INTERVAL", 60*time.Second),
		"beacon multicast interval")
	flag.StringVar(&c.BeaconScope, "beacon-scope", envStr("BEACON_SCOPE", "site"),
		"beacon scope: link | site | org | global | both | all")
	flag.BoolVar(&c.BeaconFlagsUnicast, "beacon-flags-unicast", envBool("BEACON_FLAGS_UNICAST", false),
		"advertise unicast retransmit support")
	flag.BoolVar(&c.BeaconFlagsMulticast, "beacon-flags-multicast", envBool("BEACON_FLAGS_MULTICAST", true),
		"advertise multicast retransmit support")
	flag.BoolVar(&c.BeaconFlagsDraining, "beacon-flags-draining", envBool("BEACON_FLAGS_DRAINING", false),
		"advertise draining status (listeners will not add this endpoint)")
	flag.StringVar(&c.BeaconNACKAddr, "nack-addr", envStr("NACK_ADDR", ""),
		"explicit IPv6 unicast address for ADVERT (auto-detected from egress iface if empty)")

	// Response suppression flags.
	flag.BoolVar(&c.SuppressACK, "suppress-ack", envBool("SUPPRESS_ACK", false),
		"suppress ACK responses (listeners fall back to timeout + backoff)")
	flag.BoolVar(&c.SuppressMISS, "suppress-miss", envBool("SUPPRESS_MISS", false),
		"suppress MISS responses")

	flag.Parse()

	// Resolve per-FrameVer cache TTLs.
	//
	// Detect explicit-ness via env presence (already applied to defaults
	// above) plus flag.Visit (CLI overrides). For any per-FrameVer TTL not
	// explicitly set, fall back to CACHE_TTL when the operator set it
	// explicitly; otherwise keep the differentiated default.
	cacheTTLExplicit := os.Getenv("CACHE_TTL") != ""
	ttlTxExplicit := os.Getenv("CACHE_TTL_TX") != ""
	ttlBlockExplicit := os.Getenv("CACHE_TTL_BLOCK") != ""
	ttlSubtreeExplicit := os.Getenv("CACHE_TTL_SUBTREE") != ""
	ttlAnchorExplicit := os.Getenv("CACHE_TTL_ANCHOR") != ""
	flag.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "cache-ttl":
			cacheTTLExplicit = true
		case "cache-ttl-tx":
			ttlTxExplicit = true
		case "cache-ttl-block":
			ttlBlockExplicit = true
		case "cache-ttl-subtree":
			ttlSubtreeExplicit = true
		case "cache-ttl-anchor":
			ttlAnchorExplicit = true
		}
	})
	resolveCacheTTLs(c, cacheTTLExplicit, ttlTxExplicit, ttlBlockExplicit, ttlSubtreeExplicit, ttlAnchorExplicit)

	c.BeaconTier = *beaconTier
	c.BeaconPreference = *beaconPref

	// Validate shard bit width. Top of the 16-bit shard space is reserved for
	// control-plane groups (0xFFFC–0xFFFE), so practical bits is bounded at 15.
	if *bits < 1 || *bits > 15 {
		return nil, fmt.Errorf("shard-bits must be in [1, 15], got %d", *bits)
	}
	c.ShardBits = *bits
	c.NumGroups = 1 << c.ShardBits
	c.OTLPInterval = *otlpInterval

	// Resolve multicast scope + source-mode → upper-16-bit prefix.
	switch strings.ToLower(c.SourceMode) {
	case "asm":
		c.SourceMode = "asm"
		prefix, ok := Scopes[c.MCScope]
		if !ok {
			return nil, fmt.Errorf("unknown scope %q; valid values: link, site, org, global", c.MCScope)
		}
		c.MCPrefix = prefix
	case "ssm":
		c.SourceMode = "ssm"
		scope, err := shard.ParseScope(c.MCScope)
		if err != nil {
			return nil, fmt.Errorf("source-mode=ssm requires -scope site|global: %w", err)
		}
		prefix, err := shard.Prefix(shard.SourceModeSSM, scope)
		if err != nil {
			return nil, err
		}
		c.MCPrefix = prefix
	default:
		return nil, fmt.Errorf("invalid source-mode %q (asm|ssm)", c.SourceMode)
	}

	c.SSMBootstrapManifest = splitCSV(*ssmBootstrapManifest)
	c.SSMBootstrapBeacon = splitCSV(*ssmBootstrapBeacon)
	c.SSMBootstrapSubtreeAnn = splitCSV(*ssmBootstrapSubtreeAnn)
	c.SSMPublishersStatic = splitCSV(*ssmPublishersStatic)
	if c.SSMBootstrapRefresh <= 0 {
		return nil, fmt.Errorf("ssm-bootstrap-refresh must be > 0")
	}
	if c.SourceMode == "ssm" {
		if c.BindSource != "" {
			ip := net.ParseIP(c.BindSource)
			if ip == nil || ip.To4() != nil {
				return nil, fmt.Errorf("invalid -bind-source %q: must be an IPv6 literal", c.BindSource)
			}
		}
		// Posture C requires at least one bootstrap list OR a static
		// data-plane source list (lab/CI). Fail closed otherwise so
		// operators don't silently get ASM fallback.
		if len(c.SSMBootstrapManifest) == 0 &&
			len(c.SSMBootstrapBeacon) == 0 &&
			len(c.SSMBootstrapSubtreeAnn) == 0 &&
			len(c.SSMPublishersStatic) == 0 {
			return nil, fmt.Errorf("source-mode=ssm requires at least one of -ssm-bootstrap-manifest/-beacon/-subtree-announce or -ssm-publishers-static")
		}
		if len(c.SSMPublishersStatic) > 16 && len(c.SSMBootstrapManifest) == 0 {
			return nil, fmt.Errorf("ssm-publishers-static has %d entries; production at this size must use manifest discovery (set -ssm-bootstrap-manifest)", len(c.SSMPublishersStatic))
		}
	}

	// Parse IANA group-id (default 0x000B = IANA Bitcoin allocation).
	gid, err := parseGroupID(*groupIDFlag)
	if err != nil {
		return nil, fmt.Errorf("invalid -mc-group-id %q: %w", *groupIDFlag, err)
	}
	c.MCGroupID = gid

	// Validate cache backend.
	if c.CacheBackend != "redis" && c.CacheBackend != "memory" {
		return nil, fmt.Errorf("cache-backend must be 'redis' or 'memory', got %q", c.CacheBackend)
	}

	// Validate per-FrameVer TTLs.
	for name, ttl := range map[string]time.Duration{
		"cache-ttl-tx":      c.CacheTTLTx,
		"cache-ttl-block":   c.CacheTTLBlock,
		"cache-ttl-subtree": c.CacheTTLSubtree,
		"cache-ttl-anchor":  c.CacheTTLAnchor,
	} {
		if ttl <= 0 {
			return nil, fmt.Errorf("%s must be > 0, got %s", name, ttl)
		}
	}

	// Ingress is always single worker (SO_REUSEPORT multicast duplication).
	c.NumWorkers = 1

	// Default NACK workers to NumCPU if set to zero.
	if c.NACKWorkers <= 0 {
		c.NACKWorkers = runtime.NumCPU()
	}

	// Parse and validate egress interfaces.
	for _, name := range strings.Split(*egressFlag, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, err := net.InterfaceByName(name); err != nil {
			return nil, fmt.Errorf("egress interface %q not found: %w", name, err)
		}
		c.EgressIfaces = append(c.EgressIfaces, name)
	}
	if len(c.EgressIfaces) == 0 {
		return nil, fmt.Errorf("at least one egress interface must be specified via -egress-iface")
	}

	// Validate beacon scope and tier/preference ranges.
	switch c.BeaconScope {
	case "site":
		c.BeaconScopeByte = 0x05
	case "org":
		c.BeaconScopeByte = 0x08
	case "global":
		c.BeaconScopeByte = 0x0E
	case "both", "all":
		c.BeaconScopeByte = 0xFF
	default:
		return nil, fmt.Errorf("beacon-scope must be one of link|site|org|global|both|all, got %q", c.BeaconScope)
	}
	if c.BeaconTier > 255 {
		return nil, fmt.Errorf("beacon-tier must fit in uint8, got %d", c.BeaconTier)
	}
	if c.BeaconPreference > 255 {
		return nil, fmt.Errorf("beacon-preference must fit in uint8, got %d", c.BeaconPreference)
	}
	if c.BeaconInterval < time.Second {
		return nil, fmt.Errorf("beacon-interval must be ≥ 1s (advert carries an integer seconds field), got %s", c.BeaconInterval)
	}
	if c.BeaconNACKAddr != "" {
		ip := net.ParseIP(c.BeaconNACKAddr)
		if ip == nil || ip.To16() == nil || ip.To4() != nil {
			return nil, fmt.Errorf("nack-addr must be a valid IPv6 unicast address, got %q", c.BeaconNACKAddr)
		}
	}

	return c, nil
}

// resolveCacheTTLs applies the per-FrameVer TTL fallback rule:
// for any per-type TTL that was not explicitly set by the operator,
// substitute CacheTTL when the operator set it explicitly. The
// differentiated defaults from envDuration remain in place when neither
// the per-type knob nor CACHE_TTL was set.
func resolveCacheTTLs(c *Config, cacheTTLExplicit, txExplicit, blockExplicit, subtreeExplicit, anchorExplicit bool) {
	if !cacheTTLExplicit {
		return
	}
	if !txExplicit {
		c.CacheTTLTx = c.CacheTTL
	}
	if !blockExplicit {
		c.CacheTTLBlock = c.CacheTTL
	}
	if !subtreeExplicit {
		c.CacheTTLSubtree = c.CacheTTL
	}
	if !anchorExplicit {
		c.CacheTTLAnchor = c.CacheTTL
	}
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

// parseGroupID accepts either a hex literal (0x000B, 000B) or a decimal
// integer in the range [0, 0xFFFF].
func parseGroupID(s string) (uint16, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty value")
	}
	base := 10
	low := strings.ToLower(s)
	if strings.HasPrefix(low, "0x") {
		s = s[2:]
		base = 16
	} else if _, err := strconv.ParseUint(s, 10, 16); err != nil {
		base = 16
	}
	n, err := strconv.ParseUint(s, base, 16)
	if err != nil {
		return 0, err
	}
	return uint16(n), nil
}

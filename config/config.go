// Package config loads and validates runtime configuration for
// bitcoin-retry-endpoint. Parameters are accepted from CLI flags first;
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
	MCScope    string // Human name; one of the keys in Scopes
	MCPrefix   uint16 // Derived from MCScope — upper 16 bits of the IPv6 group address
	MCGroupID  uint16 // IANA group-id occupying bytes 12–13 (default 0x000B)

	// Cache
	CacheBackend string // "redis" or "memory"
	RedisAddr    string // Redis server address (e.g., "localhost:6379")
	CacheTTL     time.Duration
	CacheMaxKeys int // Maximum number of keys in cache (0 = no limit)

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
	RLChainRate      float64       // Max NACKs per window per (srcIP, chainID)
	RLChainWindow    time.Duration // Sliding window for per-chain limiter
	RLSequenceMax    int           // Max requests per SequenceID per SequenceWindow
	RLSequenceWindow time.Duration // SequenceID sliding window duration
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
		"cache TTL for frames")
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
		"max NACKs per window per (srcIP, chainID)")
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
		"multicast scope: link | site | org | global")
	groupIDFlag := flag.String("mc-group-id", envStr("MC_GROUP_ID", "0x000B"),
		"IANA group-id (bytes 12–13 of the IPv6 multicast address); default 0x000B (IANA Bitcoin)")

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

	shardBitsDefault := uint(envInt("SHARD_BITS", 8))
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

	// Resolve multicast scope.
	prefix, ok := Scopes[c.MCScope]
	if !ok {
		return nil, fmt.Errorf("unknown scope %q; valid values: link, site, org, global", c.MCScope)
	}
	c.MCPrefix = prefix

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

package metrics

import (
	"log/slog"
	"net/netip"
	"strconv"

	"github.com/lightwebinc/shard-common/hostinfo"
	promclient "github.com/prometheus/client_golang/prometheus"
)

// SetLevelVar registers the runtime log-level variable so [Recorder.Serve]
// exposes a /loglevel endpoint for runtime level change.
func (r *Recorder) SetLevelVar(lvl *slog.LevelVar) { r.levelVar = lvl }

// SetHostInfo publishes a slim bre_host_info gauge (value 1) carrying
// low-cardinality host facts as labels, joining the host.inventory log event
// emitted at startup. Best-effort; registration errors are ignored.
func (r *Recorder) SetHostInfo(inv hostinfo.Inventory) {
	var nic, speed string
	for _, ifc := range inv.Interfaces {
		if ifc.OperState == "up" && (len(ifc.IPv4) > 0 || len(ifc.IPv6) > 0) {
			nic = ifc.Name
			if ifc.SpeedMbps > 0 {
				speed = strconv.Itoa(ifc.SpeedMbps)
			}
			break
		}
	}
	g := promclient.NewGaugeVec(promclient.GaugeOpts{
		Name: "bre_host_info",
		Help: "Static host facts (value always 1); join with host.inventory log on service_instance_id.",
	}, []string{
		"hostname", "kernel_version", "cpu_logical", "mem_bytes",
		"rmem_max", "nic", "speed_mbps", "version",
	})
	if err := r.promOtel.Register(g); err != nil {
		return
	}
	g.WithLabelValues(
		inv.Hostname,
		inv.KernelVersion,
		strconv.Itoa(inv.CPULogical),
		strconv.FormatUint(inv.MemTotalBytes, 10),
		inv.Sysctl["net.core.rmem_max"],
		nic,
		speed,
		inv.Version,
	).Set(1)
}

// SetOwnSource publishes bre_own_source_info (value 1) carrying this node's
// own fabric source address. A node never joins its own source — see
// excludeOwnSource — so it structurally cannot cache its own frames, and the
// (site, source) pair naming itself must never be read as starvation.
//
// The pair cannot simply be assumed absent: [Recorder.PreCreateSources] skips
// it, but [Recorder.FrameCached] creates the series on the data path with no
// roster check, so a single own-source frame leaked in by a co-located
// listener's wildcard socket (IPV6_MULTICAST_ALL is on by default) births the
// pair permanently at a frozen count. This gauge is what lets the
// RetryCacheSourceStarved rule subtract that pair — and only that pair — with
// `unless on(site, source)`. Best-effort; registration errors are ignored.
func (r *Recorder) SetOwnSource(src string) {
	// Canonicalise here rather than at the call site: the data path labels
	// bre_cache_stored_total with netip.Addr.String(), and the two spellings
	// must be byte-identical or the rule's join silently matches nothing.
	addr, err := netip.ParseAddr(src)
	if err != nil {
		return
	}
	g := promclient.NewGaugeVec(promclient.GaugeOpts{
		Name: "bre_own_source_info",
		Help: "This node's own fabric source address (value always 1); it never joins this source, so it cannot cache its own frames.",
	}, []string{"source"})
	if err := r.promOtel.Register(g); err != nil {
		return
	}
	g.WithLabelValues(addr.String()).Set(1)
}

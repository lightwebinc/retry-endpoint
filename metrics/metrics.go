// Package metrics initialises an OpenTelemetry MeterProvider backed by both
// a Prometheus exporter (for scraping) and an optional OTLP gRPC exporter
// (for push-based delivery to any OTel-compatible backend).
package metrics

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	prometheusexporter "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"

	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const ServiceName = "bitcoin-retry-endpoint"

var Version = "dev"

type Recorder struct {
	provider   *sdkmetric.MeterProvider
	promReg    promclient.Gatherer
	numWorkers int
	startTime  time.Time
	readyCount atomic.Int32
	draining   atomic.Bool
	shutdownFn func(context.Context) error

	// Cache metrics
	cacheHits   metric.Int64Counter
	cacheMisses metric.Int64Counter
	cacheSize   metric.Int64Gauge
	cacheErrors metric.Int64Counter

	// Server metrics
	nackRequests       metric.Int64Counter
	retransmits        metric.Int64Counter
	retransmitDedup    metric.Int64Counter
	responsesSent      metric.Int64Counter // labelled type={ack,miss}
	responseSendErrors metric.Int64Counter

	// Rate limit metrics
	rateLimitDrops metric.Int64Counter

	// Ingress metrics
	framesReceived metric.Int64Counter
	framesCached   metric.Int64Counter
	framesDropped  metric.Int64Counter
}

func New(instanceID string, numWorkers int, otlpEndpoint string, otlpInterval time.Duration) (*Recorder, error) {
	if instanceID == "" {
		h, err := os.Hostname()
		if err != nil {
			h = "unknown"
		}
		instanceID = h
	}

	res, err := resource.New(context.Background(),
		resource.WithAttributes(
			attribute.String("service.name", ServiceName),
			attribute.String("service.instance.id", instanceID),
			attribute.String("service.version", Version),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("metrics: build resource: %w", err)
	}

	reg := promclient.NewRegistry()
	promExp, err := prometheusexporter.New(prometheusexporter.WithRegisterer(reg))
	if err != nil {
		return nil, fmt.Errorf("metrics: prometheus exporter: %w", err)
	}

	runtimeReg := promclient.NewRegistry()
	runtimeReg.MustRegister(collectors.NewGoCollector())
	runtimeReg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	mpOpts := []sdkmetric.Option{
		sdkmetric.WithReader(promExp),
		sdkmetric.WithResource(res),
	}

	var shutdownFuncs []func(context.Context) error

	if otlpEndpoint != "" {
		otlpExp, oerr := otlpmetricgrpc.New(
			context.Background(),
			otlpmetricgrpc.WithEndpoint(otlpEndpoint),
			otlpmetricgrpc.WithInsecure(),
		)
		if oerr != nil {
			return nil, fmt.Errorf("metrics: OTLP exporter: %w", oerr)
		}
		mpOpts = append(mpOpts, sdkmetric.WithReader(
			sdkmetric.NewPeriodicReader(otlpExp, sdkmetric.WithInterval(otlpInterval)),
		))
		shutdownFuncs = append(shutdownFuncs, otlpExp.Shutdown)
		slog.Info("OTLP exporter enabled", "endpoint", otlpEndpoint, "interval", otlpInterval)
	}

	mp := sdkmetric.NewMeterProvider(mpOpts...)
	shutdownFuncs = append(shutdownFuncs, mp.Shutdown)

	r := &Recorder{
		provider:   mp,
		promReg:    promclient.Gatherers{reg, runtimeReg},
		numWorkers: numWorkers,
		startTime:  time.Now(),
		shutdownFn: func(ctx context.Context) error {
			var last error
			for _, fn := range shutdownFuncs {
				if err := fn(ctx); err != nil {
					last = err
				}
			}
			return last
		},
	}

	meter := mp.Meter(ServiceName)

	if r.cacheHits, err = meter.Int64Counter("bre_cache_hits_total",
		metric.WithDescription("Cache hits")); err != nil {
		return nil, err
	}
	if r.cacheMisses, err = meter.Int64Counter("bre_cache_misses_total",
		metric.WithDescription("Cache misses")); err != nil {
		return nil, err
	}
	if r.cacheSize, err = meter.Int64Gauge("bre_cache_size",
		metric.WithDescription("Current cache size")); err != nil {
		return nil, err
	}
	if r.cacheErrors, err = meter.Int64Counter("bre_cache_errors_total",
		metric.WithDescription("Cache errors")); err != nil {
		return nil, err
	}

	if r.nackRequests, err = meter.Int64Counter("bre_nack_requests_total",
		metric.WithDescription("NACK requests received")); err != nil {
		return nil, err
	}
	if r.retransmits, err = meter.Int64Counter("bre_retransmits_total",
		metric.WithDescription("Frames retransmitted")); err != nil {
		return nil, err
	}
	if r.retransmitDedup, err = meter.Int64Counter("bre_retransmit_dedup_total",
		metric.WithDescription("Retransmissions dropped due to deduplication")); err != nil {
		return nil, err
	}
	if r.responsesSent, err = meter.Int64Counter("bre_responses_sent_total",
		metric.WithDescription("ACK/MISS responses successfully written to the NACK socket")); err != nil {
		return nil, err
	}
	if r.responseSendErrors, err = meter.Int64Counter("bre_response_send_errors_total",
		metric.WithDescription("ACK/MISS responses that failed to send (WriteTo error)")); err != nil {
		return nil, err
	}

	if r.rateLimitDrops, err = meter.Int64Counter("bre_rate_limit_drops_total",
		metric.WithDescription("Requests dropped due to rate limiting")); err != nil {
		return nil, err
	}

	if r.framesReceived, err = meter.Int64Counter("bre_frames_received_total",
		metric.WithDescription("Multicast frames received")); err != nil {
		return nil, err
	}
	if r.framesCached, err = meter.Int64Counter("bre_frames_cached_total",
		metric.WithDescription("Frames cached")); err != nil {
		return nil, err
	}
	if r.framesDropped, err = meter.Int64Counter("bre_frames_dropped_total",
		metric.WithDescription("Frames dropped")); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *Recorder) CacheHit() {
	r.cacheHits.Add(context.Background(), 1)
}

func (r *Recorder) CacheMiss() {
	r.cacheMisses.Add(context.Background(), 1)
}

func (r *Recorder) CacheSize(size int64) {
	r.cacheSize.Record(context.Background(), size)
}

func (r *Recorder) CacheError() {
	r.cacheErrors.Add(context.Background(), 1)
}

func (r *Recorder) NACKRequest() {
	r.nackRequests.Add(context.Background(), 1)
}

func (r *Recorder) Retransmit() {
	r.retransmits.Add(context.Background(), 1)
}

func (r *Recorder) RetransmitDedup() {
	r.retransmitDedup.Add(context.Background(), 1)
}

// ResponseSent records an ACK or MISS datagram successfully written to the
// NACK socket. typ must be "ack" or "miss".
func (r *Recorder) ResponseSent(typ string) {
	r.responsesSent.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("type", typ),
	))
}

// ResponseSendError records a failed WriteTo for an ACK or MISS response.
func (r *Recorder) ResponseSendError(typ string) {
	r.responseSendErrors.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("type", typ),
	))
}

func (r *Recorder) RateLimitDrop(level string) {
	r.rateLimitDrops.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("level", level),
	))
}

func (r *Recorder) FrameReceived() {
	r.framesReceived.Add(context.Background(), 1)
}

func (r *Recorder) FrameCached() {
	r.framesCached.Add(context.Background(), 1)
}

func (r *Recorder) FrameDropped(reason string) {
	r.framesDropped.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("reason", reason),
	))
}

func (r *Recorder) WorkerReady() {
	r.readyCount.Add(1)
}

func (r *Recorder) WorkerDone() {
	r.readyCount.Add(-1)
}

func (r *Recorder) SetDraining() {
	r.draining.Store(true)
}

func (r *Recorder) Shutdown(ctx context.Context) {
	if err := r.shutdownFn(ctx); err != nil {
		slog.Warn("metrics shutdown error", "err", err)
	}
}

func (r *Recorder) Serve(addr string, done <-chan struct{}) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(r.promReg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", r.handleHealthz)
	mux.HandleFunc("/readyz", r.handleReadyz)

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		slog.Info("metrics server listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server error", "err", err)
		}
	}()
	<-done
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		slog.Warn("metrics server shutdown error", "err", err)
	}
}

func (r *Recorder) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(w, `{"status":"ok","uptime_seconds":%.1f}`, time.Since(r.startTime).Seconds())
}

func (r *Recorder) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	ready := int(r.readyCount.Load())
	total := r.numWorkers
	w.Header().Set("Content-Type", "application/json")
	if r.draining.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprintf(w, `{"status":"draining","workers_ready":%d,"workers_total":%d}`, ready, total)
		return
	}
	if ready >= total && total > 0 {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, `{"status":"ready","workers_ready":%d,"workers_total":%d}`, ready, total)
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = fmt.Fprintf(w, `{"status":"starting","workers_ready":%d,"workers_total":%d}`, ready, total)
}

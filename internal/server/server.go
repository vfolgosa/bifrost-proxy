// Package server provides the HTTP observability server for the Kafka L7 proxy.
//
// It exposes three endpoints on the configured metrics_port (default 8080):
//
//	/health  — Liveness check, returns 200 OK when the proxy process is healthy.
//	/metrics — Prometheus text-format metrics (connection counts, health check
//	           status, circuit breaker state, build info).
//	/status  — JSON snapshot of per-BU health, effective weights, drain state,
//	           and failover state summary.
//
// The server is designed to run alongside the main proxy listener. All handlers
// acquire locks only briefly to take consistent snapshots.
package server

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/vfolgosa/bifrost-proxy/internal/config"
	"github.com/vfolgosa/bifrost-proxy/internal/logger"
	"github.com/vfolgosa/bifrost-proxy/internal/routing"
)

//go:embed dashboard.html
var dashboardHTML []byte

// ── HealthProvider ──────────────────────────────────────────────────────

// HealthProvider exposes per-cluster, per-endpoint health state for
// /metrics and /status endpoints.
type HealthProvider interface {
	Health() map[string]ClusterHealthSnapshot
}

// ClusterHealthSnapshot is a lightweight view of one cluster's health state,
// suitable for JSON serialization and Prometheus metrics.
type ClusterHealthSnapshot struct {
	Name       string         `json:"name"`
	Primary   EndpointHealth `json:"primary"`
	Secondary EndpointHealth `json:"secondary"`
}

// EndpointHealth captures the current health of a single cluster endpoint.
type EndpointHealth struct {
	Healthy              bool          `json:"healthy"`
	ConsecutiveFailures  int           `json:"consecutive_failures"`
	ConsecutiveSuccesses int           `json:"consecutive_successes"`
	LastCheckLatency     time.Duration `json:"last_check_latency_ns"`
	LastStatus           string        `json:"last_status"`
	LastError            string        `json:"last_error,omitempty"`
	UpSince              time.Time     `json:"up_since,omitempty"`
	Bootstrap            string        `json:"bootstrap"`
}

// ── DrainStatusProvider ─────────────────────────────────────────────────

// DrainStatusProvider exposes drain and connection state.
type DrainStatusProvider interface {
	IsDraining(clusterName string) bool
	ActiveConnections(clusterName string) int64
	TotalActiveConnections() int64
}

// ── ConfigProvider ──────────────────────────────────────────────────────

// ConfigProvider exposes the current proxy configuration.
type ConfigProvider interface {
	Config() *config.Config
}

// WeightProvider exposes effective load_balance weights after auto-rebalance.
type WeightProvider interface {
	EffectiveWeights(clusterName string) (primary, secondary int, ok bool)
}

// FailoverStateProvider exposes the effective active cluster from the DR
// state machine for active_passive clusters.
type FailoverStateProvider interface {
	EffectiveActive(clusterName string) (active string, ok bool)
}

// ── FailoverMetrics ─────────────────────────────────────────────────────

// FailoverMetrics tracks failover and circuit breaker counters for
// Prometheus exposition. The failover controller calls RecordFailover
// and RecordCircuitBreaker when events occur.
type FailoverMetrics struct {
	mu             sync.RWMutex
	totalFailovers atomic.Int64
	circuitBreaker map[string]string // bu → state ("broken" / "ok")
}

// NewFailoverMetrics creates a new FailoverMetrics tracker.
func NewFailoverMetrics() *FailoverMetrics {
	return &FailoverMetrics{
		circuitBreaker: make(map[string]string),
	}
}

// RecordFailover increments the total failover counter.
func (fm *FailoverMetrics) RecordFailover() {
	fm.totalFailovers.Add(1)
}

// RecordCircuitBreaker records the circuit breaker state for a BU.
func (fm *FailoverMetrics) RecordCircuitBreaker(bu, state string) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	fm.circuitBreaker[bu] = state
}

// Snapshot returns a consistent snapshot of failover metrics.
func (fm *FailoverMetrics) Snapshot() (total int64, circuitBreaker map[string]string) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	cb := make(map[string]string, len(fm.circuitBreaker))
	for k, v := range fm.circuitBreaker {
		cb[k] = v
	}
	return fm.totalFailovers.Load(), cb
}

// ── Server ──────────────────────────────────────────────────────────────

// Server is the HTTP observability server.
type Server struct {
	mu      sync.RWMutex
	srv     *http.Server
	port    int

	healthProv   HealthProvider
	drainProv    DrainStatusProvider
	cfgProv      ConfigProvider
	weightProv   WeightProvider
	failoverProv FailoverStateProvider
	failoverM    *FailoverMetrics
	startTime    time.Time
}

// SetWeightProvider attaches the effective-weight source (e.g. proxy.Listener).
func (s *Server) SetWeightProvider(wp WeightProvider) {
	s.mu.Lock()
	s.weightProv = wp
	s.mu.Unlock()
}

// SetFailoverStateProvider attaches the DR state source.
func (s *Server) SetFailoverStateProvider(fp FailoverStateProvider) {
	s.mu.Lock()
	s.failoverProv = fp
	s.mu.Unlock()
}

// New creates a new HTTP observability server.
func New(port int, healthProv HealthProvider, drainProv DrainStatusProvider, cfgProv ConfigProvider, fm *FailoverMetrics) *Server {
	if port <= 0 {
		port = 8080
	}
	return &Server{
		port:       port,
		healthProv: healthProv,
		drainProv:  drainProv,
		cfgProv:    cfgProv,
		failoverM:  fm,
		startTime:  time.Now(),
	}
}

// Start begins listening on the configured port. Blocks until ctx is
// cancelled or a fatal error occurs. Call in a goroutine if the caller
// has other work to do.
func (s *Server) Start(ctx context.Context) error {
	// ── Prometheus registry ───────────────────────────────────────────
	reg := prometheus.NewRegistry()
	reg.MustRegister(newMetricsCollector(s))

	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/topic-stats", handleTopicStats)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(dashboardHTML)
	})

	addr := fmt.Sprintf(":%d", s.port)
	s.srv = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  30 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.srv.Shutdown(shutdownCtx); err != nil {
			logger.Default().Error("server: shutdown error", "error", err)
		}
	}()

	logger.Default().Info("Observability server listening", "address", addr)
	if err := s.srv.ListenAndServe(); err != http.ErrServerClosed {
		return fmt.Errorf("observability server failed: %w", err)
	}
	return nil
}

// ── /health ─────────────────────────────────────────────────────────────

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, `{"status":"ok"}`)
}

// ── Prometheus custom Collector ─────────────────────────────────────────
//
// metricsCollector implements prometheus.Collector so that metric values
// are fetched from the live providers on every scrape — no background
// update loop needed.
//
// Exported metric families:
//  1. proxy_up               — gauge, always 1 while running
//  2. proxy_uptime_seconds   — gauge, seconds since start
//  3. proxy_connections_active — gauge, total and per-BU
//  4. proxy_failover_total   — counter, total failover events
//  5. proxy_health_status    — gauge, 1=healthy (per BU, per endpoint)
//  6. proxy_health_consecutive_failures    — gauge
//  7. proxy_health_consecutive_successes   — gauge
//  8. proxy_health_last_check_latency_seconds — gauge
//  9. proxy_health_up_since_seconds        — gauge
// 10. proxy_circuit_breaker  — gauge, 1=broken (per BU)
// 11. proxy_drain_active     — gauge, 1=active drain (per BU)
// 12. proxy_build_info       — gauge (info metric)

type metricsCollector struct {
	srv *Server

	// Descriptors
	upDesc                  *prometheus.Desc
	uptimeDesc              *prometheus.Desc
	connectionsActiveDesc   *prometheus.Desc
	failoverTotalDesc       *prometheus.Desc
	healthStatusDesc        *prometheus.Desc
	healthConsecFailDesc    *prometheus.Desc
	healthConsecSuccDesc    *prometheus.Desc
	healthLatencyDesc       *prometheus.Desc
	healthUpSinceDesc       *prometheus.Desc
	circuitBreakerDesc      *prometheus.Desc
	drainActiveDesc         *prometheus.Desc
	buildInfoDesc           *prometheus.Desc
}

func newMetricsCollector(srv *Server) *metricsCollector {
	labelNames := []string{"bu", "endpoint"}

	return &metricsCollector{
		srv: srv,

		upDesc: prometheus.NewDesc(
			"proxy_up",
			"Whether the proxy process is running.",
			nil, nil,
		),
		uptimeDesc: prometheus.NewDesc(
			"proxy_uptime_seconds",
			"Seconds since proxy process start.",
			nil, nil,
		),
		connectionsActiveDesc: prometheus.NewDesc(
			"proxy_connections_active",
			"Active client connections.",
			[]string{"bu"}, nil,
		),
		failoverTotalDesc: prometheus.NewDesc(
			"proxy_failover_total",
			"Total number of failover events.",
			nil, nil,
		),
		healthStatusDesc: prometheus.NewDesc(
			"proxy_health_status",
			"Cluster endpoint health status (1 = healthy).",
			labelNames, nil,
		),
		healthConsecFailDesc: prometheus.NewDesc(
			"proxy_health_consecutive_failures",
			"Consecutive failed health checks.",
			labelNames, nil,
		),
		healthConsecSuccDesc: prometheus.NewDesc(
			"proxy_health_consecutive_successes",
			"Consecutive successful health checks.",
			labelNames, nil,
		),
		healthLatencyDesc: prometheus.NewDesc(
			"proxy_health_last_check_latency_seconds",
			"Latency of last health check.",
			labelNames, nil,
		),
		healthUpSinceDesc: prometheus.NewDesc(
			"proxy_health_up_since_seconds",
			"Seconds since endpoint became healthy.",
			labelNames, nil,
		),
		circuitBreakerDesc: prometheus.NewDesc(
			"proxy_circuit_breaker",
			"Circuit breaker state (1 = broken).",
			[]string{"bu"}, nil,
		),
		drainActiveDesc: prometheus.NewDesc(
			"proxy_drain_active",
			"Whether a drain is active for this cluster.",
			[]string{"bu"}, nil,
		),
		buildInfoDesc: prometheus.NewDesc(
			"proxy_build_info",
			"Proxy build metadata.",
			[]string{"version"}, nil,
		),
	}
}

// Describe sends all metric descriptors to ch.
func (c *metricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.upDesc
	ch <- c.uptimeDesc
	ch <- c.connectionsActiveDesc
	ch <- c.failoverTotalDesc
	ch <- c.healthStatusDesc
	ch <- c.healthConsecFailDesc
	ch <- c.healthConsecSuccDesc
	ch <- c.healthLatencyDesc
	ch <- c.healthUpSinceDesc
	ch <- c.circuitBreakerDesc
	ch <- c.drainActiveDesc
	ch <- c.buildInfoDesc
}

// Collect gathers current metric values from providers and sends them to ch.
func (c *metricsCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.srv
	now := time.Now()

	// 1. proxy_up
	ch <- prometheus.MustNewConstMetric(c.upDesc, prometheus.GaugeValue, 1)

	// 2. proxy_uptime_seconds
	uptime := now.Sub(s.startTime).Seconds()
	ch <- prometheus.MustNewConstMetric(c.uptimeDesc, prometheus.GaugeValue, uptime)

	// 4. proxy_failover_total
	failoverTotal, cbStates := s.failoverMetrics()
	ch <- prometheus.MustNewConstMetric(c.failoverTotalDesc, prometheus.CounterValue, float64(failoverTotal))

	// 3. proxy_connections_active
	if s.drainProv != nil {
		total := s.drainProv.TotalActiveConnections()
		ch <- prometheus.MustNewConstMetric(c.connectionsActiveDesc, prometheus.GaugeValue, float64(total), "")
	}

	// 5-9, 11. Per-cluster health + drain + per-BU connections
	if s.healthProv != nil {
		health := s.healthProv.Health()

		for name, chHealth := range health {
			emitEndpointMetrics(ch, c.healthStatusDesc, c.healthConsecFailDesc, c.healthConsecSuccDesc,
				c.healthLatencyDesc, c.healthUpSinceDesc, name, "primary", chHealth.Primary)
			emitEndpointMetrics(ch, c.healthStatusDesc, c.healthConsecFailDesc, c.healthConsecSuccDesc,
				c.healthLatencyDesc, c.healthUpSinceDesc, name, "secondary", chHealth.Secondary)

			// 11. proxy_drain_active
			if s.drainProv != nil {
				drainVal := 0.0
				if s.drainProv.IsDraining(name) {
					drainVal = 1
				}
				ch <- prometheus.MustNewConstMetric(c.drainActiveDesc, prometheus.GaugeValue, drainVal, name)
			}

			// Per-BU connections
			if s.drainProv != nil {
				conns := s.drainProv.ActiveConnections(name)
				ch <- prometheus.MustNewConstMetric(c.connectionsActiveDesc, prometheus.GaugeValue, float64(conns), name)
			}
		}
	}

	// 10. proxy_circuit_breaker
	for bu, state := range cbStates {
		val := 0.0
		if state == "broken" {
			val = 1
		}
		ch <- prometheus.MustNewConstMetric(c.circuitBreakerDesc, prometheus.GaugeValue, val, bu)
	}

	// 12. proxy_build_info
	ch <- prometheus.MustNewConstMetric(c.buildInfoDesc, prometheus.GaugeValue, 1, "1.0.0")
}

// failoverMetrics returns a snapshot of failover counters.
func (s *Server) failoverMetrics() (int64, map[string]string) {
	if s.failoverM == nil {
		return 0, nil
	}
	return s.failoverM.Snapshot()
}

func emitEndpointMetrics(ch chan<- prometheus.Metric,
	statusDesc, failDesc, succDesc, latencyDesc, upSinceDesc *prometheus.Desc,
	bu, endpoint string, h EndpointHealth) {

	healthy := 0.0
	if h.Healthy {
		healthy = 1
	}
	ch <- prometheus.MustNewConstMetric(statusDesc, prometheus.GaugeValue, healthy, bu, endpoint)
	ch <- prometheus.MustNewConstMetric(failDesc, prometheus.GaugeValue, float64(h.ConsecutiveFailures), bu, endpoint)
	ch <- prometheus.MustNewConstMetric(succDesc, prometheus.GaugeValue, float64(h.ConsecutiveSuccesses), bu, endpoint)
	ch <- prometheus.MustNewConstMetric(latencyDesc, prometheus.GaugeValue, h.LastCheckLatency.Seconds(), bu, endpoint)

	upSince := 0.0
	if !h.UpSince.IsZero() {
		upSince = time.Since(h.UpSince).Seconds()
	}
	ch <- prometheus.MustNewConstMetric(upSinceDesc, prometheus.GaugeValue, upSince, bu, endpoint)
}

// ── /status (JSON) ───────────────────────────────────────────────────────

type statusResponse struct {
	UptimeSeconds float64             `json:"uptime_seconds"`
	Connections   connectionsStatus   `json:"connections"`
	Clusters      []clusterStatus     `json:"clusters"`
	Failover      failoverStatus      `json:"failover"`
}

type connectionsStatus struct {
	Total int64            `json:"total"`
	PerBU map[string]int64 `json:"per_bu"`
}

type clusterStatus struct {
	Name       string               `json:"name"`
	Mode       string               `json:"mode"`
	Active     string               `json:"active,omitempty"`
	Primary   endpointStatusDetail `json:"primary"`
	Secondary endpointStatusDetail `json:"secondary"`
	Draining   bool                 `json:"draining"`
}

type endpointStatusDetail struct {
	Healthy              bool    `json:"healthy"`
	ConsecutiveFailures  int     `json:"consecutive_failures"`
	ConsecutiveSuccesses int     `json:"consecutive_successes"`
	LastCheckLatencyMS   float64 `json:"last_check_latency_ms"`
	LastStatus           string  `json:"last_status"`
	LastError            string  `json:"last_error,omitempty"`
	Bootstrap            string  `json:"bootstrap"`
	Weight               int     `json:"weight,omitempty"`
}

type failoverStatus struct {
	TotalFailovers int64             `json:"total_failovers"`
	CircuitBreaker map[string]string `json:"circuit_breaker_per_bu"`
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	failoverTotal, cbStates := s.failoverMetrics()

	resp := statusResponse{
		UptimeSeconds: time.Since(s.startTime).Seconds(),
		Connections: connectionsStatus{
			PerBU: make(map[string]int64),
		},
		Failover: failoverStatus{
			TotalFailovers: failoverTotal,
			CircuitBreaker: cbStates,
		},
	}

	if s.drainProv != nil {
		resp.Connections.Total = s.drainProv.TotalActiveConnections()
	}

	// Per-cluster status
	if s.cfgProv != nil && s.healthProv != nil {
		cfg := s.cfgProv.Config()
		health := s.healthProv.Health()

		for name, clusterCfg := range cfg.Clusters {
			cs := clusterStatus{
				Name: name,
				Mode: clusterCfg.Mode,
			}

			if clusterCfg.Mode == config.ModeActivePassive {
				cs.Active = clusterCfg.Active
				s.mu.RLock()
				fp := s.failoverProv
				s.mu.RUnlock()
				if fp != nil {
					if eff, ok := fp.EffectiveActive(name); ok {
						cs.Active = eff
					}
				}
			}

			primaryWeight := clusterCfg.Primary.Weight
			secondaryWeight := clusterCfg.Secondary.Weight
			s.mu.RLock()
			wp := s.weightProv
			s.mu.RUnlock()
			if wp != nil && clusterCfg.Mode == config.ModeLoadBalance {
				if p, sec, ok := wp.EffectiveWeights(name); ok {
					primaryWeight, secondaryWeight = p, sec
				}
			}

			if ch, ok := health[name]; ok {
				cs.Primary = endpointToDetail(ch.Primary, primaryWeight)
				cs.Secondary = endpointToDetail(ch.Secondary, secondaryWeight)
			}

			if s.drainProv != nil {
				cs.Draining = s.drainProv.IsDraining(name)
				resp.Connections.PerBU[name] = s.drainProv.ActiveConnections(name)
			}

			resp.Clusters = append(resp.Clusters, cs)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(resp)
}

func endpointToDetail(h EndpointHealth, weight int) endpointStatusDetail {
	return endpointStatusDetail{
		Healthy:              h.Healthy,
		ConsecutiveFailures:  h.ConsecutiveFailures,
		ConsecutiveSuccesses: h.ConsecutiveSuccesses,
		LastCheckLatencyMS:   float64(h.LastCheckLatency.Microseconds()) / 1000.0,
		LastStatus:           h.LastStatus,
		LastError:            h.LastError,
		Bootstrap:            h.Bootstrap,
		Weight:               weight,
	}
}

// handleTopicStats serves per-topic and per-cluster produce/fetch counters as JSON.
func handleTopicStats(w http.ResponseWriter, r *http.Request) {
	stats := struct {
		Topics   []routing.TopicStats   `json:"topics"`
		Clusters []routing.ClusterStats `json:"clusters"`
	}{
		Topics:   routing.GetTopicStats(),
		Clusters: routing.GetClusterStats(),
	}
	if stats.Topics == nil {
		stats.Topics = []routing.TopicStats{}
	}
	if stats.Clusters == nil {
		stats.Clusters = []routing.ClusterStats{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

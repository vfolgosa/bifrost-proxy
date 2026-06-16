package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/vfolgosa/bifrost-proxy/internal/config"
)

// ── Mock providers ──────────────────────────────────────────────────────

type mockHealthProvider struct {
	health map[string]ClusterHealthSnapshot
}

func (m *mockHealthProvider) Health() map[string]ClusterHealthSnapshot {
	return m.health
}

type mockDrainProvider struct {
	draining   map[string]bool
	conns      map[string]int64
	totalConns int64
}

func (m *mockDrainProvider) IsDraining(name string) bool {
	return m.draining[name]
}

func (m *mockDrainProvider) ActiveConnections(name string) int64 {
	return m.conns[name]
}

func (m *mockDrainProvider) TotalActiveConnections() int64 {
	return m.totalConns
}

type mockConfigProvider struct {
	cfg *config.Config
}

func (m *mockConfigProvider) Config() *config.Config {
	return m.cfg
}

func makeTestConfig() *config.Config {
	return &config.Config{
		Proxy: config.ProxyConfig{
			BindAddress: "0.0.0.0",
			MetricsPort: 8080,
		},
		Clusters: map[string]config.ClusterConfig{
			"bu1.example.com": {
				Mode:   config.ModeActivePassive,
				Active: config.ActivePrimary,
				Primary: config.ClusterEndpoint{
					Bootstrap: "primary:9093",
					Weight:    100,
				},
				Secondary: config.ClusterEndpoint{
					Bootstrap: "secondary:9094",
					Weight:    0,
				},
			},
			"bu2.example.com": {
				Mode: config.ModeLoadBalance,
				Primary: config.ClusterEndpoint{
					Bootstrap: "lb-primary:9095",
					Weight:    70,
				},
				Secondary: config.ClusterEndpoint{
					Bootstrap: "lb-secondary:9096",
					Weight:    30,
				},
			},
		},
	}
}

func makeTestHealth() map[string]ClusterHealthSnapshot {
	now := time.Now()
	return map[string]ClusterHealthSnapshot{
		"bu1.example.com": {
			Name: "bu1.example.com",
			Primary: EndpointHealth{
				Healthy:              true,
				ConsecutiveFailures:  0,
				ConsecutiveSuccesses: 5,
				LastCheckLatency:     12 * time.Millisecond,
				LastStatus:           "healthy",
				UpSince:              now.Add(-1 * time.Hour),
				Bootstrap:            "primary:9093",
			},
			Secondary: EndpointHealth{
				Healthy:              true,
				ConsecutiveFailures:  0,
				ConsecutiveSuccesses: 3,
				LastCheckLatency:     8 * time.Millisecond,
				LastStatus:           "healthy",
				UpSince:              now.Add(-30 * time.Minute),
				Bootstrap:            "secondary:9094",
			},
		},
		"bu2.example.com": {
			Name: "bu2.example.com",
			Primary: EndpointHealth{
				Healthy:              false,
				ConsecutiveFailures:  3,
				ConsecutiveSuccesses: 0,
				LastCheckLatency:     5 * time.Second,
				LastStatus:           "unreachable",
				LastError:            "connection refused",
				Bootstrap:            "lb-primary:9095",
			},
			Secondary: EndpointHealth{
				Healthy:              true,
				ConsecutiveFailures:  0,
				ConsecutiveSuccesses: 2,
				LastCheckLatency:     15 * time.Millisecond,
				LastStatus:           "healthy",
				UpSince:              now.Add(-5 * time.Minute),
				Bootstrap:            "lb-secondary:9096",
			},
		},
	}
}

// metricsHandlerFor creates a test HTTP handler for /metrics that wires
// the server's providers into a fresh prometheus registry.
func metricsHandlerFor(srv *Server) http.Handler {
	reg := prometheus.NewRegistry()
	reg.MustRegister(newMetricsCollector(srv))
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}

// ── Tests ───────────────────────────────────────────────────────────────

func TestHealthEndpoint(t *testing.T) {
	fm := NewFailoverMetrics()
	srv := New(0, nil, nil, nil, fm)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	srv.handleHealth(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", resp.StatusCode)
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("expected Content-Type application/json, got %q", contentType)
	}

	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode JSON: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("expected status=ok, got %q", body["status"])
	}
}

func TestHealthEndpoint_MethodNotAllowed(t *testing.T) {
	fm := NewFailoverMetrics()
	srv := New(0, nil, nil, nil, fm)

	req := httptest.NewRequest(http.MethodPost, "/health", nil)
	w := httptest.NewRecorder()
	srv.handleHealth(w, req)

	if w.Result().StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Result().StatusCode)
	}
}

func TestMetricsEndpoint_NoProviders(t *testing.T) {
	fm := NewFailoverMetrics()
	srv := New(0, nil, nil, nil, fm)

	handler := metricsHandlerFor(srv)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", resp.StatusCode)
	}

	contentType := resp.Header.Get("Content-Type")
	if !strings.HasPrefix(contentType, "text/plain") {
		t.Errorf("expected text/plain Content-Type, got %q", contentType)
	}
}

func TestMetricsEndpoint_AllProviders(t *testing.T) {
	health := &mockHealthProvider{health: makeTestHealth()}
	drain := &mockDrainProvider{
		draining:   map[string]bool{"bu1.example.com": true},
		conns:      map[string]int64{"bu1.example.com": 12, "bu2.example.com": 7},
		totalConns: 19,
	}
	cfg := &mockConfigProvider{cfg: makeTestConfig()}
	fm := NewFailoverMetrics()
	fm.RecordFailover()
	fm.RecordFailover()
	fm.RecordCircuitBreaker("bu2.example.com", "broken")

	srv := New(0, health, drain, cfg, fm)
	handler := metricsHandlerFor(srv)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Result().StatusCode)
	}

	body := w.Body.String()

	// Verify all 12 metric families are present
	requiredMetrics := []string{
		"proxy_up",
		"proxy_uptime_seconds",
		"proxy_connections_active",
		"proxy_failover_total",
		"proxy_health_status",
		"proxy_health_consecutive_failures",
		"proxy_health_consecutive_successes",
		"proxy_health_last_check_latency_seconds",
		"proxy_health_up_since_seconds",
		"proxy_circuit_breaker",
		"proxy_drain_active",
		"proxy_build_info",
	}

	for _, m := range requiredMetrics {
		if !strings.Contains(body, m) {
			t.Errorf("metrics output missing %q", m)
		}
	}

	// Verify specific values
	if !strings.Contains(body, "proxy_failover_total 2") {
		t.Error("expected proxy_failover_total 2")
	}

	if !strings.Contains(body, "proxy_up 1") {
		t.Error("expected proxy_up 1")
	}

	// Verify circuit breaker is broken for bu2
	if !strings.Contains(body, `proxy_circuit_breaker{bu="bu2.example.com"} 1`) {
		t.Error("expected circuit breaker broken for bu2")
	}

	// Verify drain active for bu1
	if !strings.Contains(body, `proxy_drain_active{bu="bu1.example.com"} 1`) {
		t.Error("expected drain_active=1 for bu1")
	}

	// Verify health status
	if !strings.Contains(body, `proxy_health_status{bu="bu1.example.com",endpoint="primary"} 1`) {
		t.Error("expected healthy primary for bu1")
	}
	if !strings.Contains(body, `proxy_health_status{bu="bu2.example.com",endpoint="primary"} 0`) {
		t.Error("expected unhealthy primary for bu2")
	}
}

func TestStatusEndpoint(t *testing.T) {
	health := &mockHealthProvider{health: makeTestHealth()}
	drain := &mockDrainProvider{
		draining:   map[string]bool{"bu1.example.com": true},
		conns:      map[string]int64{"bu1.example.com": 12, "bu2.example.com": 7},
		totalConns: 19,
	}
	cfg := &mockConfigProvider{cfg: makeTestConfig()}
	fm := NewFailoverMetrics()
	fm.RecordFailover()
	fm.RecordCircuitBreaker("bu2.example.com", "broken")

	srv := New(0, health, drain, cfg, fm)

	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	w := httptest.NewRecorder()
	srv.handleStatus(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Result().StatusCode)
	}

	contentType := w.Result().Header.Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("expected application/json, got %q", contentType)
	}

	var resp statusResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode JSON: %v", err)
	}

	if resp.UptimeSeconds <= 0 {
		t.Error("expected positive uptime")
	}

	if resp.Connections.Total != 19 {
		t.Errorf("expected 19 total connections, got %d", resp.Connections.Total)
	}

	if len(resp.Clusters) != 2 {
		t.Fatalf("expected 2 clusters, got %d", len(resp.Clusters))
	}

	// Find clusters by name (map iteration is non-deterministic in Go)
	clustersByName := make(map[string]clusterStatus)
	for _, c := range resp.Clusters {
		clustersByName[c.Name] = c
	}

	// Verify bu1 (active_passive)
	bu1, ok := clustersByName["bu1.example.com"]
	if !ok {
		t.Fatal("bu1.example.com not found in response")
	}
	if bu1.Mode != "active_passive" {
		t.Errorf("expected active_passive mode, got %q", bu1.Mode)
	}
	if bu1.Active != "primary" {
		t.Errorf("expected active=primary, got %q", bu1.Active)
	}
	if !bu1.Draining {
		t.Error("expected bu1 to be draining")
	}
	if !bu1.Primary.Healthy {
		t.Error("expected bu1 primary healthy")
	}

	// Verify bu2 (load_balance)
	bu2, ok := clustersByName["bu2.example.com"]
	if !ok {
		t.Fatal("bu2.example.com not found in response")
	}
	if bu2.Mode != "load_balance" {
		t.Errorf("expected load_balance mode, got %q", bu2.Mode)
	}
	if bu2.Primary.Healthy {
		t.Error("expected bu2 primary unhealthy")
	}
	if !bu2.Secondary.Healthy {
		t.Error("expected bu2 secondary healthy")
	}
	if bu2.Primary.Weight != 70 {
		t.Errorf("expected weight 70 for primary, got %d", bu2.Primary.Weight)
	}
	if bu2.Secondary.Weight != 30 {
		t.Errorf("expected weight 30 for secondary, got %d", bu2.Secondary.Weight)
	}

	// Verify failover
	if resp.Failover.TotalFailovers != 1 {
		t.Errorf("expected 1 failover, got %d", resp.Failover.TotalFailovers)
	}
	if resp.Failover.CircuitBreaker["bu2.example.com"] != "broken" {
		t.Errorf("expected circuit breaker broken for bu2, got %q", resp.Failover.CircuitBreaker["bu2.example.com"])
	}
}

func TestFailoverMetrics_Concurrent(t *testing.T) {
	fm := NewFailoverMetrics()

	// Concurrent writes should not race
	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				fm.RecordFailover()
				fm.RecordCircuitBreaker(fmt.Sprintf("bu%d", j%5), "broken")
			}
			done <- struct{}{}
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	total, cb := fm.Snapshot()
	if total != 1000 {
		t.Errorf("expected 1000 total failovers, got %d", total)
	}
	if len(cb) < 1 {
		t.Error("expected at least 1 circuit breaker entry")
	}
}

func TestHealthEndpoint_Integration(t *testing.T) {
	health := &mockHealthProvider{health: makeTestHealth()}
	fm := NewFailoverMetrics()

	srv := New(0, health, nil, nil, fm)

	// Start the server on a random port and test it.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			srv.handleHealth(w, r)
		case "/metrics":
			metricsHandlerFor(srv).ServeHTTP(w, r)
		case "/status":
			srv.handleStatus(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	// Test /health
	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatalf("GET /health failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	// Test /metrics
	resp, err = http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestFailoverMetrics_SnapshotIsCopy(t *testing.T) {
	fm := NewFailoverMetrics()
	fm.RecordCircuitBreaker("bu1", "broken")

	_, cb1 := fm.Snapshot()
	cb1["bu1"] = "modified" // mutate the copy

	_, cb2 := fm.Snapshot()
	if cb2["bu1"] != "broken" {
		t.Error("snapshot should be a copy, mutation should not affect original")
	}
}

func TestNew_PortDefaults(t *testing.T) {
	srv := New(0, nil, nil, nil, nil)
	if srv.port != 8080 {
		t.Errorf("expected default port 8080, got %d", srv.port)
	}

	srv2 := New(9090, nil, nil, nil, nil)
	if srv2.port != 9090 {
		t.Errorf("expected port 9090, got %d", srv2.port)
	}
}

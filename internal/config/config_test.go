package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// projectRoot walks up from the test file to find the project root
// so we can reference testdata/ regardless of where "go test" is invoked.
func projectRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("cannot find project root (go.mod)")
		}
		dir = parent
	}
}

// ── Happy path ────────────────────────────────────────────────────────

func TestLoadConfig_ValidFile(t *testing.T) {
	root := projectRoot(t)
	cfg, err := LoadConfig(filepath.Join(root, "testdata", "valid_config.yaml"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ── Proxy ──
	if cfg.Proxy.BindAddress != "0.0.0.0" {
		t.Errorf("bind_address: got %q, want %q", cfg.Proxy.BindAddress, "0.0.0.0")
	}
	if cfg.Proxy.ConnectionPool.MaxConnectionsPerBroker != 50 {
		t.Errorf("max_connections_per_broker: got %d", cfg.Proxy.ConnectionPool.MaxConnectionsPerBroker)
	}
	if cfg.Proxy.ConnectionPool.IdleTimeout.Duration() != 30*time.Second {
		t.Errorf("idle_timeout: got %v", cfg.Proxy.ConnectionPool.IdleTimeout.Duration())
	}
	if cfg.Proxy.ConnectionPool.KeepAliveInterval.Duration() != 30*time.Second {
		t.Errorf("keep_alive_interval: got %v", cfg.Proxy.ConnectionPool.KeepAliveInterval.Duration())
	}
	if cfg.Proxy.MetricsPort != 8080 {
		t.Errorf("metrics_port: got %d", cfg.Proxy.MetricsPort)
	}

	// ── Clusters ──
	if len(cfg.Clusters) != 2 {
		t.Fatalf("clusters count: got %d, want 2", len(cfg.Clusters))
	}

	// ──  (active_passive) ──
	fi, ok := cfg.Clusters["finance.proxy.example.com"]
	if !ok {
		t.Fatal("finance.proxy.example.com cluster not found")
	}
	if fi.Mode != "active_passive" {
		t.Errorf(" mode: got %q", fi.Mode)
	}
	if fi.Active != "primary" {
		t.Errorf(" active: got %q", fi.Active)
	}
	if fi.Primary.Bootstrap != "pkc-11111.us-east-1.aws.confluent.cloud:9092" {
		t.Errorf(" primary bootstrap: got %q", fi.Primary.Bootstrap)
	}
	if fi.Primary.Weight != 0 {
		t.Errorf(" primary weight: got %d, want 0", fi.Primary.Weight)
	}
	if fi.Secondary.Bootstrap != "pkc-22222.us-east-2.aws.confluent.cloud:9092" {
		t.Errorf(" secondary bootstrap: got %q", fi.Secondary.Bootstrap)
	}

	hc := fi.HealthCheck
	if !hc.Enabled {
		t.Error(" health_check.enabled: want true")
	}
	if hc.Interval.Duration() != 10*time.Second {
		t.Errorf(" health_check.interval: got %v", hc.Interval.Duration())
	}
	if hc.FailureThreshold != 3 {
		t.Errorf(" failure_threshold: got %d", hc.FailureThreshold)
	}
	if hc.RecoveryThreshold != 2 {
		t.Errorf(" recovery_threshold: got %d", hc.RecoveryThreshold)
	}
	if hc.MinTimeBetweenFailovers.Duration() != 60*time.Second {
		t.Errorf(" min_time_between_failovers: got %v", hc.MinTimeBetweenFailovers.Duration())
	}
	if !hc.AutoFailover {
		t.Error(" auto_failover: want true")
	}
	if hc.AutoFailback {
		t.Error(" auto_failback: want false")
	}
	if !hc.RequireTargetHealthy {
		t.Error(" require_target_healthy: want true")
	}
	if hc.CircuitBreakerMaxFailovers != 3 {
		t.Errorf(" circuit_breaker_max_failovers: got %d", hc.CircuitBreakerMaxFailovers)
	}
	if hc.CircuitBreakerWindow.Duration() != 300*time.Second {
		t.Errorf(" circuit_breaker_window: got %v", hc.CircuitBreakerWindow.Duration())
	}

	// ──  (load_balance) ──
	lo, ok := cfg.Clusters["logistics.proxy.example.com"]
	if !ok {
		t.Fatal("logistics.proxy.example.com cluster not found")
	}
	if lo.Mode != "load_balance" {
		t.Errorf(" mode: got %q", lo.Mode)
	}
	if lo.Primary.Bootstrap != "pkc-33333.us-east-1.aws.confluent.cloud:9092" {
		t.Errorf(" primary bootstrap: got %q", lo.Primary.Bootstrap)
	}
	if lo.Primary.Weight != 70 {
		t.Errorf(" primary weight: got %d, want 70", lo.Primary.Weight)
	}
	if lo.Secondary.Bootstrap != "pkc-44444.us-east-2.aws.confluent.cloud:9092" {
		t.Errorf(" secondary bootstrap: got %q", lo.Secondary.Bootstrap)
	}
	if lo.Secondary.Weight != 30 {
		t.Errorf(" secondary weight: got %d, want 30", lo.Secondary.Weight)
	}

	lhc := lo.HealthCheck
	if lhc.RecoveryThreshold != 5 {
		t.Errorf(" recovery_threshold: got %d, want 5", lhc.RecoveryThreshold)
	}
	if lhc.RecoveryMinUptime.Duration() != 120*time.Second {
		t.Errorf(" recovery_min_uptime: got %v", lhc.RecoveryMinUptime.Duration())
	}
	if !lhc.AutoRebalance {
		t.Error(" auto_rebalance: want true")
	}
}

// ── Defaults ──────────────────────────────────────────────────────────

func TestLoadConfig_Defaults(t *testing.T) {
	root := projectRoot(t)
	cfg, err := LoadConfig(filepath.Join(root, "testdata", "minimal_config.yaml"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Proxy.BindAddress != "0.0.0.0" {
		t.Errorf("default bind_address: got %q, want \"0.0.0.0\"", cfg.Proxy.BindAddress)
	}
	if cfg.Proxy.ConnectionPool.MaxConnectionsPerBroker != 50 {
		t.Errorf("default max_connections_per_broker: got %d", cfg.Proxy.ConnectionPool.MaxConnectionsPerBroker)
	}
	if cfg.Proxy.ConnectionPool.IdleTimeout.Duration() != 30*time.Second {
		t.Errorf("default idle_timeout: got %v", cfg.Proxy.ConnectionPool.IdleTimeout.Duration())
	}
	if cfg.Proxy.ConnectionPool.KeepAliveInterval.Duration() != 30*time.Second {
		t.Errorf("default keep_alive_interval: got %v", cfg.Proxy.ConnectionPool.KeepAliveInterval.Duration())
	}

	for name, cluster := range cfg.Clusters {
		hc := cluster.HealthCheck
		if hc.FailureThreshold != 3 {
			t.Errorf("%s: default failure_threshold: got %d, want 3", name, hc.FailureThreshold)
		}
		if hc.RecoveryThreshold != 2 {
			t.Errorf("%s: default recovery_threshold: got %d, want 2", name, hc.RecoveryThreshold)
		}
		if hc.RecoveryMinUptime.Duration() != 120*time.Second {
			t.Errorf("%s: default recovery_min_uptime: got %v", name, hc.RecoveryMinUptime.Duration())
		}
	}
}

// ── Error paths ───────────────────────────────────────────────────────

func TestLoadConfig_MissingFile(t *testing.T) {
	_, err := LoadConfig("/nonexistent/path/config.yaml")
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestLoadConfig_InvalidYAML(t *testing.T) {
	root := projectRoot(t)
	_, err := LoadConfig(filepath.Join(root, "testdata", "invalid_config.yaml"))
	if err == nil {
		t.Fatal("expected error for invalid YAML, got nil")
	}
}

func TestLoadConfig_MissingPrimary(t *testing.T) {
	root := projectRoot(t)
	_, err := LoadConfig(filepath.Join(root, "testdata", "active_passive_missing_primary.yaml"))
	if err == nil {
		t.Fatal("expected error for missing primary bootstrap, got nil")
	}
}

func TestLoadConfig_WeightsNot100(t *testing.T) {
	root := projectRoot(t)
	_, err := LoadConfig(filepath.Join(root, "testdata", "load_balance_weights_not_100.yaml"))
	if err == nil {
		t.Fatal("expected error for load_balance weights not summing to 100, got nil")
	}
}

// ── Duration unmarshaling ─────────────────────────────────────────────

func TestDuration_UnmarshalYAML(t *testing.T) {
	tests := []struct {
		yaml     string
		expected time.Duration
	}{
		{"10s", 10 * time.Second},
		{"5m", 5 * time.Minute},
		{"1h", 1 * time.Hour},
		{"120s", 120 * time.Second},
		{"30s", 30 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.yaml, func(t *testing.T) {
			var d Duration
			if err := d.UnmarshalYAML(&yaml.Node{Kind: yaml.ScalarNode, Value: tt.yaml}); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if d.Duration() != tt.expected {
				t.Errorf("got %v, want %v", d.Duration(), tt.expected)
			}
		})
	}
}

func TestDuration_Invalid(t *testing.T) {
	var d Duration
	err := d.UnmarshalYAML(&yaml.Node{Kind: yaml.ScalarNode, Value: "not-a-duration"})
	if err == nil {
		t.Fatal("expected error for invalid duration string, got nil")
	}
}

// ── ClusterEndpoint unmarshaling ──────────────────────────────────────

func TestClusterEndpoint_String(t *testing.T) {
	var e ClusterEndpoint
	err := e.UnmarshalYAML(&yaml.Node{Kind: yaml.ScalarNode, Value: "pkc-xxx.region.aws.confluent.cloud:9092"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.Bootstrap != "pkc-xxx.region.aws.confluent.cloud:9092" {
		t.Errorf("bootstrap: got %q", e.Bootstrap)
	}
	if e.Weight != 0 {
		t.Errorf("weight: got %d, want 0", e.Weight)
	}
}

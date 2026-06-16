package routing

import (
	"sync"
	"testing"
	"time"

	"github.com/vfolgosa/bifrost-proxy/internal/config"
	"github.com/vfolgosa/bifrost-proxy/internal/health"
)

// ── Mock Health Checker ────────────────────────────────────────────────

// mockHealthChecker implements RebalanceHealthChecker for testing.
// Health() returns a snapshot that can be updated externally via Update/SetHealthy.
type mockHealthChecker struct {
	mu      sync.Mutex
	data    map[string]health.ClusterHealth
}

func newMockHealthChecker() *mockHealthChecker {
	return &mockHealthChecker{
		data: make(map[string]health.ClusterHealth),
	}
}

func (m *mockHealthChecker) Health() map[string]health.ClusterHealth {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Return a copy so callers can't mutate our state.
	result := make(map[string]health.ClusterHealth, len(m.data))
	for k, v := range m.data {
		result[k] = v
	}
	return result
}

func (m *mockHealthChecker) setHealthy(cluster, endpoint string, healthy bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ch := m.data[cluster]
	ch.Name = cluster

	snap := health.Snapshot{
		Healthy:              healthy,
		ConsecutiveFailures:  0,
		ConsecutiveSuccesses: 0,
		UpSince:              time.Time{},
	}
	if healthy {
		snap.ConsecutiveSuccesses = 5
		snap.UpSince = time.Now()
	} else {
		snap.ConsecutiveFailures = 5
	}

	switch endpoint {
	case "primary":
		ch.Primary = snap
	case "secondary":
		ch.Secondary = snap
	}

	m.data[cluster] = ch
}

func (m *mockHealthChecker) setBothHealthy(cluster string, primHealthy, secHealthy bool) {
	m.setHealthy(cluster, "primary", primHealthy)
	m.setHealthy(cluster, "secondary", secHealthy)
}

// ── Test Helpers ───────────────────────────────────────────────────────

func lbConfig(primW, secW int) *config.Config {
	return &config.Config{
		Proxy: config.ProxyConfig{
			ConnectionPool: config.ConnectionPoolConfig{
				MaxConnectionsPerBroker: 10,
			},
		},
		Clusters: map[string]config.ClusterConfig{
			"bu-test": {
				Mode:     config.ModeLoadBalance,
				Primary: config.ClusterEndpoint{Bootstrap: "p:9092", Weight: primW},
				Secondary: config.ClusterEndpoint{Bootstrap: "s:9092", Weight: secW},
				HealthCheck: config.HealthCheckConfig{
					Enabled:           true,
					FailureThreshold:  3,
					RecoveryThreshold: 5,
					RecoveryMinUptime: config.Duration(120 * time.Second),
				},
			},
		},
	}
}

// ── Tests ───────────────────────────────────────────────────────────────

func TestRebalancer_PrimaryDown(t *testing.T) {
	cfg := lbConfig(50, 50)
	cache := NewMapPartitionLeaderCache()
	router := NewRouter(cfg, cache)
	mock := newMockHealthChecker()
	mock.setBothHealthy("bu-test", true, true)

	rb := NewRebalancer(router, mock, cfg)
	rb.tick() // seed prevHealthy

	// Verify initial weights are from config.
	primW, secW := router.GetEffectiveWeights("bu-test", cfg.Clusters["bu-test"])
	if primW != 50 || secW != 50 {
		t.Fatalf("initial weights = %d/%d, want 50/50", primW, secW)
	}

	// Simulate primary going DOWN.
	mock.setBothHealthy("bu-test", false, true)

	// Trigger a tick.
	rb.tick()

	primW, secW = router.GetEffectiveWeights("bu-test", cfg.Clusters["bu-test"])
	if primW != 0 || secW != 100 {
		t.Errorf("after primary DOWN: weights = %d/%d, want 0/100", primW, secW)
	}
}

func TestRebalancer_SecondaryDown(t *testing.T) {
	cfg := lbConfig(60, 40)
	cache := NewMapPartitionLeaderCache()
	router := NewRouter(cfg, cache)
	mock := newMockHealthChecker()
	mock.setBothHealthy("bu-test", true, true)

	rb := NewRebalancer(router, mock, cfg)
	rb.tick() // seed prevHealthy

	// Simulate secondary going DOWN.
	mock.setBothHealthy("bu-test", true, false)
	rb.tick()

	primW, secW := router.GetEffectiveWeights("bu-test", cfg.Clusters["bu-test"])
	if primW != 100 || secW != 0 {
		t.Errorf("after secondary DOWN: weights = %d/%d, want 100/0", primW, secW)
	}
}

func TestRebalancer_BothDown(t *testing.T) {
	// When both are down, primary weight goes to 0, secondary to 100
	// (the handleDown logic: if primaryDown → 0/100).
	cfg := lbConfig(50, 50)
	cache := NewMapPartitionLeaderCache()
	router := NewRouter(cfg, cache)
	mock := newMockHealthChecker()
	mock.setBothHealthy("bu-test", true, true)

	rb := NewRebalancer(router, mock, cfg)
	rb.tick() // seed prevHealthy

	// Simulate both going DOWN.
	mock.setBothHealthy("bu-test", false, false)
	rb.tick()

	primW, secW := router.GetEffectiveWeights("bu-test", cfg.Clusters["bu-test"])
	if primW != 0 || secW != 100 {
		t.Errorf("after both DOWN: weights = %d/%d, want 0/100 (prim down takes priority)", primW, secW)
	}
}

func TestRebalancer_SecondaryUp_RestoresImmediately(t *testing.T) {
	cfg := lbConfig(60, 40)
	cache := NewMapPartitionLeaderCache()
	router := NewRouter(cfg, cache)
	mock := newMockHealthChecker()
	mock.setBothHealthy("bu-test", true, false) // secondary starts down

	rb := NewRebalancer(router, mock, cfg)
	rb.tick() // seed prevHealthy

	// Shift weights due to down secondary.
	mock.setBothHealthy("bu-test", true, false)
	// First we need a transition. Let's set prevHealthy manually via a tick.
	// Actually, the first tick seeds state — we need two ticks for transition.
	// Seed: both healthy initially
	mock.setBothHealthy("bu-test", true, true)
	rb.tick() // seed prevHealthy = {true, true}

	// Now take secondary down.
	mock.setBothHealthy("bu-test", true, false)
	rb.tick() // transition detected

	primW, secW := router.GetEffectiveWeights("bu-test", cfg.Clusters["bu-test"])
	if primW != 100 || secW != 0 {
		t.Fatalf("after secondary DOWN: weights = %d/%d, want 100/0", primW, secW)
	}

	// Bring secondary back up.
	mock.setBothHealthy("bu-test", true, true)
	rb.tick() // UP transition → restore

	primW, secW = router.GetEffectiveWeights("bu-test", cfg.Clusters["bu-test"])
	if primW != 60 || secW != 40 {
		t.Errorf("after secondary UP: weights = %d/%d, want 60/40", primW, secW)
	}
}

func TestRebalancer_PrimaryUp_RecoveryMinUptime(t *testing.T) {
	cfg := lbConfig(70, 30)
	cache := NewMapPartitionLeaderCache()
	router := NewRouter(cfg, cache)
	mock := newMockHealthChecker()
	mock.setBothHealthy("bu-test", true, true)

	rb := NewRebalancer(router, mock, cfg)
	rb.tick() // seed prevHealthy = {true, true}

	// Take primary down.
	mock.setBothHealthy("bu-test", false, true)
	rb.tick()

	primW, secW := router.GetEffectiveWeights("bu-test", cfg.Clusters["bu-test"])
	if primW != 0 || secW != 100 {
		t.Fatalf("after primary DOWN: weights = %d/%d, want 0/100", primW, secW)
	}

	// Bring primary back UP — but UpSince is set to time.Now(),
	// which is < 120s ago, so weights should stay shifted.
	mock.setBothHealthy("bu-test", true, true)
	rb.tick()

	primW, secW = router.GetEffectiveWeights("bu-test", cfg.Clusters["bu-test"])
	if primW != 0 || secW != 100 {
		t.Errorf("immediately after primary UP: weights = %d/%d, want still 0/100 (recovery min_uptime not met)", primW, secW)
	}

	if !rb.primaryRecovering["bu-test"] {
		t.Error("expected primaryRecovering=true after primary UP with insufficient uptime")
	}

	// Advance time beyond recovery_min_uptime and tick again.
	// We need to update the mock's UpSince to be in the past.
	mock.setBothHealthy("bu-test", true, true)
	// Manually set UpSince to 130s ago.
	mock.mu.Lock()
	ch := mock.data["bu-test"]
	ch.Primary.UpSince = time.Now().Add(-130 * time.Second)
	mock.data["bu-test"] = ch
	mock.mu.Unlock()

	rb.tick()

	primW, secW = router.GetEffectiveWeights("bu-test", cfg.Clusters["bu-test"])
	if primW != 70 || secW != 30 {
		t.Errorf("after recovery_min_uptime met: weights = %d/%d, want 70/30", primW, secW)
	}

	if rb.primaryRecovering["bu-test"] {
		t.Error("expected primaryRecovering=false after recovery complete")
	}
}

func TestRebalancer_PrimaryUp_AlreadyPastMinUptime(t *testing.T) {
	cfg := lbConfig(55, 45)
	cache := NewMapPartitionLeaderCache()
	router := NewRouter(cfg, cache)
	mock := newMockHealthChecker()
	mock.setBothHealthy("bu-test", true, true)

	rb := NewRebalancer(router, mock, cfg)
	rb.tick() // seed

	// Take primary down.
	mock.setBothHealthy("bu-test", false, true)
	rb.tick()

	// Bring primary back, but set UpSince to 200s ago.
	mock.setBothHealthy("bu-test", true, true)
	mock.mu.Lock()
	ch := mock.data["bu-test"]
	ch.Primary.UpSince = time.Now().Add(-200 * time.Second)
	mock.data["bu-test"] = ch
	mock.mu.Unlock()

	rb.tick()

	primW, secW := router.GetEffectiveWeights("bu-test", cfg.Clusters["bu-test"])
	if primW != 55 || secW != 45 {
		t.Errorf("after primary UP (already past min_uptime): weights = %d/%d, want 55/45", primW, secW)
	}

	if rb.primaryRecovering["bu-test"] {
		t.Error("expected primaryRecovering=false when already past min_uptime")
	}
}

func TestRebalancer_NoTransitionOnFirstTick(t *testing.T) {
	cfg := lbConfig(50, 50)
	cache := NewMapPartitionLeaderCache()
	router := NewRouter(cfg, cache)
	mock := newMockHealthChecker()
	mock.setBothHealthy("bu-test", false, true) // primary already down

	rb := NewRebalancer(router, mock, cfg)
	rb.tick() // first tick: seed only, no transition

	// Weights should still be at config defaults.
	primW, secW := router.GetEffectiveWeights("bu-test", cfg.Clusters["bu-test"])
	if primW != 50 || secW != 50 {
		t.Errorf("after first tick (no transition): weights = %d/%d, want 50/50", primW, secW)
	}
}

func TestRebalancer_ActivePassiveIgnored(t *testing.T) {
	cfg := &config.Config{
		Proxy: config.ProxyConfig{
			ConnectionPool: config.ConnectionPoolConfig{
				MaxConnectionsPerBroker: 10,
			},
		},
		Clusters: map[string]config.ClusterConfig{
			"bu-ap": {
				Mode:     config.ModeActivePassive,
				Active:   config.ActivePrimary,
				Primary: config.ClusterEndpoint{Bootstrap: "p:9092"},
				Secondary: config.ClusterEndpoint{Bootstrap: "s:9092"},
			},
		},
	}

	cache := NewMapPartitionLeaderCache()
	router := NewRouter(cfg, cache)
	mock := newMockHealthChecker()
	mock.setBothHealthy("bu-ap", true, true)

	rb := NewRebalancer(router, mock, cfg)
	rb.tick() // seed

	// Take primary down.
	mock.setBothHealthy("bu-ap", false, true)
	rb.tick()

	// Weights should be 0,0 (not overridden for active_passive).
	primW, secW := router.GetEffectiveWeights("bu-ap", cfg.Clusters["bu-ap"])
	if primW != 0 || secW != 0 {
		t.Errorf("active_passive weights after DOWN: %d/%d, want 0/0", primW, secW)
	}
}

func TestRebalancer_MultipleClusters(t *testing.T) {
	cfg := &config.Config{
		Proxy: config.ProxyConfig{
			ConnectionPool: config.ConnectionPoolConfig{
				MaxConnectionsPerBroker: 10,
			},
		},
		Clusters: map[string]config.ClusterConfig{
			"bu-a": {
				Mode:     config.ModeLoadBalance,
				Primary: config.ClusterEndpoint{Bootstrap: "pa:9092", Weight: 60},
				Secondary: config.ClusterEndpoint{Bootstrap: "sa:9092", Weight: 40},
				HealthCheck: config.HealthCheckConfig{
					Enabled:           true,
					RecoveryMinUptime: config.Duration(120 * time.Second),
				},
			},
			"bu-b": {
				Mode:     config.ModeLoadBalance,
				Primary: config.ClusterEndpoint{Bootstrap: "pb:9092", Weight: 80},
				Secondary: config.ClusterEndpoint{Bootstrap: "sb:9092", Weight: 20},
				HealthCheck: config.HealthCheckConfig{
					Enabled:           true,
					RecoveryMinUptime: config.Duration(120 * time.Second),
				},
			},
		},
	}

	cache := NewMapPartitionLeaderCache()
	router := NewRouter(cfg, cache)
	mock := newMockHealthChecker()
	mock.setBothHealthy("bu-a", true, true)
	mock.setBothHealthy("bu-b", true, true)

	rb := NewRebalancer(router, mock, cfg)
	rb.tick() // seed

	// Take bu-a secondary down, bu-b primary down.
	mock.setBothHealthy("bu-a", true, false)
	mock.setBothHealthy("bu-b", false, true)
	rb.tick()

	// bu-a: secondary down → 100/0
	paW, saW := router.GetEffectiveWeights("bu-a", cfg.Clusters["bu-a"])
	if paW != 100 || saW != 0 {
		t.Errorf("bu-a after secondary DOWN: %d/%d, want 100/0", paW, saW)
	}

	// bu-b: primary down → 0/100
	pbW, sbW := router.GetEffectiveWeights("bu-b", cfg.Clusters["bu-b"])
	if pbW != 0 || sbW != 100 {
		t.Errorf("bu-b after primary DOWN: %d/%d, want 0/100", pbW, sbW)
	}
}

func TestRebalancer_ResetWeights(t *testing.T) {
	cfg := lbConfig(65, 35)
	cache := NewMapPartitionLeaderCache()
	router := NewRouter(cfg, cache)
	mock := newMockHealthChecker()
	mock.setBothHealthy("bu-test", true, true)

	rb := NewRebalancer(router, mock, cfg)
	rb.tick() // seed

	// Take primary down.
	mock.setBothHealthy("bu-test", false, true)
	rb.tick()

	primW, secW := router.GetEffectiveWeights("bu-test", cfg.Clusters["bu-test"])
	if primW != 0 || secW != 100 {
		t.Fatalf("after primary DOWN: %d/%d, want 0/100", primW, secW)
	}

	// Reset all weights.
	rb.ResetWeights()

	primW, secW = router.GetEffectiveWeights("bu-test", cfg.Clusters["bu-test"])
	if primW != 65 || secW != 35 {
		t.Errorf("after ResetWeights: %d/%d, want 65/35", primW, secW)
	}
}

func TestRebalancer_StartStop(t *testing.T) {
	cfg := lbConfig(50, 50)
	cache := NewMapPartitionLeaderCache()
	router := NewRouter(cfg, cache)
	mock := newMockHealthChecker()
	mock.setBothHealthy("bu-test", true, true)

	rb := NewRebalancer(router, mock, cfg)
	rb.Start()
	// Let it run briefly.
	time.Sleep(100 * time.Millisecond)
	rb.Stop()

	// Should not panic or deadlock.
}

func TestRebalancer_RecoveryMinUptimeDefault(t *testing.T) {
	// Cluster with no explicit RecoveryMinUptime — should default to 120s.
	cfg := &config.Config{
		Proxy: config.ProxyConfig{
			ConnectionPool: config.ConnectionPoolConfig{
				MaxConnectionsPerBroker: 10,
			},
		},
		Clusters: map[string]config.ClusterConfig{
			"bu-test": {
				Mode:     config.ModeLoadBalance,
				Primary: config.ClusterEndpoint{Bootstrap: "p:9092", Weight: 50},
				Secondary: config.ClusterEndpoint{Bootstrap: "s:9092", Weight: 50},
				HealthCheck: config.HealthCheckConfig{
					Enabled: true,
					// RecoveryMinUptime not set — defaults to 120s via applyDefaults.
				},
			},
		},
	}

	// Apply defaults like LoadConfig does.
	for name, cluster := range cfg.Clusters {
		if cluster.HealthCheck.RecoveryMinUptime == 0 {
			cluster.HealthCheck.RecoveryMinUptime = config.Duration(120 * time.Second)
		}
		cfg.Clusters[name] = cluster
	}

	cache := NewMapPartitionLeaderCache()
	router := NewRouter(cfg, cache)
	mock := newMockHealthChecker()
	mock.setBothHealthy("bu-test", true, true)

	rb := NewRebalancer(router, mock, cfg)
	rb.tick() // seed

	// Take primary down, then up.
	mock.setBothHealthy("bu-test", false, true)
	rb.tick()

	mock.setBothHealthy("bu-test", true, true)
	rb.tick()

	// Should be in recovery.
	if !rb.primaryRecovering["bu-test"] {
		t.Error("expected primaryRecovering=true after primary UP with insufficient uptime")
	}
}

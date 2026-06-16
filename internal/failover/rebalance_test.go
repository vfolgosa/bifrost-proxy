package failover

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/vfolgosa/bifrost-proxy/internal/config"
)

// ── Test helpers ──────────────────────────────────────────────────────

// lbConfig creates a load_balance cluster config for testing.
func lbConfig(pw, sw int) config.ClusterConfig {
	return config.ClusterConfig{
		Mode: config.ModeLoadBalance,
		Primary: config.ClusterEndpoint{
			Bootstrap: "primary.example.com:9092",
			Weight:    pw,
		},
		Secondary: config.ClusterEndpoint{
			Bootstrap: "secondary.example.com:9092",
			Weight:    sw,
		},
		HealthCheck: config.HealthCheckConfig{
			AutoRebalance:              true,
			FailureThreshold:           3,
			RecoveryThreshold:          5,
			RecoveryMinUptime:          config.Duration(0), // immediate for tests
			MinTimeBetweenFailovers:    config.Duration(0),
			CircuitBreakerMaxFailovers: 0, // disabled for tests
		},
	}
}

// ── NewRebalancer ─────────────────────────────────────────────────────

func TestNewRebalancer_InitialState(t *testing.T) {
	cfg := lbConfig(60, 40)
	r := NewRebalancer(cfg)

	if pw := r.GetEffectiveWeight(config.ActivePrimary); pw != 60 {
		t.Errorf("primary effective weight = %d, want 60", pw)
	}
	if sw := r.GetEffectiveWeight(config.ActiveSecondary); sw != 40 {
		t.Errorf("secondary effective weight = %d, want 40", sw)
	}
	if r.Status(config.ActivePrimary) != StatusUp {
		t.Errorf("primary status = %s, want %s", r.Status(config.ActivePrimary), StatusUp)
	}
	if r.Status(config.ActiveSecondary) != StatusUp {
		t.Errorf("secondary status = %s, want %s", r.Status(config.ActiveSecondary), StatusUp)
	}
}

func TestNewRebalancer_EffectiveWeightsMatchConfig(t *testing.T) {
	cfg := lbConfig(30, 70)
	r := NewRebalancer(cfg)

	pw := r.GetEffectiveWeight(config.ActivePrimary)
	sw := r.GetEffectiveWeight(config.ActiveSecondary)

	if pw != 30 {
		t.Errorf("primary = %d, want 30", pw)
	}
	if sw != 70 {
		t.Errorf("secondary = %d, want 70", sw)
	}
}

// ── RecordFailure: primary DOWN ───────────────────────────────────────

func TestRecordFailure_Primary_DemotesToZero(t *testing.T) {
	cfg := lbConfig(60, 40)
	r := NewRebalancer(cfg)

	// Three consecutive failures should trigger DOWN
	r.RecordFailure(cfg.Primary.Bootstrap)
	r.RecordFailure(cfg.Primary.Bootstrap)
	r.RecordFailure(cfg.Primary.Bootstrap)

	if r.Status(config.ActivePrimary) != StatusDown {
		t.Errorf("primary status = %s, want %s", r.Status(config.ActivePrimary), StatusDown)
	}
	if pw := r.GetEffectiveWeight(config.ActivePrimary); pw != 0 {
		t.Errorf("primary effective weight = %d, want 0", pw)
	}
	// Secondary should still be UP with full weight
	if sw := r.GetEffectiveWeight(config.ActiveSecondary); sw != 40 {
		t.Errorf("secondary effective weight = %d, want 40", sw)
	}

	// SelectTarget should go to secondary
	// Run multiple times to verify weighted random selection
	secCount := 0
	for i := 0; i < 100; i++ {
		if r.SelectTarget() == cfg.Secondary.Bootstrap {
			secCount++
		}
	}
	if secCount != 100 {
		t.Errorf("SelectTarget went to secondary %d/100 times (expected all traffic to secondary)", secCount)
	}
}

func TestRecordFailure_Secondary_DemotesToZero(t *testing.T) {
	cfg := lbConfig(30, 70)
	r := NewRebalancer(cfg)

	r.RecordFailure(cfg.Secondary.Bootstrap)
	r.RecordFailure(cfg.Secondary.Bootstrap)
	r.RecordFailure(cfg.Secondary.Bootstrap)

	if r.Status(config.ActiveSecondary) != StatusDown {
		t.Errorf("secondary status = %s, want %s", r.Status(config.ActiveSecondary), StatusDown)
	}
	if sw := r.GetEffectiveWeight(config.ActiveSecondary); sw != 0 {
		t.Errorf("secondary effective weight = %d, want 0", sw)
	}

	// All traffic should go to primary
	primCount := 0
	for i := 0; i < 100; i++ {
		if r.SelectTarget() == cfg.Primary.Bootstrap {
			primCount++
		}
	}
	if primCount != 100 {
		t.Errorf("SelectTarget went to primary %d/100 times (expected all traffic to primary)", primCount)
	}
}

// ── Both DOWN ─────────────────────────────────────────────────────────

func TestRecordFailure_BothDown_FallsBackToOriginalWeights(t *testing.T) {
	cfg := lbConfig(60, 40)
	r := NewRebalancer(cfg)

	// Take both down
	for i := 0; i < 3; i++ {
		r.RecordFailure(cfg.Primary.Bootstrap)
	}
	for i := 0; i < 3; i++ {
		r.RecordFailure(cfg.Secondary.Bootstrap)
	}

	if r.Status(config.ActivePrimary) != StatusDown {
		t.Errorf("primary status = %s, want %s", r.Status(config.ActivePrimary), StatusDown)
	}
	if r.Status(config.ActiveSecondary) != StatusDown {
		t.Errorf("secondary status = %s, want %s", r.Status(config.ActiveSecondary), StatusDown)
	}
	if pw := r.GetEffectiveWeight(config.ActivePrimary); pw != 0 {
		t.Errorf("primary effective weight = %d, want 0", pw)
	}
	if sw := r.GetEffectiveWeight(config.ActiveSecondary); sw != 0 {
		t.Errorf("secondary effective weight = %d, want 0", sw)
	}

	// SelectTarget should fall back to original weights (60/40 split)
	primCount := 0
	secCount := 0
	const trials = 1000
	for i := 0; i < trials; i++ {
		target := r.SelectTarget()
		if target == cfg.Primary.Bootstrap {
			primCount++
		} else if target == cfg.Secondary.Bootstrap {
			secCount++
		}
	}

	// With 60/40 split, expect roughly 60% to primary, 40% to secondary
	primPct := float64(primCount) / float64(trials) * 100
	if primPct < 40 || primPct > 80 {
		t.Errorf("primary got %.1f%% of traffic (want ~60%% with 60/40 split)", primPct)
	}
	if primCount+secCount != trials {
		t.Errorf("expected %d selections, got %d", trials, primCount+secCount)
	}
}

// ── RecordSuccess: recovery ───────────────────────────────────────────

func TestRecordSuccess_RecoveryPath(t *testing.T) {
	cfg := lbConfig(60, 40)
	// Use minimal recovery threshold and uptime for fast test
	cfg.HealthCheck.RecoveryThreshold = 2
	cfg.HealthCheck.RecoveryMinUptime = config.Duration(1) // 1ns, bypasses default 120s
	r := NewRebalancer(cfg)

	// Take primary down
	r.RecordFailure(cfg.Primary.Bootstrap)
	r.RecordFailure(cfg.Primary.Bootstrap)
	r.RecordFailure(cfg.Primary.Bootstrap)

	if r.Status(config.ActivePrimary) != StatusDown {
		t.Fatalf("expected primary DOWN, got %s", r.Status(config.ActivePrimary))
	}

	// First success → RECOVERING
	r.RecordSuccess(cfg.Primary.Bootstrap)
	if r.Status(config.ActivePrimary) != StatusRecovering {
		t.Errorf("after 1st success: status = %s, want %s", r.Status(config.ActivePrimary), StatusRecovering)
	}
	if pw := r.GetEffectiveWeight(config.ActivePrimary); pw != 0 {
		t.Errorf("during recovery: primary weight = %d, want 0", pw)
	}

	// Second success + uptime met → UP
	r.RecordSuccess(cfg.Primary.Bootstrap)
	if r.Status(config.ActivePrimary) != StatusUp {
		t.Errorf("after recovery: status = %s, want %s", r.Status(config.ActivePrimary), StatusUp)
	}
	if pw := r.GetEffectiveWeight(config.ActivePrimary); pw != 60 {
		t.Errorf("after recovery: primary weight = %d, want 60", pw)
	}
}

func TestRecordSuccess_RecoveryInterrupted(t *testing.T) {
	cfg := lbConfig(60, 40)
	cfg.HealthCheck.RecoveryThreshold = 3
	r := NewRebalancer(cfg)

	// Take primary down
	for i := 0; i < 3; i++ {
		r.RecordFailure(cfg.Primary.Bootstrap)
	}

	// Start recovering
	r.RecordSuccess(cfg.Primary.Bootstrap) // 1 success
	if r.Status(config.ActivePrimary) != StatusRecovering {
		t.Fatalf("expected RECOVERING, got %s", r.Status(config.ActivePrimary))
	}

	// Failure during recovery → back to DOWN
	r.RecordFailure(cfg.Primary.Bootstrap)
	if r.Status(config.ActivePrimary) != StatusDown {
		t.Errorf("after recovery failure: status = %s, want %s", r.Status(config.ActivePrimary), StatusDown)
	}
	if pw := r.GetEffectiveWeight(config.ActivePrimary); pw != 0 {
		t.Errorf("after recovery failure: weight = %d, want 0", pw)
	}
}

// ── EffectiveWeight metric exposure ───────────────────────────────────

func TestRebalancer_EffectiveWeightJSON(t *testing.T) {
	cfg := lbConfig(50, 50)
	r := NewRebalancer(cfg)

	json, err := EffectiveWeightJSON(r)
	if err != nil {
		t.Fatalf("EffectiveWeightJSON error: %v", err)
	}

	if !strings.Contains(string(json), `"primary":50`) {
		t.Errorf("JSON missing primary: %s", string(json))
	}
	if !strings.Contains(string(json), `"secondary":50`) {
		t.Errorf("JSON missing secondary: %s", string(json))
	}
}

func TestRebalancer_EffectiveWeightJSON_AfterFailover(t *testing.T) {
	cfg := lbConfig(70, 30)
	r := NewRebalancer(cfg)

	// Take primary down
	for i := 0; i < 3; i++ {
		r.RecordFailure(cfg.Primary.Bootstrap)
	}

	data, err := EffectiveWeightJSON(r)
	if err != nil {
		t.Fatalf("EffectiveWeightJSON error: %v", err)
	}

	if !strings.Contains(string(data), `"primary":0`) {
		t.Errorf("JSON missing primary=0: %s", string(data))
	}
	if !strings.Contains(string(data), `"secondary":30`) {
		t.Errorf("JSON missing secondary=30: %s", string(data))
	}
}

// ── Metrics HTTP handler ──────────────────────────────────────────────

type testRebalancerProvider struct {
	rebalancers map[string]*Rebalancer
}

func (p *testRebalancerProvider) GetRebalancers() map[string]*Rebalancer {
	return p.rebalancers
}

func TestMetricsHandler_PrometheusFormat(t *testing.T) {
	cfg := lbConfig(60, 40)
	r := NewRebalancer(cfg)

	provider := &testRebalancerProvider{
		rebalancers: map[string]*Rebalancer{"test-cluster": r},
	}

	handler := MetricsHandler(provider)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	resp := rec.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("metrics status = %d, want 200", resp.StatusCode)
	}

	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "text/plain") {
		t.Errorf("Content-Type = %q, want text/plain", ct)
	}

	buf := make([]byte, 4096)
	n, _ := resp.Body.Read(buf)
	body := string(buf[:n])

	expected := []string{
		`kafkaproxy_effective_weight{cluster="test-cluster",endpoint="primary"} 60`,
		`kafkaproxy_effective_weight{cluster="test-cluster",endpoint="secondary"} 40`,
	}
	for _, exp := range expected {
		if !strings.Contains(body, exp) {
			t.Errorf("metrics missing: %q", exp)
		}
	}
}

func TestMetricsHandler_AfterFailover(t *testing.T) {
	cfg := lbConfig(30, 70)
	r := NewRebalancer(cfg)

	// Take secondary down
	for i := 0; i < 3; i++ {
		r.RecordFailure(cfg.Secondary.Bootstrap)
	}

	provider := &testRebalancerProvider{
		rebalancers: map[string]*Rebalancer{"bu-x": r},
	}

	handler := MetricsHandler(provider)
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	buf := make([]byte, 4096)
	n, _ := rec.Result().Body.Read(buf)
	body := string(buf[:n])

	// Primary should be at full weight (30), secondary at 0 (DOWN)
	if !strings.Contains(body, `kafkaproxy_effective_weight{cluster="bu-x",endpoint="primary"} 30`) {
		t.Errorf("expected primary weight 30: %s", body)
	}
	if !strings.Contains(body, `kafkaproxy_effective_weight{cluster="bu-x",endpoint="secondary"} 0`) {
		t.Errorf("expected secondary weight 0: %s", body)
	}
}

// ── SelectTarget: weighted random distribution ────────────────────────

func TestSelectTarget_WeightedDistribution(t *testing.T) {
	cfg := lbConfig(70, 30)
	r := NewRebalancer(cfg)

	primCount := 0
	secCount := 0
	const trials = 10000
	for i := 0; i < trials; i++ {
		switch r.SelectTarget() {
		case cfg.Primary.Bootstrap:
			primCount++
		case cfg.Secondary.Bootstrap:
			secCount++
		}
	}

	primPct := float64(primCount) / float64(trials) * 100
	if primPct < 55 || primPct > 85 {
		t.Errorf("primary got %.1f%% (want ~70%%)", primPct)
	}
	if primCount+secCount != trials {
		t.Errorf("total selections = %d, want %d", primCount+secCount, trials)
	}
}

// ── Concurrent access ─────────────────────────────────────────────────

func TestRebalance_Concurrent(t *testing.T) {
	cfg := lbConfig(50, 50)
	r := NewRebalancer(cfg)

	var wg sync.WaitGroup
	const goroutines = 20

	// Concurrent reads
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				r.GetEffectiveWeight(config.ActivePrimary)
				r.GetEffectiveWeight(config.ActiveSecondary)
				r.Status(config.ActivePrimary)
				r.Status(config.ActiveSecondary)
				r.SelectTarget()
				r.PrimaryAddr()
				r.SecondaryAddr()
			}
		}()
	}

	// Concurrent writes
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			addr := cfg.Primary.Bootstrap
			if idx%2 == 0 {
				addr = cfg.Secondary.Bootstrap
			}
			for j := 0; j < 20; j++ {
				r.RecordSuccess(addr)
				r.RecordFailure(addr)
			}
		}(i)
	}

	wg.Wait()
	// Should not panic
	_ = r.SelectTarget()
}

// ── Edge cases ────────────────────────────────────────────────────────

func TestGetEffectiveWeight_UnknownEndpoint(t *testing.T) {
	cfg := lbConfig(50, 50)
	r := NewRebalancer(cfg)

	if got := r.GetEffectiveWeight("unknown"); got != -1 {
		t.Errorf("GetEffectiveWeight(unknown) = %d, want -1", got)
	}
}

func TestStatus_UnknownEndpoint(t *testing.T) {
	cfg := lbConfig(50, 50)
	r := NewRebalancer(cfg)

	if got := r.Status("unknown"); got != "" {
		t.Errorf("Status(unknown) = %q, want empty string", got)
	}
}

func TestRecordFailure_UnknownAddr(t *testing.T) {
	cfg := lbConfig(50, 50)
	r := NewRebalancer(cfg)

	// Should not panic for unknown address
	r.RecordFailure("unknown:9092")
	r.RecordSuccess("unknown:9092")

	// State should be unchanged
	if pw := r.GetEffectiveWeight(config.ActivePrimary); pw != 50 {
		t.Errorf("primary weight changed to %d", pw)
	}
}

func TestPrimaryAddr_SecondaryAddr(t *testing.T) {
	cfg := lbConfig(50, 50)
	r := NewRebalancer(cfg)

	if r.PrimaryAddr() != cfg.Primary.Bootstrap {
		t.Errorf("PrimaryAddr = %q, want %q", r.PrimaryAddr(), cfg.Primary.Bootstrap)
	}
	if r.SecondaryAddr() != cfg.Secondary.Bootstrap {
		t.Errorf("SecondaryAddr = %q, want %q", r.SecondaryAddr(), cfg.Secondary.Bootstrap)
	}
}

// ── EffectiveWeightJSON helper test ───────────────────────────────────

func TestEffectiveWeightJSON_EmptyRebalancer(t *testing.T) {
	// Test with nil rebalancer edge case
	_, err := EffectiveWeightJSON(nil)
	if err == nil {
		t.Error("Expected error for nil rebalancer")
	}
}

// ── helpers for tests ─────────────────────────────────────────────────

// EffectiveWeightJSON returns a JSON representation of a Rebalancer's
// effective weights, suitable for metrics exposition.
func EffectiveWeightJSON(r *Rebalancer) ([]byte, error) {
	if r == nil {
		return nil, fmt.Errorf("rebalancer is nil")
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	return json.Marshal(map[string]int{
		"primary":   r.primary.effectiveWeight,
		"secondary": r.secondary.effectiveWeight,
	})
}

// RebalancerProvider is an interface for types that can provide
// a map of cluster name → Rebalancer for metrics exposition.
type RebalancerProvider interface {
	GetRebalancers() map[string]*Rebalancer
}

// MetricsHandler returns an http.Handler that serves effective_weight
// metrics in Prometheus text format for all registered rebalancers.
func MetricsHandler(provider RebalancerProvider) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		for name, rb := range provider.GetRebalancers() {
			rb.mu.RLock()
			fmt.Fprintf(w, "kafkaproxy_effective_weight{cluster=%q,endpoint=\"primary\"} %d\n",
				name, rb.primary.effectiveWeight)
			fmt.Fprintf(w, "kafkaproxy_effective_weight{cluster=%q,endpoint=\"secondary\"} %d\n",
				name, rb.secondary.effectiveWeight)
			rb.mu.RUnlock()
		}
	})
}

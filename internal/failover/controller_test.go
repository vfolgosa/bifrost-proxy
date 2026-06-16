package failover

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/vfolgosa/bifrost-proxy/internal/config"
)

// ── Helpers ───────────────────────────────────────────────────────────

func testHC() config.HealthCheckConfig {
	return config.HealthCheckConfig{
		Enabled:                    true,
		FailureThreshold:           3,
		RecoveryThreshold:          2,
		MinTimeBetweenFailovers:    config.Duration(0),
		AutoFailover:               true,
		AutoFailback:               false,
		RequireTargetHealthy:       true,
		CircuitBreakerMaxFailovers: 100, // high = effectively disabled
		CircuitBreakerWindow:       config.Duration(300 * time.Second),
	}
}

// newTestController creates a Controller backed by a fresh StateMachine
// with the BU already initialized.
func newTestController(active, mode string, hc config.HealthCheckConfig) (*Controller, *StateMachine) {
	sm := NewStateMachine()
	initState := StatePrimary
	if active == config.ActiveSecondary {
		initState = StateSecondary
	}
	sm.Initialize("test-bu", initState)
	c := NewController("test-bu", active, mode, hc, sm)
	return c, sm
}

func recordSuccesses(c *Controller, cluster string, n int) {
	for i := 0; i < n; i++ {
		c.RecordHealthResult(cluster, true, 10*time.Millisecond)
	}
}

func recordFailures(c *Controller, cluster string, n int) {
	for i := 0; i < n; i++ {
		c.RecordHealthResult(cluster, false, 0)
	}
}

// ── Initialization ────────────────────────────────────────────────────

func TestNewController_PrimaryActive(t *testing.T) {
	c, sm := newTestController("primary", config.ModeActivePassive, testHC())

	if got := sm.State("test-bu"); got != StatePrimary {
		t.Errorf("initial state: got %q, want primary", got)
	}
	if c.PrimaryHealthy() {
		t.Error("primary should not be healthy initially (no checks recorded)")
	}
}

func TestNewController_SecondaryActive(t *testing.T) {
	c, sm := newTestController("secondary", config.ModeActivePassive, testHC())

	if got := sm.State("test-bu"); got != StateSecondary {
		t.Errorf("initial state: got %q, want secondary", got)
	}
	_ = c
}

// ── Health tracking ───────────────────────────────────────────────────

func TestHealth_BecomesHealthyAfterRecoveryThreshold(t *testing.T) {
	c, _ := newTestController("primary", config.ModeActivePassive, testHC())

	recordFailures(c, "primary", 3)
	if c.PrimaryHealthy() {
		t.Error("primary should be unhealthy after 3 failures")
	}

	recordSuccesses(c, "primary", 2)
	if !c.PrimaryHealthy() {
		t.Error("primary should be healthy after 2 consecutive successes (recovery_threshold=2)")
	}
}

func TestHealth_BecomesUnhealthyAfterFailureThreshold(t *testing.T) {
	c, _ := newTestController("primary", config.ModeActivePassive, testHC())

	recordSuccesses(c, "primary", 2)
	if !c.PrimaryHealthy() {
		t.Fatal("setup: primary should be healthy")
	}

	recordFailures(c, "primary", 3)
	if c.PrimaryHealthy() {
		t.Error("primary should be unhealthy after 3 consecutive failures")
	}
}

func TestHealth_CountersReset(t *testing.T) {
	c, _ := newTestController("primary", config.ModeActivePassive, testHC())

	// Make primary healthy first.
	recordSuccesses(c, "primary", 2)
	if !c.PrimaryHealthy() {
		t.Fatal("setup: primary should be healthy after 2 successes")
	}

	// A single failure — still healthy (1 < failure_threshold=3).
	c.RecordHealthResult("primary", false, 0)
	if !c.PrimaryHealthy() {
		t.Error("should still be healthy after 1 failure (threshold=3 not met)")
	}

	// A success resets the failure counter.
	c.RecordHealthResult("primary", true, 10*time.Millisecond)

	// Three consecutive failures = unhealthy.
	c.RecordHealthResult("primary", false, 0)
	c.RecordHealthResult("primary", false, 0)
	c.RecordHealthResult("primary", false, 0)
	if c.PrimaryHealthy() {
		t.Error("should be unhealthy after 3 consecutive failures")
	}
}

// ── BOTH_DOWN ────────────────────────────────────────────────────────

func TestBothDown_EnteredWhenBothUnhealthy(t *testing.T) {
	c, sm := newTestController("primary", config.ModeActivePassive, testHC())

	recordFailures(c, "primary", 3)
	recordFailures(c, "secondary", 3)

	c.Evaluate()

	if got := sm.State("test-bu"); got != StateBothDown {
		t.Errorf("state: got %q, want ", got)
	}
	if c.BothDownCount() != 1 {
		t.Errorf("_count: got %d, want 1", c.BothDownCount())
	}
}

func TestBothDown_HoldsLastActiveCluster(t *testing.T) {
	c, sm := newTestController("primary", config.ModeActivePassive, testHC())

	recordFailures(c, "primary", 3)
	recordFailures(c, "secondary", 3)
	c.Evaluate()

	if sm.State("test-bu") != StateBothDown {
		t.Fatal("expected ")
	}
	// BothDownActiveCluster stores raw BUState strings ("PRIMARIO", "SECUNDARIO").
	if got := c.BothDownActiveCluster(); got != string(StatePrimary) {
		t.Errorf(" active: got %q, want %q", got, StatePrimary)
	}
}

func TestBothDown_HoldsLastActive_SecondaryStarted(t *testing.T) {
	c, sm := newTestController("secondary", config.ModeActivePassive, testHC())

	recordFailures(c, "primary", 3)
	recordFailures(c, "secondary", 3)
	c.Evaluate()

	if sm.State("test-bu") != StateBothDown {
		t.Fatal("expected ")
	}
	if got := c.BothDownActiveCluster(); got != string(StateSecondary) {
		t.Errorf(" active: got %q, want %q", got, StateSecondary)
	}
}

func TestBothDown_AfterFailover_HoldsSecondary(t *testing.T) {
	c, sm := newTestController("primary", config.ModeActivePassive, testHC())

	// Primary fails, secondary healthy: failover to secondary (through DRAINING).
	recordSuccesses(c, "secondary", 2)
	recordFailures(c, "primary", 3)
	action := c.Evaluate()
	if action != ActionFailoverToSecondary {
		t.Fatalf("failover failed: got %q", action)
	}
	// State is DRAINING after health-based failover (drain must complete first).
	if sm.State("test-bu") != StateDraining {
		t.Fatalf("expected  after failover, got %q", sm.State("test-bu"))
	}

	// Now secondary also fails during drain → .
	recordFailures(c, "secondary", 3)
	c.Evaluate()

	if sm.State("test-bu") != StateBothDown {
		t.Fatalf("expected , got %q", sm.State("test-bu"))
	}
	if got := c.BothDownActiveCluster(); got != string(StateSecondary) {
		t.Errorf(" after failover: active should be %q, got %q",
			StateSecondary, got)
	}
}

func TestBothDown_NeverFlapBetweenTwoBrokenClusters(t *testing.T) {
	c, sm := newTestController("primary", config.ModeActivePassive, testHC())

	recordFailures(c, "primary", 3)
	recordFailures(c, "secondary", 3)
	c.Evaluate()

	if sm.State("test-bu") != StateBothDown {
		t.Fatal("expected ")
	}

	cluster := c.BothDownActiveCluster()

	// Many cycles while both stay down.
	for i := 0; i < 50; i++ {
		c.RecordHealthResult("primary", false, 0)
		c.RecordHealthResult("secondary", false, 0)
		action := c.Evaluate()

		if action != ActionNone {
			t.Errorf("cycle %d: expected none action, got %q", i, action)
		}
		if sm.State("test-bu") != StateBothDown {
			t.Errorf("cycle %d: state changed to %q (flap!)", i, sm.State("test-bu"))
		}
		if c.BothDownActiveCluster() != cluster {
			t.Errorf("cycle %d: active cluster changed from %q to %q (flap!)",
				i, cluster, c.BothDownActiveCluster())
		}
	}
}

func TestBothDown_PrimaryRecoversFirst(t *testing.T) {
	c, sm := newTestController("primary", config.ModeActivePassive, testHC())

	recordFailures(c, "primary", 3)
	recordFailures(c, "secondary", 3)
	c.Evaluate()

	if sm.State("test-bu") != StateBothDown {
		t.Fatal("expected ")
	}

	// Primary recovers.
	recordSuccesses(c, "primary", 2)
	// Secondary still down.
	c.RecordHealthResult("secondary", false, 0)

	c.Evaluate()

	if sm.State("test-bu") != StatePrimary {
		t.Errorf("after primary recovery: state=%q, want primary", sm.State("test-bu"))
	}
}

func TestBothDown_SecondaryRecoversFirst(t *testing.T) {
	c, sm := newTestController("primary", config.ModeActivePassive, testHC())

	recordFailures(c, "primary", 3)
	recordFailures(c, "secondary", 3)
	c.Evaluate()

	if sm.State("test-bu") != StateBothDown {
		t.Fatal("expected ")
	}

	// Secondary recovers first.
	recordSuccesses(c, "secondary", 2)
	c.RecordHealthResult("primary", false, 0)

	c.Evaluate()

	if sm.State("test-bu") != StateSecondary {
		t.Errorf("after secondary recovery: state=%q, want secondary", sm.State("test-bu"))
	}
}

func TestBothDown_BothRecoverSimultaneously(t *testing.T) {
	c, sm := newTestController("primary", config.ModeActivePassive, testHC())

	recordFailures(c, "primary", 3)
	recordFailures(c, "secondary", 3)
	c.Evaluate()

	// Both recover in the same cycle.
	recordSuccesses(c, "primary", 2)
	recordSuccesses(c, "secondary", 2)

	c.Evaluate()

	// Primary is checked first in evaluateLocked, so it wins.
	if sm.State("test-bu") != StatePrimary {
		t.Errorf("state: got %q, want primary (first evaluated wins)", sm.State("test-bu"))
	}
}

func TestBothDown_CountIncrements(t *testing.T) {
	c, _ := newTestController("primary", config.ModeActivePassive, testHC())

	if c.BothDownCount() != 0 {
		t.Errorf("initial: got %d", c.BothDownCount())
	}

	// First entry.
	recordFailures(c, "primary", 3)
	recordFailures(c, "secondary", 3)
	c.Evaluate()

	if c.BothDownCount() != 1 {
		t.Errorf("after first: got %d, want 1", c.BothDownCount())
	}

	// Recover.
	recordSuccesses(c, "primary", 2)
	c.Evaluate()

	// Go down again.
	recordFailures(c, "primary", 3)
	recordFailures(c, "secondary", 3)
	c.Evaluate()

	if c.BothDownCount() != 2 {
		t.Errorf("after second: got %d, want 2", c.BothDownCount())
	}
}

func TestBothDown_OnlyEntersOnceWhenAlreadyDown(t *testing.T) {
	c, _ := newTestController("primary", config.ModeActivePassive, testHC())

	recordFailures(c, "primary", 3)
	recordFailures(c, "secondary", 3)
	c.Evaluate()

	count := c.BothDownCount()

	for i := 0; i < 10; i++ {
		c.RecordHealthResult("primary", false, 0)
		c.RecordHealthResult("secondary", false, 0)
		c.Evaluate()
	}

	if c.BothDownCount() != count {
		t.Errorf("count should not increment on re-evaluate: got %d, want %d",
			c.BothDownCount(), count)
	}
}

// ── Failover (primary → secondary) ────────────────────────────────────

func TestFailover_PrimaryToSecondary(t *testing.T) {
	c, sm := newTestController("primary", config.ModeActivePassive, testHC())

	recordSuccesses(c, "secondary", 2)
	recordFailures(c, "primary", 3)

	action := c.Evaluate()

	if action != ActionFailoverToSecondary {
		t.Errorf("action: got %q, want failover_to_secondary", action)
	}
	// After health-based failover, state enters DRAINING (the DRCoordinator
	// handles the actual drain and subsequent transition to Secondary).
	if sm.State("test-bu") != StateDraining {
		t.Errorf("state: got %q, want  (intermediate drain state)", sm.State("test-bu"))
	}
	if c.FailoverCount() != 1 {
		t.Errorf("failover_count: got %d, want 1", c.FailoverCount())
	}
}

func TestFailover_RequireTargetHealthy(t *testing.T) {
	hc := testHC()
	hc.RequireTargetHealthy = true
	c, _ := newTestController("primary", config.ModeActivePassive, hc)

	// Both unhealthy.
	recordFailures(c, "primary", 3)
	recordFailures(c, "secondary", 3)
	c.Evaluate()

	// It should enter BOTH_DOWN, not failover.
	if c.BothDownCount() != 1 {
		t.Errorf("expected  (both unhealthy, require_target_healthy), count=%d", c.BothDownCount())
	}
}

func TestFailover_MinTimeBetweenFailovers(t *testing.T) {
	hc := testHC()
	hc.MinTimeBetweenFailovers = config.Duration(100 * time.Millisecond)
	c, _ := newTestController("primary", config.ModeActivePassive, hc)

	recordSuccesses(c, "secondary", 2)

	// First failover succeeds.
	recordFailures(c, "primary", 3)
	action := c.Evaluate()
	if action != ActionFailoverToSecondary {
		t.Fatalf("first failover: got %q", action)
	}

	// Now test failback with cooldown.
	hc2 := testHC()
	hc2.AutoFailback = true
	hc2.MinTimeBetweenFailovers = config.Duration(100 * time.Millisecond)
	c2, _ := newTestController("secondary", config.ModeActivePassive, hc2)

	recordSuccesses(c2, "primary", 2)
	action = c2.Evaluate()
	if action != ActionFailoverToPrimary {
		t.Fatalf("first failback: got %q", action)
	}

	// Reset state for second attempt — but cooldown should block.
	_ = c2.sm.Transition("test-bu", StateSecondary, ReasonConfigChange)
	recordSuccesses(c2, "primary", 2)
	action = c2.Evaluate()
	if action != ActionNone {
		t.Errorf("second failback during cooldown should be none, got %q", action)
	}
}

func TestFailover_AutoFailbackDisabled(t *testing.T) {
	c, sm := newTestController("secondary", config.ModeActivePassive, testHC())

	recordSuccesses(c, "primary", 2)
	action := c.Evaluate()

	// auto_failback is false by default, so no failback.
	if action != ActionNone {
		t.Errorf("expected none (auto_failback disabled), got %q", action)
	}
	if sm.State("test-bu") != StateSecondary {
		t.Errorf("state should stay secondary, got %q", sm.State("test-bu"))
	}
}

func TestFailover_AutoFailbackEnabled(t *testing.T) {
	hc := testHC()
	hc.AutoFailback = true
	c, sm := newTestController("secondary", config.ModeActivePassive, hc)

	recordSuccesses(c, "primary", 2)
	action := c.Evaluate()

	if action != ActionFailoverToPrimary {
		t.Errorf("expected failback_to_primary, got %q", action)
	}
	// Health-based failback goes through DRAINING; the DRCoordinator
	// handles the subsequent transition to the target.
	if sm.State("test-bu") != StateDraining {
		t.Errorf("state should be  (intermediate drain state), got %q", sm.State("test-bu"))
	}
}

func TestFailover_NormalOperation(t *testing.T) {
	c, _ := newTestController("primary", config.ModeActivePassive, testHC())

	recordSuccesses(c, "primary", 2)
	recordSuccesses(c, "secondary", 2)

	action := c.Evaluate()
	if action != ActionNone {
		t.Errorf("normal operation: got %q, want none", action)
	}
}

// ── Circuit breaker ───────────────────────────────────────────────────

func TestCircuitBreaker_TripsAfterMaxFailovers(t *testing.T) {
	hc := testHC()
	hc.CircuitBreakerMaxFailovers = 2
	hc.CircuitBreakerWindow = config.Duration(300 * time.Second)
	hc.AutoFailback = true
	c, _ := newTestController("primary", config.ModeActivePassive, hc)

	// Make both healthy.
	recordSuccesses(c, "primary", 2)
	recordSuccesses(c, "secondary", 2)

	// First failover: primary unhealthy, secondary healthy →  (window=1).
	recordFailures(c, "primary", 3)
	action := c.Evaluate()
	if action != ActionFailoverToSecondary {
		t.Fatalf("first failover: got %q", action)
	}
	if c.IsCircuitBroken() {
		t.Error("should not be broken after 1 failover")
	}
	if c.FailoverWindowCount() != 1 {
		t.Errorf("window count after 1 failover: got %d, want 1", c.FailoverWindowCount())
	}

	// During DRAINING, both clusters become unhealthy → .
	// Then primary recovers → back to primary. Secondary recovers → no action.
	recordFailures(c, "secondary", 3)
	c.Evaluate() // enters 

	// Recover from : primary healthy → back to primary.
	recordSuccesses(c, "primary", 2)
	c.Evaluate()

	// Reset to simulate second independent failover.
	// Make primary unhealthy, secondary healthy → failover again (window=2, circuit breaks).
	recordSuccesses(c, "secondary", 2)
	recordFailures(c, "primary", 3)
	action = c.Evaluate()
	if action != ActionFailoverToSecondary {
		t.Fatalf("second failover: got %q", action)
	}

	if !c.IsCircuitBroken() {
		t.Error("should be broken after 2 failover events")
	}
	if c.CircuitBreakCount() != 1 {
		t.Errorf("circuit_break_count: got %d, want 1", c.CircuitBreakCount())
	}
	if c.FailoverWindowCount() != 2 {
		t.Errorf("window count after trip: got %d, want 2", c.FailoverWindowCount())
	}

	// After circuit broken, all subsequent evaluates return none.
	recordSuccesses(c, "secondary", 2)
	recordFailures(c, "primary", 3)
	action = c.Evaluate()
	if action != ActionNone {
		t.Errorf("circuit broken should ignore health: got %q, want none", action)
	}
}

func TestCircuitBreaker_ResetReenables(t *testing.T) {
	hc := testHC()
	hc.CircuitBreakerMaxFailovers = 1
	c, sm := newTestController("primary", config.ModeActivePassive, hc)

	recordSuccesses(c, "secondary", 2)
	recordFailures(c, "primary", 3)
	c.Evaluate()

	if !c.IsCircuitBroken() {
		t.Fatal("expected circuit broken")
	}

	// Reset circuit breaker and state machine back to PRIMARIO.
	c.ResetCircuitBreaker()
	_ = sm.Transition("test-bu", StatePrimary, ReasonConfigChange)

	if c.IsCircuitBroken() {
		t.Error("circuit should be reset")
	}

	// Should be able to failover again.
	recordFailures(c, "primary", 3)
	recordSuccesses(c, "secondary", 2)
	action := c.Evaluate()
	if action != ActionFailoverToSecondary {
		t.Errorf("after reset: got %q, want failover_to_secondary", action)
	}
	// Window count should be back to 1 after reset cleared timestamps.
	if c.FailoverWindowCount() != 1 {
		t.Errorf("window count after reset and re-failover: got %d, want 1", c.FailoverWindowCount())
	}
}

func TestCircuitBreaker_SlidingWindowPrunesExpiredTimestamps(t *testing.T) {
	// Use a very short window so timestamps expire quickly in the test.
	hc := testHC()
	hc.CircuitBreakerMaxFailovers = 3
	hc.CircuitBreakerWindow = config.Duration(1 * time.Millisecond)
	c, _ := newTestController("primary", config.ModeActivePassive, hc)

	recordSuccesses(c, "primary", 2)
	recordSuccesses(c, "secondary", 2)

	// Trigger 2 failovers quickly (both within the window).
	for i := 0; i < 2; i++ {
		recordFailures(c, "primary", 3)
		c.Evaluate() // failover to 
		// Recover back to primary for next cycle.
		recordSuccesses(c, "primary", 2)
		_ = c.sm.Transition("test-bu", StatePrimary, ReasonRecovery)
	}

	if c.FailoverWindowCount() == 0 {
		t.Skip("timestamps already expired — 1ms window is too short for this machine")
	}

	// Wait for the window to expire.
	time.Sleep(5 * time.Millisecond)

	// After expiry, window count should be 0.
	if got := c.FailoverWindowCount(); got != 0 {
		t.Errorf("window count after expiry: got %d, want 0", got)
	}

	// Should be able to failover again without tripping (since window cleared).
	recordFailures(c, "primary", 3)
	action := c.Evaluate()
	if action != ActionFailoverToSecondary {
		t.Errorf("after window expiry: got %q, want failover_to_secondary", action)
	}
}

func TestCircuitBreaker_SlidingWindowNotTrippedWhenSpreadOut(t *testing.T) {
	// Failovers spread out over time should not trip the breaker if they
	// don't accumulate fast enough within the window.
	hc := testHC()
	hc.CircuitBreakerMaxFailovers = 3
	hc.CircuitBreakerWindow = config.Duration(50 * time.Millisecond)
	c, _ := newTestController("primary", config.ModeActivePassive, hc)

	recordSuccesses(c, "primary", 2)
	recordSuccesses(c, "secondary", 2)

	// First failover.
	recordFailures(c, "primary", 3)
	c.Evaluate()
	recordSuccesses(c, "primary", 2)
	_ = c.sm.Transition("test-bu", StatePrimary, ReasonRecovery)

	// Wait longer than the window.
	time.Sleep(60 * time.Millisecond)

	// Second failover — first timestamp should be expired.
	recordFailures(c, "primary", 3)
	c.Evaluate()
	recordSuccesses(c, "primary", 2)
	_ = c.sm.Transition("test-bu", StatePrimary, ReasonRecovery)

	time.Sleep(60 * time.Millisecond)

	// Third failover — first two should be expired.
	recordFailures(c, "primary", 3)
	action := c.Evaluate()
	if action != ActionFailoverToSecondary {
		t.Errorf("third failover: got %q, want failover_to_secondary", action)
	}
	if c.IsCircuitBroken() {
		t.Error("should NOT be broken — failovers spread out over time")
	}
	if c.FailoverWindowCount() != 1 {
		t.Errorf("window count after spread-out failovers: got %d, want 1", c.FailoverWindowCount())
	}
}

func TestCircuitBreaker_ResetClearsWindowTimestamps(t *testing.T) {
	hc := testHC()
	hc.CircuitBreakerMaxFailovers = 2
	hc.CircuitBreakerWindow = config.Duration(300 * time.Second)
	c, _ := newTestController("primary", config.ModeActivePassive, hc)

	recordSuccesses(c, "primary", 2)
	recordSuccesses(c, "secondary", 2)

	// Trigger 2 failovers to trip the breaker.
	for i := 0; i < 2; i++ {
		recordFailures(c, "primary", 3)
		c.Evaluate()
		recordSuccesses(c, "primary", 2)
		_ = c.sm.Transition("test-bu", StatePrimary, ReasonRecovery)
	}

	if !c.IsCircuitBroken() {
		t.Fatal("expected circuit broken")
	}

	// Reset should clear timestamps and broken flag.
	c.ResetCircuitBreaker()

	if c.IsCircuitBroken() {
		t.Error("circuit should be reset")
	}
	if c.FailoverWindowCount() != 0 {
		t.Errorf("window count after reset: got %d, want 0", c.FailoverWindowCount())
	}
}

func TestCircuitBreaker_WindowCountAccurate(t *testing.T) {
	hc := testHC()
	hc.CircuitBreakerMaxFailovers = 10
	hc.CircuitBreakerWindow = config.Duration(300 * time.Second)
	c, _ := newTestController("primary", config.ModeActivePassive, hc)

	if c.FailoverWindowCount() != 0 {
		t.Errorf("initial window count: got %d, want 0", c.FailoverWindowCount())
	}

	recordSuccesses(c, "primary", 2)
	recordSuccesses(c, "secondary", 2)

	// Trigger 3 failovers.
	for i := 0; i < 3; i++ {
		recordFailures(c, "primary", 3)
		c.Evaluate()
		recordSuccesses(c, "primary", 2)
		_ = c.sm.Transition("test-bu", StatePrimary, ReasonRecovery)
	}

	if c.FailoverWindowCount() != 3 {
		t.Errorf("window count after 3 failovers: got %d, want 3", c.FailoverWindowCount())
	}
}

// ── Metrics ───────────────────────────────────────────────────────────

func TestMetrics_FailoverCount(t *testing.T) {
	hc := testHC()
	hc.CircuitBreakerMaxFailovers = 100
	c, _ := newTestController("primary", config.ModeActivePassive, hc)

	recordSuccesses(c, "primary", 2)
	recordSuccesses(c, "secondary", 2)

	for i := 0; i < 3; i++ {
		// Primary unhealthy, secondary healthy → failover to DRAINING.
		recordFailures(c, "primary", 3)
		recordSuccesses(c, "secondary", 2)
		c.Evaluate()

		// Primary recovers, secondary still healthy → handle by recovering
		// and preparing for next cycle.
		// DRAINING state: if primary and secondary are both healthy, the
		// drain completes (via DRCoordinator in production) and transitions
		// to Secondary. For the test, manually set state back to primary
		// to prepare for next cycle.
		recordSuccesses(c, "primary", 2)
		_ = c.sm.Transition("test-bu", StatePrimary, ReasonRecovery)
	}

	if got := c.FailoverCount(); got != 3 {
		// 3 failovers each going through DRAINING
		t.Errorf("failover_count: got %d, want 3", got)
	}
}

// ── load_balance mode ─────────────────────────────────────────────────

func TestLoadBalance_BothDown(t *testing.T) {
	c, sm := newTestController("primary", config.ModeLoadBalance, testHC())

	recordFailures(c, "primary", 3)
	recordFailures(c, "secondary", 3)
	c.Evaluate()

	if sm.State("test-bu") != StateBothDown {
		t.Errorf("load_balance : got %q, want ", sm.State("test-bu"))
	}
}

// ── Thread safety ─────────────────────────────────────────────────────

func TestConcurrentAccess(t *testing.T) {
	c, _ := newTestController("primary", config.ModeActivePassive, testHC())

	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				c.RecordHealthResult("primary", true, 10*time.Millisecond)
				c.RecordHealthResult("secondary", true, 10*time.Millisecond)
				c.Evaluate()
				c.BothDownCount()
				c.BothDownActiveCluster()
				c.IsCircuitBroken()
				c.CircuitBreakCount()
				c.FailoverCount()
				c.FailoverWindowCount()
				c.PrimaryHealthy()
				c.SecondaryHealthy()
			}
			done <- struct{}{}
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

// ── Defaults ──────────────────────────────────────────────────────────

func TestDefaults_UsedWhenZero(t *testing.T) {
	hc := config.HealthCheckConfig{
		Enabled: true,
	}
	c, _ := newTestController("primary", config.ModeActivePassive, hc)

	recordSuccesses(c, "primary", 2)
	if !c.PrimaryHealthy() {
		t.Error("primary should be healthy (default recovery_threshold=2)")
	}

	c.RecordHealthResult("primary", false, 0)
	c.RecordHealthResult("primary", false, 0)
	if !c.PrimaryHealthy() {
		t.Error("should still be healthy after 2 failures (default failure_threshold=3)")
	}

	c.RecordHealthResult("primary", false, 0) // 3rd failure
	if c.PrimaryHealthy() {
		t.Error("should be unhealthy after 3 failures (default failure_threshold=3)")
	}
}

// ── Benchmarks ────────────────────────────────────────────────────────

func BenchmarkEvaluate_NormalOperation(b *testing.B) {
	hc := testHC()
	c, _ := newTestController("primary", config.ModeActivePassive, hc)
	recordSuccesses(c, "primary", 2)
	recordSuccesses(c, "secondary", 2)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Evaluate()
	}
}

func BenchmarkEvaluate_BothDown(b *testing.B) {
	hc := testHC()
	c, _ := newTestController("primary", config.ModeActivePassive, hc)
	recordFailures(c, "primary", 3)
	recordFailures(c, "secondary", 3)
	c.Evaluate()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Evaluate()
	}
}

// ── unused import guard ───────────────────────────────────────────────
var _ = atomic.AddInt64

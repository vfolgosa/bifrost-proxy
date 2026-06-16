// Package failover — see state.go for the DR state machine and
// rebalance.go for load_balance weight management.
//
// controller.go adds the autonomous failover decision algorithm for
// active_passive mode, including BOTH_DOWN handling.
package failover

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/vfolgosa/bifrost-proxy/internal/config"
	"github.com/vfolgosa/bifrost-proxy/internal/logger"
)

// ── Health tracking ───────────────────────────────────────────────────

// clusterHealth tracks per-cluster health for autonomous failover decisions.
type clusterHealth struct {
	healthy              bool
	consecutiveFailures  int
	consecutiveSuccesses int
	lastCheckAt          time.Time
	lastCheckLatency     time.Duration
	upSince              time.Time // zero if unhealthy
}

// ── Controller ────────────────────────────────────────────────────────

// Controller manages autonomous failover for a single BU in active_passive
// mode. It integrates with the shared StateMachine (state.go) for the DR
// state graph and callbacks, and adds health tracking with the failover
// decision algorithm from proxy-spec.md §5.6.2.
//
// Key responsibilities:
//   - Record health check results per cluster
//   - Run the failover decision algorithm after each health cycle
//   - Transition the StateMachine on failover/failback/recovery events
//   - Handle BOTH_DOWN: hold last active, log ERROR, expose metric,
//     continue checks, first to recover gets traffic, never flap
type Controller struct {
	mu        sync.Mutex
	buName    string
	sm        *StateMachine
	hcCfg     config.HealthCheckConfig
	mode      string // "active_passive" or "load_balance"
	active    string // configured active cluster ("primary" or "secondary")

	primaryHealth   clusterHealth
	secondaryHealth clusterHealth

	// Anti-flap / circuit breaker
	lastFailoverAt       time.Time
	failoverTimestamps   []time.Time // sliding window of failover events
	circuitBroken        bool
	circuitBrokenAt      time.Time
	lastActiveBeforeAmbos string // remembered when entering BOTH_DOWN

	// Metrics
	ambosDownCount    int64
	failoverCount     int64
	circuitBreakCount int64
}

// NewController creates a Controller for one BU. The caller must provide
// the shared StateMachine, which should already have this BU initialized.
func NewController(
	buName, active, mode string,
	hcCfg config.HealthCheckConfig,
	sm *StateMachine,
) *Controller {
	c := &Controller{
		buName: buName,
		sm:     sm,
		hcCfg:  hcCfg,
		mode:   mode,
		active: active,
	}

	return c
}

// ── Health recording ──────────────────────────────────────────────────

// RecordHealthResult updates health tracking for one cluster.
func (c *Controller) RecordHealthResult(cluster string, healthy bool, latency time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ch := c.healthFor(cluster)
	ch.lastCheckAt = time.Now()
	ch.lastCheckLatency = latency

	ft := c.hcCfg.FailureThreshold
	if ft <= 0 {
		ft = config.DefaultFailureThreshold
	}
	rt := c.hcCfg.RecoveryThreshold
	if rt <= 0 {
		rt = config.DefaultRecoveryThreshold
	}

	if healthy {
		ch.consecutiveFailures = 0
		ch.consecutiveSuccesses++

		if ch.consecutiveSuccesses >= rt && !ch.healthy {
			ch.healthy = true
			ch.upSince = time.Now()
			logger.L().Info("cluster is now UP",
				"bu", c.buName, "cluster", cluster, "recovery_threshold", rt)
		}
	} else {
		ch.consecutiveSuccesses = 0
		ch.consecutiveFailures++

		if ch.consecutiveFailures >= ft && ch.healthy {
			ch.healthy = false
			ch.upSince = time.Time{}
			logger.L().Warn("cluster is now DOWN",
				"bu", c.buName, "cluster", cluster, "failure_threshold", ft)
		}
	}

	c.setHealthFor(cluster, *ch)
}

// ── Decision algorithm ────────────────────────────────────────────────

// Evaluate runs the failover decision algorithm. Returns the action taken
// (for observability) and transitions the StateMachine as needed.
func (c *Controller) Evaluate() Action {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.evaluateLocked()
}

func (c *Controller) evaluateLocked() Action {
	// Circuit breaker
	if c.circuitBroken {
		return ActionNone
	}

	primHealthy := c.primaryHealth.healthy
	secHealthy := c.secondaryHealth.healthy

	// ── BOTH_DOWN ──
	if !primHealthy && !secHealthy {
		current := c.sm.State(c.buName)
		if current != StateBothDown {
			c.enterBothDownLocked(string(current))
		}
		return ActionNone
	}

	// ── Recovering from BOTH_DOWN ──
	current := c.sm.State(c.buName)
	if current == StateBothDown {
		if primHealthy {
			logger.L().Info("BOTH_DOWN recovered: primary healthy, switching to PRIMARIO",
				"bu", c.buName)
			_ = c.sm.Transition(c.buName, StatePrimary, ReasonRecovery)
		} else if secHealthy {
			logger.L().Info("BOTH_DOWN recovered: secondary healthy, switching to SECUNDARIO",
				"bu", c.buName)
			_ = c.sm.Transition(c.buName, StateSecondary, ReasonRecovery)
		}
		return ActionNone
	}

	// ── active_passive failover ──
	if c.mode == config.ModeActivePassive {
		// Failover: primary → secondary
		if current == StatePrimary && !primHealthy && secHealthy {
			if c.canFailoverLocked() {
				if c.hcCfg.RequireTargetHealthy && !secHealthy {
					return ActionNone
				}
				return c.triggerFailoverLocked(StatePrimary, StateSecondary)
			}
			return ActionNone
		}

		// Failback: secondary → primary
		if current == StateSecondary && primHealthy && c.hcCfg.AutoFailback {
			if c.canFailoverLocked() {
				return c.triggerFailoverLocked(StateSecondary, StatePrimary)
			}
			return ActionNone
		}

		// During DRAINING: if the old cluster recovers but the target is
		// still down, abort the drain and go back.
		if current == StateDraining && primHealthy && !secHealthy {
			_ = c.sm.Transition(c.buName, StatePrimary, ReasonRecovery)
			logger.L().Info("DRAINING aborted: primary recovered, back to PRIMARIO",
				"bu", c.buName)
			return ActionNone
		}
		if current == StateDraining && !primHealthy && secHealthy {
			_ = c.sm.Transition(c.buName, StateSecondary, ReasonRecovery)
			logger.L().Info("DRAINING aborted: secondary recovered, back to SECUNDARIO",
				"bu", c.buName)
			return ActionNone
		}
	}

	return ActionNone
}

// ── BOTH_DOWN ────────────────────────────────────────────────────────

func (c *Controller) enterBothDownLocked(from string) {
	_ = c.sm.Transition(c.buName, StateBothDown, ReasonHealthEvent)
	atomic.AddInt64(&c.ambosDownCount, 1)

	// Resolve the active cluster before BOTH_DOWN.
	// If we were in DRAINING, the real active is the destination cluster
	// (where new connections were routing). If from is a concrete cluster
	// (PRIMARIO/SECUNDARIO), use it directly.
	var activeBefore string
	if from == string(StateDraining) {
		// During drain, new connections go to the target.
		// Determine target from the drain config.
		if c.active == config.ActivePrimary {
			// Was primary active → draining to secondary
			activeBefore = string(StateSecondary)
		} else {
			// Was secondary active → draining to primary
			activeBefore = string(StatePrimary)
		}
	} else if from != "" {
		activeBefore = from
	} else {
		activeBefore = string(StatePrimary)
	}
	c.lastActiveBeforeAmbos = activeBefore

	logger.L().Error("BOTH_DOWN: both primary and secondary clusters unhealthy",
		"bu", c.buName, "last_active", c.lastActiveBeforeAmbos,
		"_count", atomic.LoadInt64(&c.ambosDownCount))
}

// BothDownActiveCluster returns the cluster that was active before
// BOTH_DOWN. This is the cluster traffic should continue routing to
// while both are down — never flap between two broken clusters.
func (c *Controller) BothDownActiveCluster() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lastActiveBeforeAmbos == "" {
		return c.active // fallback to configured active
	}
	return c.lastActiveBeforeAmbos
}

// BothDownCount returns the total times this BU entered BOTH_DOWN.
func (c *Controller) BothDownCount() int64 {
	return atomic.LoadInt64(&c.ambosDownCount)
}

// ── Circuit breaker ───────────────────────────────────────────────────

func (c *Controller) IsCircuitBroken() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.circuitBroken
}

func (c *Controller) CircuitBreakCount() int64 {
	return atomic.LoadInt64(&c.circuitBreakCount)
}

func (c *Controller) FailoverCount() int64 {
	return atomic.LoadInt64(&c.failoverCount)
}

// FailoverWindowCount returns the number of failovers currently in the
// sliding window (after pruning expired entries). Useful for metrics and
// tests. Caller must not hold c.mu.
func (c *Controller) FailoverWindowCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	windowDur := c.hcCfg.CircuitBreakerWindow.Duration()
	if windowDur > 0 {
		c.pruneFailoverTimestampsLocked(windowDur)
	}
	return len(c.failoverTimestamps)
}

func (c *Controller) ResetCircuitBreaker() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.circuitBroken {
		logger.L().Info("circuit breaker reset — autonomous re-enabled",
			"bu", c.buName)
	}
	c.circuitBroken = false
	c.circuitBrokenAt = time.Time{}
	c.failoverTimestamps = nil
}

// ── Health accessors ──────────────────────────────────────────────────

func (c *Controller) PrimaryHealthy() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.primaryHealth.healthy
}

func (c *Controller) SecondaryHealthy() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.secondaryHealth.healthy
}

// ── Internal helpers ──────────────────────────────────────────────────

func (c *Controller) canFailoverLocked() bool {
	cooldown := c.hcCfg.MinTimeBetweenFailovers.Duration()
	if cooldown > 0 && time.Since(c.lastFailoverAt) < cooldown {
		return false
	}

	// Circuit breaker sliding window check.
	windowDur := c.hcCfg.CircuitBreakerWindow.Duration()
	maxFO := c.hcCfg.CircuitBreakerMaxFailovers
	if maxFO > 0 && windowDur > 0 {
		c.pruneFailoverTimestampsLocked(windowDur)
		if len(c.failoverTimestamps) >= maxFO {
			return false
		}
	}

	return true
}

func (c *Controller) triggerFailoverLocked(from, to BUState) Action {
	reason := ReasonHealthEvent
	if c.circuitBroken {
		return ActionNone
	}

	// Determine cluster names for logging
	fromCluster, toCluster := "primary", "secondary"
	if from == StateSecondary {
		fromCluster, toCluster = "secondary", "primary"
	}

	c.lastFailoverAt = time.Now()
	c.failoverTimestamps = append(c.failoverTimestamps, time.Now())
	atomic.AddInt64(&c.failoverCount, 1)

	// Transition to DRAINING first — the DRCoordinator will handle the
	// actual drain and subsequent transition to the target state.
	_ = c.sm.Transition(c.buName, StateDraining, reason)

	logger.L().Warn("failover initiated",
		"bu", c.buName, "from", fromCluster, "to", toCluster,
		"trigger", "health_check")

	// Circuit breaker — sliding window check.
	maxFO := c.hcCfg.CircuitBreakerMaxFailovers
	windowDur := c.hcCfg.CircuitBreakerWindow.Duration()
	if maxFO > 0 && windowDur > 0 {
		c.pruneFailoverTimestampsLocked(windowDur)
		if len(c.failoverTimestamps) >= maxFO {
			c.circuitBroken = true
			c.circuitBrokenAt = time.Now()
			atomic.AddInt64(&c.circuitBreakCount, 1)
			logger.L().Error("circuit breaker tripped",
				"bu", c.buName, "failover_count", len(c.failoverTimestamps),
				"window", windowDur.String(),
				"autonomous_failover", "DISABLED")
		}
	}

	action := ActionFailoverToSecondary
	if from == StateSecondary {
		action = ActionFailoverToPrimary
	}
	return action
}

// pruneFailoverTimestampsLocked removes timestamps older than window from
// the sliding window. Caller must hold c.mu.
func (c *Controller) pruneFailoverTimestampsLocked(window time.Duration) {
	cutoff := time.Now().Add(-window)
	first := len(c.failoverTimestamps)
	for i, ts := range c.failoverTimestamps {
		if ts.After(cutoff) {
			first = i
			break
		}
	}
	c.failoverTimestamps = c.failoverTimestamps[first:]
}

func (c *Controller) healthFor(cluster string) *clusterHealth {
	switch cluster {
	case "primary":
		return &c.primaryHealth
	case "secondary":
		return &c.secondaryHealth
	default:
		return nil
	}
}

func (c *Controller) setHealthFor(cluster string, ch clusterHealth) {
	switch cluster {
	case "primary":
		c.primaryHealth = ch
	case "secondary":
		c.secondaryHealth = ch
	}
}

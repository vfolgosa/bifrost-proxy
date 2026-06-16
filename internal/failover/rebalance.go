// Package failover implements health-aware weighted load balancing with
// asymmetric recovery (fast failover, slow failback) for the Kafka proxy's
// load_balance cluster mode.
//
// Key behaviors:
//   - On consecutive failures >= failure_threshold: endpoint weight drops to
//     zero immediately (fast failover). All traffic shifts to the healthy
//     endpoint.
//   - On recovery: endpoint enters RECOVERING state. It must accumulate
//     recovery_threshold consecutive successful health probes AND remain UP
//     for at least recovery_min_uptime before its original weight is restored.
//   - Ping-pong prevention: a min_time_between_failovers cooldown prevents
//     rapid oscillation between UP and DOWN states.
//   - Circuit breaker: if failovers exceed circuit_breaker_max_failovers
//     within circuit_breaker_window, the endpoint is locked in DOWN state
//     until manual intervention.
package failover

import (
	"math/rand"
	"sync"
	"time"

	"github.com/vfolgosa/bifrost-proxy/internal/config"
	"github.com/vfolgosa/bifrost-proxy/internal/logger"
)

// ── Status constants ──────────────────────────────────────────────────

const (
	StatusUp         = "up"
	StatusDown       = "down"
	StatusRecovering = "recovering"
)

// ── Defaults (matching config defaults) ───────────────────────────────

const (
	defaultFailureThreshold  = 3
	defaultRecoveryThreshold = 5
	defaultRecoveryMinUptime = 120 * time.Second
)

// ── Rebalancer ────────────────────────────────────────────────────────

// Rebalancer manages asymmetric recovery for a load_balance cluster. It
// tracks health state for primary and secondary endpoints independently
// and implements weighted random target selection.
//
// State machine:
//
//	UP ──── failures >= failure_threshold ────► DOWN
//	DOWN ── first success ───────────────────► RECOVERING
//	RECOVERING ── successes >= recovery_threshold &&
//	              uptime >= recovery_min_uptime ──► UP
//	RECOVERING ── any failure ────────────────► DOWN
//
// Effective weights:
//
//	UP:         originalWeight
//	DOWN:       0
//	RECOVERING: 0
//
// Both endpoints DOWN: original weights used (best-effort, nothing to lose).
type Rebalancer struct {
	mu sync.RWMutex

	primary   endpointState
	secondary endpointState

	// Original configured weights (0..100, must sum to 100).
	origPrimaryWeight   int
	origSecondaryWeight int

	// Recovery parameters.
	failureThreshold        int
	recoveryThreshold       int
	recoveryMinUptime       time.Duration
	minTimeBetweenFailovers time.Duration

	// Circuit breaker.
	circuitBreakerMaxFailovers int
	circuitBreakerWindow       time.Duration
	failoverHistory            []time.Time

	// Ping-pong prevention.
	lastFailoverAt map[string]time.Time // addr -> last failover timestamp
}

// endpointState tracks the health status of a single broker endpoint.
type endpointState struct {
	addr    string
	status  string    // "up", "down", "recovering"
	upSince time.Time // when the endpoint entered RECOVERING state

	consecutiveSuccess int
	consecutiveFailure int

	// Original weight from config.
	originalWeight int

	// Effective weight used for routing decisions.
	// 0 when DOWN or RECOVERING; originalWeight when UP.
	effectiveWeight int
}

// NewRebalancer creates a Rebalancer from cluster configuration.
// It uses the HealthCheckConfig values if present, falling back to defaults.
func NewRebalancer(cfg config.ClusterConfig) *Rebalancer {
	ft := cfg.HealthCheck.FailureThreshold
	if ft <= 0 {
		ft = defaultFailureThreshold
	}
	rt := cfg.HealthCheck.RecoveryThreshold
	if rt <= 0 {
		rt = defaultRecoveryThreshold
	}
	rmu := cfg.HealthCheck.RecoveryMinUptime.Duration()
	if rmu <= 0 {
		rmu = defaultRecoveryMinUptime
	}

	r := &Rebalancer{
		origPrimaryWeight:           cfg.Primary.Weight,
		origSecondaryWeight:         cfg.Secondary.Weight,
		failureThreshold:             ft,
		recoveryThreshold:            rt,
		recoveryMinUptime:            rmu,
		minTimeBetweenFailovers:      cfg.HealthCheck.MinTimeBetweenFailovers.Duration(),
		circuitBreakerMaxFailovers:   cfg.HealthCheck.CircuitBreakerMaxFailovers,
		circuitBreakerWindow:         cfg.HealthCheck.CircuitBreakerWindow.Duration(),
		failoverHistory:              make([]time.Time, 0),
		lastFailoverAt:               make(map[string]time.Time),
	}

	r.primary = endpointState{
		addr:            cfg.Primary.Bootstrap,
		status:          StatusUp,
		upSince:         time.Now(),
		originalWeight:  cfg.Primary.Weight,
		effectiveWeight: cfg.Primary.Weight,
	}
	r.secondary = endpointState{
		addr:            cfg.Secondary.Bootstrap,
		status:          StatusUp,
		upSince:         time.Now(),
		originalWeight:  cfg.Secondary.Weight,
		effectiveWeight: cfg.Secondary.Weight,
	}

	return r
}

// ── Public API ────────────────────────────────────────────────────────

// SelectTarget returns the upstream address to route to, chosen via weighted
// random selection based on current effective weights.
//
// If both endpoints are DOWN, original weights are used (best-effort routing).
// If one endpoint is UP and the other is DOWN/RECOVERING, all traffic goes
// to the UP endpoint.
func (r *Rebalancer) SelectTarget() string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	pw := r.primary.effectiveWeight
	sw := r.secondary.effectiveWeight

	// If both are zero (both down), fall back to original weights.
	if pw == 0 && sw == 0 {
		pw = r.primary.originalWeight
		sw = r.secondary.originalWeight
	}

	// If total weight is zero (should not happen with valid config), pick
	// primary.
	total := pw + sw
	if total == 0 {
		return r.primary.addr
	}

	roll := rand.Intn(total)
	if roll < pw {
		return r.primary.addr
	}
	return r.secondary.addr
}

// RecordSuccess registers a successful connection/request to the given
// broker address. This drives the recovery state machine.
func (r *Rebalancer) RecordSuccess(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ep := r.endpointFor(addr)
	if ep == nil {
		return
	}

	ep.consecutiveFailure = 0
	ep.consecutiveSuccess++

	switch ep.status {
	case StatusDown:
		// First success after being down → start recovering.
		ep.status = StatusRecovering
		ep.consecutiveSuccess = 1
		ep.upSince = time.Now()
		logger.L().Info("endpoint transitioning from DOWN to RECOVERING",
			"addr", ep.addr)

	case StatusRecovering:
		uptime := time.Since(ep.upSince)
		if ep.consecutiveSuccess >= r.recoveryThreshold &&
			uptime >= r.recoveryMinUptime {
			// Both conditions met → promote to UP.
			r.promoteToUp(ep)
		} else {
			// Still recovering — log progress.
			if uptime < r.recoveryMinUptime {
				need := r.recoveryMinUptime - uptime
				logger.L().Info("endpoint recovery progress (uptime not met)",
					"addr", ep.addr,
					"uptime", uptime.Truncate(time.Second).String(),
					"remaining", need.Truncate(time.Second).String(),
					"successes", ep.consecutiveSuccess,
					"threshold", r.recoveryThreshold)
			} else {
				logger.L().Info("endpoint recovery progress (successes not met)",
					"addr", ep.addr,
					"needed", r.recoveryThreshold-ep.consecutiveSuccess,
					"successes", ep.consecutiveSuccess,
					"threshold", r.recoveryThreshold)
			}
		}

	case StatusUp:
		// Already up — nothing to do.
	}
}

// RecordFailure registers a failed connection/request to the given broker
// address. This drives the failover state machine.
func (r *Rebalancer) RecordFailure(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ep := r.endpointFor(addr)
	if ep == nil {
		return
	}

	ep.consecutiveSuccess = 0
	ep.consecutiveFailure++

	switch ep.status {
	case StatusUp:
		if ep.consecutiveFailure >= r.failureThreshold {
			r.demoteToDown(ep)
		} else {
			logger.L().Info("endpoint failure recorded (still UP)",
				"addr", ep.addr,
				"failures", ep.consecutiveFailure,
				"threshold", r.failureThreshold)
		}

	case StatusRecovering:
		// Any failure during recovery resets back to DOWN.
		ep.status = StatusDown
		ep.effectiveWeight = 0
		ep.consecutiveSuccess = 0
		ep.consecutiveFailure = 1
		logger.L().Info("endpoint transitioning from RECOVERING to DOWN",
		"addr", ep.addr,
		"reason", "failure during recovery")

	case StatusDown:
		// Already down — track but don't count toward failover.
		ep.consecutiveFailure = 1 // Reset to 1 since we don't want stale counts.
	}
}

// GetEffectiveWeight returns the current effective weight for the given
// endpoint name ("primary" or "secondary"). Returns -1 if unknown.
func (r *Rebalancer) GetEffectiveWeight(name string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	switch name {
	case config.ActivePrimary:
		return r.primary.effectiveWeight
	case config.ActiveSecondary:
		return r.secondary.effectiveWeight
	default:
		return -1
	}
}

// Status returns the current health status of an endpoint ("up", "down",
// or "recovering"). Returns empty string if unknown.
func (r *Rebalancer) Status(name string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	switch name {
	case config.ActivePrimary:
		return r.primary.status
	case config.ActiveSecondary:
		return r.secondary.status
	default:
		return ""
	}
}

// PrimaryAddr returns the primary bootstrap address.
func (r *Rebalancer) PrimaryAddr() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.primary.addr
}

// SecondaryAddr returns the secondary bootstrap address.
func (r *Rebalancer) SecondaryAddr() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.secondary.addr
}

// ── Internal state transitions ────────────────────────────────────────

// endpointFor returns a pointer to the endpoint state matching the address,
// or nil if no endpoint matches.
func (r *Rebalancer) endpointFor(addr string) *endpointState {
	if r.primary.addr == addr {
		return &r.primary
	}
	if r.secondary.addr == addr {
		return &r.secondary
	}
	return nil
}

// promoteToUp transitions an endpoint from RECOVERING to UP, restoring its
// original weight. Called under mu.Lock.
func (r *Rebalancer) promoteToUp(ep *endpointState) {
	// Ping-pong prevention: enforce min_time_between_failovers.
	if r.minTimeBetweenFailovers > 0 {
		if last, ok := r.lastFailoverAt[ep.addr]; ok {
			if time.Since(last) < r.minTimeBetweenFailovers {
				remaining := r.minTimeBetweenFailovers - time.Since(last)
				logger.L().Info("cooldown active, promotion deferred",
					"addr", ep.addr,
					"remaining", remaining.Truncate(time.Second).String())
				return
			}
		}
	}

	// Circuit breaker check.
	if r.circuitBreakerMaxFailovers > 0 && r.circuitBreakerWindow > 0 {
		r.pruneFailoverHistory()
		if len(r.failoverHistory) >= r.circuitBreakerMaxFailovers {
			logger.L().Error("circuit breaker tripped, staying DOWN",
				"addr", ep.addr,
				"failover_count", len(r.failoverHistory),
				"window", r.circuitBreakerWindow.String())
			return
		}
	}

	ep.status = StatusUp
	ep.effectiveWeight = ep.originalWeight
	ep.consecutiveFailure = 0
	ep.consecutiveSuccess = 0
	logger.L().Info("endpoint transitioning from RECOVERING to UP",
		"addr", ep.addr,
		"weight", ep.originalWeight)
}

// demoteToDown transitions an endpoint from UP to DOWN, zeroing its weight.
// Called under mu.Lock.
func (r *Rebalancer) demoteToDown(ep *endpointState) {
	ep.status = StatusDown
	ep.effectiveWeight = 0
	ep.consecutiveSuccess = 0

	now := time.Now()
	r.lastFailoverAt[ep.addr] = now

	// Track failover for circuit breaker.
	if r.circuitBreakerMaxFailovers > 0 {
		r.failoverHistory = append(r.failoverHistory, now)
	}

	logger.L().Info("endpoint transitioning from UP to DOWN",
		"addr", ep.addr,
		"original_weight", ep.originalWeight,
		"consecutive_failures", ep.consecutiveFailure)
}

// pruneFailoverHistory removes failover entries older than the circuit
// breaker window. Called under mu.Lock.
func (r *Rebalancer) pruneFailoverHistory() {
	cutoff := time.Now().Add(-r.circuitBreakerWindow)
	kept := r.failoverHistory[:0]
	for _, t := range r.failoverHistory {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	r.failoverHistory = kept
}

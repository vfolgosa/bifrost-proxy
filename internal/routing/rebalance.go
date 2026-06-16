// Package routing provides auto-rebalance for load_balance mode clusters.
//
// The Rebalancer monitors cluster health via the health checker and
// automatically adjusts effective routing weights:
//
//   - On DOWN transition: immediately shifts the down endpoint's weight
//     to the healthy endpoint (0/100 or 100/0).
//   - On UP transition for secondary: immediately restores original weights.
//   - On UP transition for primary: requires recovery_threshold (5 consecutive
//     successful health checks) AND recovery_min_uptime (120s) before
//     restoring original weights.
//
// The recovery_threshold is enforced by the health checker itself (the IsHealthy()
// transition already requires N consecutive successes). The rebalancer
// additionally enforces the recovery_min_uptime delay after the health checker
// marks the endpoint as UP.
package routing

import (
	"sync"
	"time"

	"github.com/vfolgosa/bifrost-proxy/internal/config"
	"github.com/vfolgosa/bifrost-proxy/internal/health"
	"github.com/vfolgosa/bifrost-proxy/internal/logger"
)

// ── Health interface ────────────────────────────────────────────────

// RebalanceHealthChecker is the subset of health.Checker used by the
// Rebalancer. Using an interface allows test doubles.
type RebalanceHealthChecker interface {
	Health() map[string]health.ClusterHealth
}

// ── Rebalancer ──────────────────────────────────────────────────────────

// Rebalancer monitors cluster health and adjusts effective weights for
// load_balance mode clusters. Start() launches a background goroutine;
// Stop() shuts it down.
type Rebalancer struct {
	mu      sync.Mutex
	router  *Router
	checker RebalanceHealthChecker
	cfg     *config.Config

	// original stores the configured weights (from config) for each cluster.
	original map[string]clusterWeights

	// primaryRecovering marks clusters whose primary has recovered (IsHealthy)
	// but whose weights have not yet been restored due to recovery_min_uptime.
	primaryRecovering map[string]bool

	// prevHealthy stores the last known healthy state per endpoint, used to
	// detect UP/DOWN transitions.
	prevHealthy map[string]perClusterPrev

	stopCh chan struct{}
	doneCh chan struct{}
}

// perClusterPrev holds the previous healthy state for one cluster's endpoints.
type perClusterPrev struct {
	primaryHealthy   bool
	secondaryHealthy bool
}

// NewRebalancer creates a Rebalancer for all load_balance clusters in the config.
// It captures the original configured weights so they can be restored later.
func NewRebalancer(router *Router, checker RebalanceHealthChecker, cfg *config.Config) *Rebalancer {
	r := &Rebalancer{
		router:             router,
		checker:            checker,
		cfg:                cfg,
		original:           make(map[string]clusterWeights),
		primaryRecovering: make(map[string]bool),
		prevHealthy:        make(map[string]perClusterPrev),
		stopCh:             make(chan struct{}),
		doneCh:             make(chan struct{}),
	}

	// Capture original weights for load_balance clusters.
	for name, cc := range cfg.Clusters {
		if cc.Mode == config.ModeLoadBalance {
			r.original[name] = clusterWeights{
				Primary:   cc.Primary.Weight,
				Secondary: cc.Secondary.Weight,
			}
		}
	}

	return r
}

// Start launches the rebalance goroutine. It is safe to call after Stop.
func (r *Rebalancer) Start() {
	go r.loop()
}

// Stop signals the rebalance goroutine to exit and waits for it.
func (r *Rebalancer) Stop() {
	select {
	case <-r.stopCh:
		return // already stopped
	default:
	}
	close(r.stopCh)
	<-r.doneCh
}

// loop is the background goroutine that periodically checks health and
// adjusts weights. It polls every second — polling is cheap since it only
// reads atomic snapshots.
func (r *Rebalancer) loop() {
	defer close(r.doneCh)

	logger.L().Info("rebalance: loop started")

	// Run an immediate check on start.
	r.tick()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.tick()
		}
	}
}

// tick reads current health, compares against previous state, and adjusts
// weights accordingly.
func (r *Rebalancer) tick() {
	r.mu.Lock()
	defer r.mu.Unlock()

	healthMap := r.checker.Health()
	if healthMap == nil {
		return
	}

	for clusterName, ch := range healthMap {
		prev, hasPrev := r.prevHealthy[clusterName]

		nowHealthy := perClusterPrev{
			primaryHealthy:   ch.Primary.Healthy,
			secondaryHealthy: ch.Secondary.Healthy,
		}

		if !hasPrev {
			// First tick — seed previous state, no transition detection.
			r.prevHealthy[clusterName] = nowHealthy
			continue
		}

		// Detect transitions.
		primaryWentDown := prev.primaryHealthy && !nowHealthy.primaryHealthy
		secondaryWentDown := prev.secondaryHealthy && !nowHealthy.secondaryHealthy
		primaryCameUp := !prev.primaryHealthy && nowHealthy.primaryHealthy
		secondaryCameUp := !prev.secondaryHealthy && nowHealthy.secondaryHealthy

		if primaryWentDown || secondaryWentDown {
			r.handleDown(clusterName, primaryWentDown, secondaryWentDown)
		}

		if secondaryCameUp {
			r.handleSecondaryUp(clusterName)
		}

		if primaryCameUp {
			r.handlePrimaryUp(clusterName, ch.Primary.UpSince)
		}

		// Check primary recovery progress (waiting for min uptime).
		if r.primaryRecovering[clusterName] {
			r.checkPrimaryRecovery(clusterName, ch.Primary.UpSince)
		}

		r.prevHealthy[clusterName] = nowHealthy
	}
}

// handleDown shifts the weight of a down endpoint to the healthy one.
func (r *Rebalancer) handleDown(clusterName string, primaryDown, secondaryDown bool) {
	clusterCfg, ok := r.cfg.Clusters[clusterName]
	if !ok || clusterCfg.Mode != config.ModeLoadBalance {
		return
	}

	if !primaryDown && !secondaryDown {
		return
	}

	var newPrimW, newSecW int
	if primaryDown {
		newPrimW, newSecW = 0, 100
	} else {
		newPrimW, newSecW = 100, 0
	}

	r.router.SetEffectiveWeights(clusterName, newPrimW, newSecW)
	logger.L().Info("rebalance: weights adjusted",
		"cluster", clusterName, "primary_down", primaryDown, "secondary_down", secondaryDown,
		"primary_weight", newPrimW, "secondary_weight", newSecW)
}

// handleSecondaryUp restores original weights immediately.
func (r *Rebalancer) handleSecondaryUp(clusterName string) {
	orig, ok := r.original[clusterName]
	if !ok {
		return
	}
	r.router.SetEffectiveWeights(clusterName, orig.Primary, orig.Secondary)
	logger.L().Info("rebalance: secondary UP, weights restored",
		"cluster", clusterName, "primary_weight", orig.Primary, "secondary_weight", orig.Secondary)
}

// handlePrimaryUp starts tracking primary recovery. Weights stay shifted
// until recovery conditions are met.
func (r *Rebalancer) handlePrimaryUp(clusterName string, upSince time.Time) {
	clusterCfg, ok := r.cfg.Clusters[clusterName]
	if !ok || clusterCfg.Mode != config.ModeLoadBalance {
		return
	}

	recoveryMinUptime := clusterCfg.HealthCheck.RecoveryMinUptime.Duration()
	if recoveryMinUptime <= 0 {
		recoveryMinUptime = 120 * time.Second
	}

	elapsed := time.Since(upSince)
	if elapsed >= recoveryMinUptime {
		// Conditions already met — restore immediately.
		orig, ok := r.original[clusterName]
		if !ok {
			return
		}
		r.router.SetEffectiveWeights(clusterName, orig.Primary, orig.Secondary)
		r.primaryRecovering[clusterName] = false
		logger.L().Info("rebalance: primary UP, recovery conditions met, weights restored",
			"cluster", clusterName, "uptime", elapsed, "primary_weight", orig.Primary, "secondary_weight", orig.Secondary)
	} else {
		// Not enough uptime yet — enter recovery mode.
		r.primaryRecovering[clusterName] = true
		logger.L().Info("rebalance: primary UP, waiting for recovery min uptime",
			"cluster", clusterName, "elapsed", elapsed, "required", recoveryMinUptime)
	}
}

// checkPrimaryRecovery checks whether a recovering primary has met the
// recovery_min_uptime condition and restores weights if so.
func (r *Rebalancer) checkPrimaryRecovery(clusterName string, upSince time.Time) {
	clusterCfg, ok := r.cfg.Clusters[clusterName]
	if !ok || clusterCfg.Mode != config.ModeLoadBalance {
		r.primaryRecovering[clusterName] = false
		return
	}

	recoveryMinUptime := clusterCfg.HealthCheck.RecoveryMinUptime.Duration()
	if recoveryMinUptime <= 0 {
		recoveryMinUptime = 120 * time.Second
	}

	if time.Since(upSince) >= recoveryMinUptime {
		orig, ok := r.original[clusterName]
		if !ok {
			r.primaryRecovering[clusterName] = false
			return
		}
		r.router.SetEffectiveWeights(clusterName, orig.Primary, orig.Secondary)
		r.primaryRecovering[clusterName] = false
		logger.L().Info("rebalance: primary recovery complete, weights restored",
			"cluster", clusterName, "uptime", time.Since(upSince), "required", recoveryMinUptime,
			"primary_weight", orig.Primary, "secondary_weight", orig.Secondary)
	}
}

// ResetWeights restores all effective weights to their configured values.
// Useful on shutdown or reconfiguration.
func (r *Rebalancer) ResetWeights() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for name, orig := range r.original {
		r.router.SetEffectiveWeights(name, orig.Primary, orig.Secondary)
		r.primaryRecovering[name] = false
	}
	logger.L().Info("rebalance: reset all weights to configured values")
}

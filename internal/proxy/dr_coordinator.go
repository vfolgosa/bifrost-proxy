// Package proxy provides the DRCoordinator that integrates the failover
// StateMachine with the connection DrainManager.
//
// The coordinator wires the two components together so that:
//   - StateMachine transitions into DRAINING trigger graceful connection drain.
//   - Drain completion transitions the StateMachine from DRAINING to the
//     target state (PRIMARIO or SECUNDARIO).
//   - Routing decisions during DRAINING consult the coordinator to determine
//     where new connections should go.
//
// Per-BU isolation: each BU has its own state in the StateMachine and an
// independent drain process in the DrainManager.  Draining one BU never
// affects connections on another.
package proxy

import (
	"github.com/vfolgosa/bifrost-proxy/internal/config"
	"github.com/vfolgosa/bifrost-proxy/internal/failover"
	"github.com/vfolgosa/bifrost-proxy/internal/logger"
)

// ControllerLookup resolves the failover Controller for a BU.
type ControllerLookup func(bu string) *failover.Controller

// DRCoordinator wires the failover StateMachine to the connection
// DrainManager, orchestrating the full PRIMARIO → DRAINING → SECUNDARIO
// workflow (and the reverse failback).
type DRCoordinator struct {
	sm          *failover.StateMachine
	dm          *DrainManager
	ctrlLookup  ControllerLookup
	boot        map[string]string // bu → current bootstrap target resolved from coordinator state
}

// SetControllerLookup attaches a failover controller resolver for BOTH_DOWN routing.
func (c *DRCoordinator) SetControllerLookup(fn ControllerLookup) {
	c.ctrlLookup = fn
}

// DrainNewActive returns the drain destination cluster for a BU.
func (c *DRCoordinator) DrainNewActive(bu string) (string, bool) {
	ds := c.dm.DrainState(bu)
	if ds == nil {
		return "", false
	}
	return ds.NewActive, true
}

// NewDRCoordinator creates a coordinator that bridges the StateMachine
// and DrainManager.  After creation, call Wire() to connect callbacks.
func NewDRCoordinator(sm *failover.StateMachine, dm *DrainManager) *DRCoordinator {
	return &DRCoordinator{
		sm:   sm,
		dm:   dm,
		boot: make(map[string]string),
	}
}

// Wire connects the StateMachine and DrainManager callbacks so that:
//
//  1. When the StateMachine transitions a BU into DRAINING, the
//     DrainManager.StartDrain is called for the old→new target.
//  2. When the DrainManager finishes draining, the StateMachine is
//     transitioned from DRAINING to the new target.
//
// Call once before any transitions occur.  Wire panics if either
// component is nil (programmer error).
func (c *DRCoordinator) Wire() {
	// StateMachine callback: start drain on DRAINING.
	// Also handles auto-initialization of new BUs.
	c.sm.OnStateChange(func(t failover.Transition) {
		if t.To != failover.StateDraining {
			return
		}

		oldTarget := stateToTarget(string(t.From))
		newTarget := stateToTarget(string(t.To))

		// When entering DRAINING, we need to know the target.
		// From is the current state (PRIMARIO or SECUNDARIO).
		// We drain from oldTarget to the opposite target.
		switch t.From {
		case failover.StatePrimary:
			newTarget = config.ActiveSecondary
		case failover.StateSecondary:
			newTarget = config.ActivePrimary
		}

		logger.Default().Info("state change, starting drain",
			"bu", t.BU,
			"from_state", string(t.From),
			"to_state", string(t.To),
			"old_active", oldTarget,
			"new_active", newTarget)

		c.dm.StartDrain(t.BU, oldTarget, newTarget, 0)
	})

	// DrainManager callback: complete StateMachine transition on drain finish.
	c.dm.OnDrainComplete(func(clusterName, oldActive, newActive string) {
		current := c.sm.State(clusterName)
		if current != failover.StateDraining {
			// Drain finished but state isn't DRAINING — config may have
			// changed independently.  Don't force a transition.
			logger.Default().Warn("drain complete but state is not DRAINING, skipping transition",
				"cluster", clusterName,
				"current_state", string(current))
			return
		}

		// Map drain target to SM state.
		var targetState failover.BUState
		switch newActive {
		case config.ActivePrimary:
			targetState = failover.StatePrimary
		case config.ActiveSecondary:
			targetState = failover.StateSecondary
		default:
			logger.Default().Warn("drain complete but unknown new_active, skipping transition",
				"cluster", clusterName,
				"new_active", newActive)
			return
		}

		logger.Default().Info("drain complete, transitioning state",
			"cluster", clusterName,
			"from_state", string(current),
			"to_state", string(targetState))

		if err := c.sm.Transition(clusterName, targetState, failover.ReasonDrainComplete); err != nil {
			logger.Default().Error("failed to transition state after drain",
				"cluster", clusterName,
				"error", err)
		}
	})
}

// TargetForRouting returns the effective target cluster (\"primary\" or
// \"secondary\") for new connections to the given BU.  During DRAINING,
// new connections should go to the target (destination) cluster, not the
// old one that is being drained.
//
// Returns the target string and whether the BU is known.
func (c *DRCoordinator) TargetForRouting(bu string) (target string, ok bool) {
	state := c.sm.State(bu)

	switch state {
	case failover.StatePrimary:
		return config.ActivePrimary, true
	case failover.StateSecondary:
		return config.ActiveSecondary, true
	case failover.StateDraining:
		// During drain, we need to know which way we're going.
		// Check the drain state to determine the target.
		drainState := c.dm.DrainState(bu)
		if drainState == nil {
			// Not actively draining — fall through.
			return config.ActivePrimary, true
		}
		return drainState.NewActive, true
	case failover.StateBothDown:
		if c.ctrlLookup != nil {
			if ctrl := c.ctrlLookup(bu); ctrl != nil {
				switch ctrl.BothDownActiveCluster() {
				case string(failover.StatePrimary):
					return config.ActivePrimary, true
				case string(failover.StateSecondary):
					return config.ActiveSecondary, true
				}
			}
		}
		return config.ActivePrimary, true
	default:
		return config.ActivePrimary, true
	}
}

// stateToTarget maps a BUState string to a config target.
func stateToTarget(state string) string {
	switch state {
	case string(failover.StatePrimary):
		return config.ActivePrimary
	case string(failover.StateSecondary):
		return config.ActiveSecondary
	case string(failover.StateDraining):
		return config.ActivePrimary // fallback; caller should use drain state
	case string(failover.StateBothDown):
		return config.ActivePrimary
	}
	return config.ActivePrimary
}

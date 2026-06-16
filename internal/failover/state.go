// Package failover provides the DR state machine for per-BU active/passive
// cluster routing in the Kafka L7 proxy.
//
// Each business unit (cluster) has one of four states:
//   - PRIMARIO:   routing traffic to the primary cluster.
//   - SECUNDARIO: routing traffic to the secondary cluster.
//   - DRAINING:   transition in progress — no new connections to old target;
//                 existing connections drain; once drained, the state
//                 advances to the new target.
//   - BOTH_DOWN: both clusters are unreachable; hold last valid target.
//
// Transitions are triggered by configuration changes (hot-reload) or
// health-check events.  Registered callbacks fire on every state change
// so connection managers, drain managers, and metrics can react.
//
// All methods are concurrent-safe: each BU is protected by a sync.Mutex
// embedded in its entry.
package failover

import (
	"fmt"
	"sync"
)

// BUState is the DR state of a single business unit (cluster).
type BUState string

const (
	StatePrimary   BUState = "PRIMARIO"
	StateSecondary BUState = "SECUNDARIO"
	StateDraining   BUState = "DRAINING"
	StateBothDown  BUState = "BOTH_DOWN"
)

// StateChangeReason describes what triggered a state transition.
type StateChangeReason string

const (
	ReasonConfigChange  StateChangeReason = "config_change"
	ReasonHealthEvent   StateChangeReason = "health_event"
	ReasonDrainComplete StateChangeReason = "drain_complete"
	ReasonRecovery      StateChangeReason = "recovery"
)

// Transition records a complete state change event for a single BU.
type Transition struct {
	BU     string
	From   BUState
	To     BUState
	Reason StateChangeReason
}

// String returns a human-readable description of the transition.
func (t Transition) String() string {
	return fmt.Sprintf("%s: %s → %s (%s)", t.BU, t.From, t.To, t.Reason)
}

// StateChangeCallback is invoked synchronously after every state change.
// Implementations must not block for long periods or call back into
// the StateMachine (deadlock risk).
type StateChangeCallback func(transition Transition)

// buEntry holds the current state for one business unit.
type buEntry struct {
	state BUState
	mu    sync.Mutex
}

// StateMachine manages per-BU DR states for the proxy. It tracks the
// current state of every cluster, validates transitions, and fires
// registered callbacks on every change.
//
// Usage:
//
//	sm := failover.NewStateMachine()
//	sm.Initialize("bu-", failover.StatePrimary)
//	sm.OnStateChange(func(t failover.Transition) {
//	    log.Printf("state change: %s", t)
//	})
//	sm.Transition("bu-", failover.StateDraining, failover.ReasonConfigChange)
type StateMachine struct {
	mu        sync.Mutex
	entries   map[string]*buEntry
	callbacks []StateChangeCallback
}

// NewStateMachine creates a ready-to-use StateMachine with no BUs
// initialized.  Callers must call Initialize() for each BU before
// transitioning — Transition on an uninitialized BU is a no-op.
func NewStateMachine() *StateMachine {
	return &StateMachine{
		entries: make(map[string]*buEntry),
	}
}

// Initialize sets the initial DR state for a business unit. It does NOT
// fire callbacks (this is the starting state, not a transition). Returns
// true if this is a new BU; false if the BU was already initialized
// (in which case the state is unchanged and callers should use Transition).
func (sm *StateMachine) Initialize(bu string, state BUState) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.entries[bu]; exists {
		return false
	}

	sm.entries[bu] = &buEntry{state: state}
	return true
}

// State returns the current DR state for a business unit.
// Returns StatePrimary as a safe default for unknown BUs.
func (sm *StateMachine) State(bu string) BUState {
	sm.mu.Lock()
	entry, ok := sm.entries[bu]
	sm.mu.Unlock()

	if !ok {
		return StatePrimary
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()
	return entry.state
}

// Transition attempts to move a BU to the requested state. It validates
// the transition against the allowed graph and fires registered callbacks
// on success. Returns an error if the transition is not permitted.
//
// Allowed transitions:
//
//	PRIMARIO   → SECUNDARIO | DRAINING | BOTH_DOWN
//	SECUNDARIO → PRIMARIO   | DRAINING | BOTH_DOWN
//	DRAINING   → PRIMARIO   | SECUNDARIO | BOTH_DOWN
//	BOTH_DOWN → PRIMARIO   | SECUNDARIO
//
// Self-transitions (e.g. PRIMARIO → PRIMARIO) are silently accepted as
// no-ops (no callbacks fire).
func (sm *StateMachine) Transition(bu string, to BUState, reason StateChangeReason) error {
	sm.mu.Lock()
	entry, ok := sm.entries[bu]
	sm.mu.Unlock()

	if !ok {
		// Uninitialized BU — create it at the target state.
		// This is a convenience: OnReload callbacks may discover
		// new clusters and want to set their initial state via
		// Transition rather than Initialize (so callbacks fire).
		sm.mu.Lock()
		// Double-check under lock to avoid races.
		if _, exists := sm.entries[bu]; !exists {
			sm.entries[bu] = &buEntry{state: to}
			sm.mu.Unlock()
			// Fire callbacks for the new BU outside the SM lock.
			t := Transition{BU: bu, From: "", To: to, Reason: reason}
			sm.fireCallbacks(t)
			return nil
		}
		sm.mu.Unlock()
		// Fall through — another goroutine initialized it; retry below.
		return sm.Transition(bu, to, reason) // single retry
	}

	entry.mu.Lock()
	from := entry.state
	entry.mu.Unlock()

	// Self-transition: no-op, no callback.
	if from == to {
		return nil
	}

	// Validate the transition.
	if !isValidTransition(from, to) {
		return fmt.Errorf("invalid transition for %s: %s → %s", bu, from, to)
	}

	// Commit under the BU lock.
	entry.mu.Lock()
	// Re-read from in case another goroutine changed it.
	if entry.state != from {
		entry.mu.Unlock()
		return nil // another transition already happened; skip
	}
	entry.state = to
	entry.mu.Unlock()

	// Fire callbacks outside all locks.
	t := Transition{BU: bu, From: from, To: to, Reason: reason}
	sm.fireCallbacks(t)

	return nil
}

// OnStateChange registers a callback that is invoked synchronously after
// every successful state transition. Callbacks fire in registration order.
// Callbacks must not block for long periods and must not call back into
// the StateMachine (deadlock risk).
func (sm *StateMachine) OnStateChange(fn StateChangeCallback) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.callbacks = append(sm.callbacks, fn)
}

// AllStates returns a snapshot of every BU's current state. The returned
// map is a copy safe for the caller to read without holding any locks.
func (sm *StateMachine) AllStates() map[string]BUState {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	states := make(map[string]BUState, len(sm.entries))
	for bu, entry := range sm.entries {
		entry.mu.Lock()
		states[bu] = entry.state
		entry.mu.Unlock()
	}
	return states
}

// Remove removes a BU from the state machine entirely. No callback fires.
// Returns true if the BU existed and was removed.
func (sm *StateMachine) Remove(bu string) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	_, ok := sm.entries[bu]
	delete(sm.entries, bu)
	return ok
}

// ── Helpers ────────────────────────────────────────────────────────────

// isValidTransition returns true when the source→target transition is
// permitted by the state graph.
func isValidTransition(from, to BUState) bool {
	switch from {
	case StatePrimary:
		return to == StateSecondary || to == StateDraining || to == StateBothDown
	case StateSecondary:
		return to == StatePrimary || to == StateDraining || to == StateBothDown
	case StateDraining:
		return to == StatePrimary || to == StateSecondary || to == StateBothDown
	case StateBothDown:
		return to == StatePrimary || to == StateSecondary
	default:
		return false
	}
}

// fireCallbacks invokes all registered callbacks with the transition.
// Must be called outside the StateMachine and per-BU locks.
func (sm *StateMachine) fireCallbacks(t Transition) {
	sm.mu.Lock()
	cbs := make([]StateChangeCallback, len(sm.callbacks))
	copy(cbs, sm.callbacks)
	sm.mu.Unlock()

	for _, fn := range cbs {
		fn(t)
	}
}

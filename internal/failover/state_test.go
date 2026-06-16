package failover

import (
	"sync"
	"testing"
)

func TestNewStateMachine(t *testing.T) {
	sm := NewStateMachine()
	if sm == nil {
		t.Fatal("NewStateMachine returned nil")
	}
	if len(sm.entries) != 0 {
		t.Errorf("expected 0 entries, got %d", len(sm.entries))
	}
}

func TestInitialize(t *testing.T) {
	sm := NewStateMachine()

	// First initialization should succeed.
	if !sm.Initialize("bu-", StatePrimary) {
		t.Error("expected Initialize to return true for new BU")
	}

	// Second initialization should be a no-op.
	if sm.Initialize("bu-", StateSecondary) {
		t.Error("expected Initialize to return false for existing BU")
	}

	// State should still be the original value.
	if st := sm.State("bu-"); st != StatePrimary {
		t.Errorf("expected StatePrimary, got %s", st)
	}
}

func TestState_UnknownBU(t *testing.T) {
	sm := NewStateMachine()
	// Unknown BUs default to StatePrimary (safe default).
	if st := sm.State("nonexistent"); st != StatePrimary {
		t.Errorf("expected StatePrimary for unknown BU, got %s", st)
	}
}

func TestTransition_Valid(t *testing.T) {
	tests := []struct {
		name string
		init BUState
		to   BUState
	}{
		{"primary→secondary", StatePrimary, StateSecondary},
		{"primary→", StatePrimary, StateDraining},
		{"primary→", StatePrimary, StateBothDown},
		{"secondary→primary", StateSecondary, StatePrimary},
		{"secondary→", StateSecondary, StateDraining},
		{"secondary→", StateSecondary, StateBothDown},
		{"→primary", StateDraining, StatePrimary},
		{"→secondary", StateDraining, StateSecondary},
		{"→", StateDraining, StateBothDown},
		{"→primary", StateBothDown, StatePrimary},
		{"→secondary", StateBothDown, StateSecondary},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := NewStateMachine()
			sm.Initialize("bu-test", tt.init)

			err := sm.Transition("bu-test", tt.to, ReasonConfigChange)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if st := sm.State("bu-test"); st != tt.to {
				t.Errorf("expected state %s, got %s", tt.to, st)
			}
		})
	}
}

func TestTransition_Invalid(t *testing.T) {
	invalid := []struct {
		name string
		init BUState
		to   BUState
	}{
		// BothDown cannot go to Draining.
		{"→", StateBothDown, StateDraining},
		// BothDown cannot go to BothDown (no-op; no error but no change either).
		// We handle self-transition separately.
	}

	for _, tt := range invalid {
		t.Run(tt.name, func(t *testing.T) {
			sm := NewStateMachine()
			sm.Initialize("bu-test", tt.init)

			err := sm.Transition("bu-test", tt.to, ReasonHealthEvent)
			if err == nil {
				t.Errorf("expected error for invalid transition %s→%s", tt.init, tt.to)
			}

			// State should remain unchanged.
			if st := sm.State("bu-test"); st != tt.init {
				t.Errorf("expected state unchanged (%s), got %s", tt.init, st)
			}
		})
	}

	// Additional invalid transitions: from invalid states.
	invalidState := BUState("INVALID")
	sm := NewStateMachine()
	sm.Initialize("bu-test", StatePrimary)
	// Force the internal state to an invalid value (bypass Transition validation).
	sm.mu.Lock()
	sm.entries["bu-test"].mu.Lock()
	sm.entries["bu-test"].state = invalidState
	sm.entries["bu-test"].mu.Unlock()
	sm.mu.Unlock()

	err := sm.Transition("bu-test", StateDraining, ReasonConfigChange)
	if err == nil {
		t.Error("expected error for transition from invalid state")
	}
}

func TestTransition_SelfTransition(t *testing.T) {
	sm := NewStateMachine()
	sm.Initialize("bu-", StatePrimary)

	// Self-transition should be a no-op (no error, no callback).
	var fired bool
	sm.OnStateChange(func(tr Transition) { fired = true })

	err := sm.Transition("bu-", StatePrimary, ReasonConfigChange)
	if err != nil {
		t.Errorf("unexpected error for self-transition: %v", err)
	}
	if fired {
		t.Error("callback should NOT fire on self-transition")
	}
}

func TestTransition_UninitializedBU(t *testing.T) {
	sm := NewStateMachine()

	var callbackFired bool
	var captured Transition
	sm.OnStateChange(func(tr Transition) {
		callbackFired = true
		captured = tr
	})

	// Transition on uninitialized BU should auto-initialize and fire callback.
	err := sm.Transition("new-bu", StateSecondary, ReasonConfigChange)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if st := sm.State("new-bu"); st != StateSecondary {
		t.Errorf("expected StateSecondary, got %s", st)
	}

	if !callbackFired {
		t.Error("callback should fire when auto-initializing via Transition")
	}

	if captured.BU != "new-bu" || captured.From != "" || captured.To != StateSecondary {
		t.Errorf("unexpected transition: %+v", captured)
	}
}

func TestCallbacks(t *testing.T) {
	sm := NewStateMachine()
	sm.Initialize("bu-a", StatePrimary)

	var transitions []Transition
	sm.OnStateChange(func(tr Transition) {
		transitions = append(transitions, tr)
	})

	// First transition: primary → 
	if err := sm.Transition("bu-a", StateDraining, ReasonHealthEvent); err != nil {
		t.Fatal(err)
	}

	// Second transition:  → secondary
	if err := sm.Transition("bu-a", StateSecondary, ReasonDrainComplete); err != nil {
		t.Fatal(err)
	}

	if len(transitions) != 2 {
		t.Fatalf("expected 2 callbacks, got %d", len(transitions))
	}

	// Verify first callback.
	tr0 := transitions[0]
	if tr0.BU != "bu-a" || tr0.From != StatePrimary || tr0.To != StateDraining || tr0.Reason != ReasonHealthEvent {
		t.Errorf("unexpected transition[0]: %+v", tr0)
	}

	// Verify second callback.
	tr1 := transitions[1]
	if tr1.BU != "bu-a" || tr1.From != StateDraining || tr1.To != StateSecondary || tr1.Reason != ReasonDrainComplete {
		t.Errorf("unexpected transition[1]: %+v", tr1)
	}
}

func TestCallbacks_MultipleRegistrations(t *testing.T) {
	sm := NewStateMachine()
	sm.Initialize("bu-x", StatePrimary)

	var aFired, bFired bool
	sm.OnStateChange(func(tr Transition) { aFired = true })
	sm.OnStateChange(func(tr Transition) { bFired = true })

	if err := sm.Transition("bu-x", StateDraining, ReasonConfigChange); err != nil {
		t.Fatal(err)
	}

	if !aFired || !bFired {
		t.Errorf("both callbacks should fire: a=%v b=%v", aFired, bFired)
	}
}

func TestAllStates(t *testing.T) {
	sm := NewStateMachine()
	sm.Initialize("bu-a", StatePrimary)
	sm.Initialize("bu-b", StateSecondary)
	_ = sm.Transition("bu-c", StateBothDown, ReasonHealthEvent) // auto-init

	states := sm.AllStates()

	if len(states) != 3 {
		t.Fatalf("expected 3 BUs, got %d", len(states))
	}

	if states["bu-a"] != StatePrimary {
		t.Errorf("bu-a: expected Primary, got %s", states["bu-a"])
	}
	if states["bu-b"] != StateSecondary {
		t.Errorf("bu-b: expected Secondary, got %s", states["bu-b"])
	}
	if states["bu-c"] != StateBothDown {
		t.Errorf("bu-c: expected BothDown, got %s", states["bu-c"])
	}
}

func TestRemove(t *testing.T) {
	sm := NewStateMachine()
	sm.Initialize("bu-removeme", StatePrimary)

	if !sm.Remove("bu-removeme") {
		t.Error("Remove should return true for existing BU")
	}

	if sm.Remove("bu-removeme") {
		t.Error("Remove should return false for already-removed BU")
	}

	// State for removed BU should return default.
	if st := sm.State("bu-removeme"); st != StatePrimary {
		t.Errorf("expected default StatePrimary for removed BU, got %s", st)
	}

	// Removing unknown BU.
	if sm.Remove("never-existed") {
		t.Error("Remove should return false for unknown BU")
	}
}

func TestTransition_DRWorkflow(t *testing.T) {
	// Simulate a complete DR failover + recovery workflow.
	sm := NewStateMachine()
	sm.Initialize("bu-", StatePrimary)

	// 1. Health check detects primary failure → drain.
	if err := sm.Transition("bu-", StateDraining, ReasonHealthEvent); err != nil {
		t.Fatal("step 1:", err)
	}

	// 2. Drain completes → switch to secondary.
	if err := sm.Transition("bu-", StateSecondary, ReasonDrainComplete); err != nil {
		t.Fatal("step 2:", err)
	}

	// 3. Secondary also goes down → .
	if err := sm.Transition("bu-", StateBothDown, ReasonHealthEvent); err != nil {
		t.Fatal("step 3:", err)
	}

	// 4. Primary recovers → back to primary.
	if err := sm.Transition("bu-", StatePrimary, ReasonRecovery); err != nil {
		t.Fatal("step 4:", err)
	}

	if st := sm.State("bu-"); st != StatePrimary {
		t.Errorf("expected final state Primary, got %s", st)
	}
}

func TestTransition_ConfigChangeWorkflow(t *testing.T) {
	// Simulate manual config change: primary →  → secondary.
	sm := NewStateMachine()
	sm.Initialize("bu-config", StatePrimary)

	var captured []Transition
	sm.OnStateChange(func(tr Transition) {
		captured = append(captured, tr)
	})

	if err := sm.Transition("bu-config", StateDraining, ReasonConfigChange); err != nil {
		t.Fatal("step 1:", err)
	}
	if err := sm.Transition("bu-config", StateSecondary, ReasonDrainComplete); err != nil {
		t.Fatal("step 2:", err)
	}

	if len(captured) != 2 {
		t.Fatalf("expected 2 callbacks, got %d", len(captured))
	}

	if captured[0].Reason != ReasonConfigChange || captured[1].Reason != ReasonDrainComplete {
		t.Error("reasons mismatch")
	}
}

func TestConcurrentTransitions(t *testing.T) {
	sm := NewStateMachine()
	sm.Initialize("bu-race", StatePrimary)

	var wg sync.WaitGroup
	errCh := make(chan error, 20)

	// Fire 20 concurrent transitions among valid ones.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sm.Transition("bu-race", StateDraining, ReasonConfigChange); err != nil {
				errCh <- err
			}
		}()
	}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sm.Transition("bu-race", StateSecondary, ReasonDrainComplete); err != nil {
				errCh <- err
			}
		}()
	}

	wg.Wait()
	close(errCh)

	// Count errors — invalid transitions (e.g. sec→dren after already at dren)
	// should be reported.
	errorCount := 0
	for range errCh {
		errorCount++
	}

	// After all races settle, the state should be valid.
	final := sm.State("bu-race")
	if final != StateDraining && final != StateSecondary {
		t.Errorf("expected final state Draining or Secondary, got %s", final)
	}

	t.Logf("concurrent-transition errors: %d, final state: %s", errorCount, final)
}

func TestTransitionString(t *testing.T) {
	tr := Transition{
		BU:     "bu-",
		From:   StatePrimary,
		To:     StateDraining,
		Reason: ReasonHealthEvent,
	}

	s := tr.String()
	expected := "bu-: PRIMARIO → DRAINING (health_event)"
	if s != expected {
		t.Errorf("expected %q, got %q", expected, s)
	}
}

func TestMultipleBUs_IndependentStates(t *testing.T) {
	sm := NewStateMachine()
	sm.Initialize("bu-a", StatePrimary)
	sm.Initialize("bu-b", StateSecondary)
	sm.Initialize("bu-c", StateBothDown)

	// Transition bu-a without affecting bu-b or bu-c.
	if err := sm.Transition("bu-a", StateDraining, ReasonHealthEvent); err != nil {
		t.Fatal(err)
	}

	if st := sm.State("bu-a"); st != StateDraining {
		t.Errorf("bu-a: expected Draining, got %s", st)
	}
	if st := sm.State("bu-b"); st != StateSecondary {
		t.Errorf("bu-b: expected Secondary, got %s", st)
	}
	if st := sm.State("bu-c"); st != StateBothDown {
		t.Errorf("bu-c: expected BothDown, got %s", st)
	}
}

func TestAllStates_EmptyMachine(t *testing.T) {
	sm := NewStateMachine()
	states := sm.AllStates()
	if len(states) != 0 {
		t.Errorf("expected empty map, got %d entries", len(states))
	}
}

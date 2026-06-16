package proxy

import (
	"testing"
	"time"

	"github.com/vfolgosa/bifrost-proxy/internal/config"
	"github.com/vfolgosa/bifrost-proxy/internal/failover"
)

// ── DRCoordinator tests ───────────────────────────────────────────────

func TestDRCoordinator_Wire_TriggersDrainOnStateChange(t *testing.T) {
	sm := failover.NewStateMachine()
	sm.Initialize("bu-test", failover.StatePrimary)
	dm := NewDrainManager(30 * time.Second)
	coord := NewDRCoordinator(sm, dm)
	coord.Wire()

	// Transition to DRAINING — should start a drain.
	if err := sm.Transition("bu-test", failover.StateDraining, failover.ReasonConfigChange); err != nil {
		t.Fatalf("transition to : %v", err)
	}

	if !dm.IsDraining("bu-test") {
		t.Fatal("expected drain to be active after entering DRAINING")
	}

	ds := dm.DrainState("bu-test")
	if ds == nil {
		t.Fatal("expected non-nil drain state")
	}
	if ds.OldActive != config.ActivePrimary {
		t.Errorf("oldActive: got %q, want %q", ds.OldActive, config.ActivePrimary)
	}
	if ds.NewActive != config.ActiveSecondary {
		t.Errorf("newActive: got %q, want %q", ds.NewActive, config.ActiveSecondary)
	}
}

func TestDRCoordinator_Wire_DrainCompleteTransitionsState(t *testing.T) {
	sm := failover.NewStateMachine()
	sm.Initialize("bu-test", failover.StatePrimary)
	dm := NewDrainManager(30 * time.Second)
	coord := NewDRCoordinator(sm, dm)
	coord.Wire()

	// Register a connection on the old target.
	conn := newDummyConn()
	dm.Register("bu-test", conn, config.ActivePrimary)

	// Transition to DRAINING → starts drain.
	if err := sm.Transition("bu-test", failover.StateDraining, failover.ReasonConfigChange); err != nil {
		t.Fatalf("transition to : %v", err)
	}

	// Unregister the connection (simulate natural drain).
	dm.Unregister("bu-test", 1)

	// Start drain with very short timeout → forceCloseOld will fire soon.
	dm.StartDrain("bu-test", "primary", "secondary", 50*time.Millisecond)

	// Wait for drain to complete.
	time.Sleep(200 * time.Millisecond)

	// State should now be SECUNDARIO (drain complete → transition).
	state := sm.State("bu-test")
	if state != failover.StateSecondary {
		t.Errorf("expected SECUNDARIO after drain complete, got %s", state)
	}
}

func TestDRCoordinator_Wire_ForceCloseTransitionsState(t *testing.T) {
	sm := failover.NewStateMachine()
	sm.Initialize("bu-test", failover.StatePrimary)
	dm := NewDrainManager(30 * time.Second)
	coord := NewDRCoordinator(sm, dm)
	coord.Wire()

	// Register connections on old target (primary).
	for i := 0; i < 3; i++ {
		c := newDummyConn()
		dm.Register("bu-test", c, config.ActivePrimary)
	}

	// Transition to DRAINING → starts drain via callback.
	if err := sm.Transition("bu-test", failover.StateDraining, failover.ReasonConfigChange); err != nil {
		t.Fatalf("transition to : %v", err)
	}

	// The Wire callback already called StartDrain with the default timeout.
	// We need a short timeout for the test. Override with a short drain.
	dm.StartDrain("bu-test", "primary", "secondary", 50*time.Millisecond)

	// Wait for force-close.
	time.Sleep(200 * time.Millisecond)

	// State should be SECUNDARIO.
	state := sm.State("bu-test")
	if state != failover.StateSecondary {
		t.Errorf("expected SECUNDARIO after force-close drain, got %s", state)
	}
}

func TestDRCoordinator_Wire_ReverseDrain(t *testing.T) {
	sm := failover.NewStateMachine()
	sm.Initialize("bu-test", failover.StateSecondary)
	dm := NewDrainManager(30 * time.Second)
	coord := NewDRCoordinator(sm, dm)
	coord.Wire()

	// Transition to DRAINING from SECUNDARIO — should drain secondary → primary.
	if err := sm.Transition("bu-test", failover.StateDraining, failover.ReasonHealthEvent); err != nil {
		t.Fatalf("transition to : %v", err)
	}

	ds := dm.DrainState("bu-test")
	if ds == nil {
		t.Fatal("expected non-nil drain state")
	}
	// Going from SECUNDARIO → DRAINING means old=secondary, new=primary.
	if ds.OldActive != config.ActiveSecondary {
		t.Errorf("oldActive: got %q, want %q", ds.OldActive, config.ActiveSecondary)
	}
	if ds.NewActive != config.ActivePrimary {
		t.Errorf("newActive: got %q, want %q", ds.NewActive, config.ActivePrimary)
	}
}

func TestDRCoordinator_TargetForRouting(t *testing.T) {
	sm := failover.NewStateMachine()
	sm.Initialize("bu-test", failover.StatePrimary)
	dm := NewDrainManager(30 * time.Second)
	coord := NewDRCoordinator(sm, dm)

	// PRIMARIO → targets primary.
	target, ok := coord.TargetForRouting("bu-test")
	if !ok {
		t.Fatal("expected ok")
	}
	if target != config.ActivePrimary {
		t.Errorf("PRIMARIO: got %q, want %q", target, config.ActivePrimary)
	}

	// SECUNDARIO → targets secondary.
	_ = sm.Transition("bu-test", failover.StateSecondary, failover.ReasonConfigChange)
	target, _ = coord.TargetForRouting("bu-test")
	if target != config.ActiveSecondary {
		t.Errorf("SECUNDARIO: got %q, want %q", target, config.ActiveSecondary)
	}

	// DRAINING with active drain → targets the new (destination) cluster.
	_ = sm.Transition("bu-test", failover.StateDraining, failover.ReasonConfigChange)
	dm.StartDrain("bu-test", "primary", "secondary", 30*time.Second)
	target, _ = coord.TargetForRouting("bu-test")
	if target != config.ActiveSecondary {
		t.Errorf("DRAINING: got %q, want %q (new connections go to target)", target, config.ActiveSecondary)
	}
}

func TestDRCoordinator_TargetForRouting_BothDown(t *testing.T) {
	sm := failover.NewStateMachine()
	sm.Initialize("bu-test", failover.StatePrimary)
	dm := NewDrainManager(30 * time.Second)
	coord := NewDRCoordinator(sm, dm)

	_ = sm.Transition("bu-test", failover.StateBothDown, failover.ReasonHealthEvent)
	target, ok := coord.TargetForRouting("bu-test")
	if !ok {
		t.Fatal("expected ok")
	}
	// During BOTH_DOWN, routing falls back to primary.
	if target != config.ActivePrimary {
		t.Errorf("BOTH_DOWN: got %q, want %q", target, config.ActivePrimary)
	}
}

func TestDRCoordinator_PerBUIsolation(t *testing.T) {
	sm := failover.NewStateMachine()
	sm.Initialize("bu-a", failover.StatePrimary)
	sm.Initialize("bu-b", failover.StatePrimary)
	dm := NewDrainManager(30 * time.Second)
	coord := NewDRCoordinator(sm, dm)
	coord.Wire()

	// Drain bu-a only.
	_ = sm.Transition("bu-a", failover.StateDraining, failover.ReasonConfigChange)

	if !dm.IsDraining("bu-a") {
		t.Error("bu-a should be draining")
	}
	if dm.IsDraining("bu-b") {
		t.Error("bu-b should NOT be draining")
	}

	// bu-a state should be DRAINING, bu-b should be PRIMARIO.
	if sm.State("bu-a") != failover.StateDraining {
		t.Errorf("bu-a: got %s, want DRAINING", sm.State("bu-a"))
	}
	if sm.State("bu-b") != failover.StatePrimary {
		t.Errorf("bu-b: got %s, want PRIMARIO", sm.State("bu-b"))
	}
}

func TestDRCoordinator_Wire_NoOpWhenStateChangedDuringDrain(t *testing.T) {
	sm := failover.NewStateMachine()
	sm.Initialize("bu-test", failover.StatePrimary)
	dm := NewDrainManager(30 * time.Second)
	coord := NewDRCoordinator(sm, dm)
	coord.Wire()

	// Transition to DRAINING.
	_ = sm.Transition("bu-test", failover.StateDraining, failover.ReasonConfigChange)

	// Transition back to PRIMARIO before drain completes (simulating recovery).
	_ = sm.Transition("bu-test", failover.StatePrimary, failover.ReasonRecovery)

	// Force drain complete.
	dm.StartDrain("bu-test", "primary", "secondary", 50*time.Millisecond)
	time.Sleep(200 * time.Millisecond)

	// State should remain PRIMARIO (drain callback sees state isn't DRAINING, skips).
	state := sm.State("bu-test")
	if state != failover.StatePrimary {
		t.Errorf("expected PRIMARIO (drain complete when state changed should be no-op), got %s", state)
	}
}

func TestDRCoordinator_Wire_UnknownNewActive(t *testing.T) {
	sm := failover.NewStateMachine()
	sm.Initialize("bu-test", failover.StatePrimary)
	dm := NewDrainManager(30 * time.Second)
	coord := NewDRCoordinator(sm, dm)
	coord.Wire()

	// Transition to DRAINING.
	_ = sm.Transition("bu-test", failover.StateDraining, failover.ReasonConfigChange)

	// Manually start a drain with an unexpected newActive.
	dm.StartDrain("bu-test", "primary", "unknown-target", 50*time.Millisecond)
	time.Sleep(200 * time.Millisecond)

	// State should remain DRAINING (drain callback sees unknown newActive, skips).
	state := sm.State("bu-test")
	if state != failover.StateDraining {
		t.Errorf("expected DRAINING (unknown newActive should skip transition), got %s", state)
	}
}

package proxy

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ── Test helpers ──────────────────────────────────────────────────────

// dummyConn implements net.Conn for testing connection tracking.
type dummyConn struct {
	closed   atomic.Bool
	readErr  error // returned by Read
	writeErr error // returned by Write
}

func newDummyConn() *dummyConn { return &dummyConn{} }

func (d *dummyConn) Read(b []byte) (n int, err error) {
	if d.readErr != nil {
		return 0, d.readErr
	}
	return 0, nil
}

func (d *dummyConn) Write(b []byte) (n int, err error) {
	if d.writeErr != nil {
		return 0, d.writeErr
	}
	return len(b), nil
}

func (d *dummyConn) Close() error {
	d.closed.Store(true)
	return nil
}

func (d *dummyConn) LocalAddr() net.Addr                { return &net.TCPAddr{} }
func (d *dummyConn) RemoteAddr() net.Addr               { return &net.TCPAddr{} }
func (d *dummyConn) SetDeadline(t time.Time) error      { return nil }
func (d *dummyConn) SetReadDeadline(t time.Time) error  { return nil }
func (d *dummyConn) SetWriteDeadline(t time.Time) error { return nil }

// ── Tests ─────────────────────────────────────────────────────────────

func TestDrainManager_RegisterUnregister(t *testing.T) {
	dm := NewDrainManager(30 * time.Second)

	c1 := newDummyConn()
	c2 := newDummyConn()

	id1 := dm.Register(".proxy..com", c1, "primary")
	id2 := dm.Register(".proxy..com", c2, "primary")

	if count := dm.ActiveConnections(".proxy..com"); count != 2 {
		t.Errorf("expected 2 active connections, got %d", count)
	}

	dm.Unregister(".proxy..com", id1)
	if count := dm.ActiveConnections(".proxy..com"); count != 1 {
		t.Errorf("expected 1 active connection after unregister, got %d", count)
	}

	dm.Unregister(".proxy..com", id2)
	if count := dm.ActiveConnections(".proxy..com"); count != 0 {
		t.Errorf("expected 0 active connections after all unregistered, got %d", count)
	}

	// Unregister on a non-existent tracker should not panic.
	dm.Unregister("nonexistent.proxy..com", 999)
}

func TestDrainManager_ActiveConnectionCountPerBU(t *testing.T) {
	dm := NewDrainManager(30 * time.Second)

	c1 := newDummyConn()
	dm.Register("bu-a.proxy..com", c1, "primary")

	c2 := newDummyConn()
	c3 := newDummyConn()
	dm.Register("bu-b.proxy..com", c2, "primary")
	dm.Register("bu-b.proxy..com", c3, "secondary")

	if count := dm.ActiveConnections("bu-a.proxy..com"); count != 1 {
		t.Errorf("bu-a: expected 1, got %d", count)
	}
	if count := dm.ActiveConnections("bu-b.proxy..com"); count != 2 {
		t.Errorf("bu-b: expected 2, got %d", count)
	}

	total := dm.TotalActiveConnections()
	if total != 3 {
		t.Errorf("total: expected 3, got %d", total)
	}
}

func TestDrainManager_ConcurrentRegisterUnregister(t *testing.T) {
	dm := NewDrainManager(30 * time.Second)
	cluster := "concurrent.proxy..com"
	concurrency := 100

	var wg sync.WaitGroup
	var ids []uint64
	var idsMu sync.Mutex

	// Register concurrently.
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := newDummyConn()
			id := dm.Register(cluster, c, "primary")
			idsMu.Lock()
			ids = append(ids, id)
			idsMu.Unlock()
		}()
	}
	wg.Wait()

	if count := dm.ActiveConnections(cluster); count != int64(concurrency) {
		t.Errorf("after concurrent register: expected %d, got %d", concurrency, count)
	}

	// Unregister concurrently.
	for _, id := range ids {
		wg.Add(1)
		go func(rid uint64) {
			defer wg.Done()
			dm.Unregister(cluster, rid)
		}(id)
	}
	wg.Wait()

	if count := dm.ActiveConnections(cluster); count != 0 {
		t.Errorf("after concurrent unregister: expected 0, got %d", count)
	}
}

func TestDrainManager_StartDrainForceClosesOldConnections(t *testing.T) {
	dm := NewDrainManager(30 * time.Second)
	cluster := "drain-test.proxy..com"

	// Create connections on the old cluster (primary).
	var oldConns []*dummyConn
	for i := 0; i < 5; i++ {
		c := newDummyConn()
		oldConns = append(oldConns, c)
		dm.Register(cluster, c, "primary")
	}

	// Also create connections on the new cluster (secondary).
	var newConns []*dummyConn
	for i := 0; i < 3; i++ {
		c := newDummyConn()
		newConns = append(newConns, c)
		dm.Register(cluster, c, "secondary")
	}

	if count := dm.ActiveConnections(cluster); count != 8 {
		t.Fatalf("expected 8 connections, got %d", count)
	}

	// Start drain with a very short timeout.
	dm.StartDrain(cluster, "primary", "secondary", 50*time.Millisecond)

	if !dm.IsDraining(cluster) {
		t.Fatal("expected cluster to be draining")
	}

	// Wait for the drain timer to fire.
	time.Sleep(200 * time.Millisecond)

	// Old connections on primary should be force-closed.
	for _, c := range oldConns {
		if !c.closed.Load() {
			t.Errorf("expected old connection (primary) to be force-closed")
		}
	}

	// New connections on secondary should NOT be closed.
	for _, c := range newConns {
		if c.closed.Load() {
			t.Errorf("new connection (secondary) should NOT be force-closed")
		}
	}

	// Drain state should be cleared.
	if dm.IsDraining(cluster) {
		t.Error("expected drain state to be cleared after timeout")
	}

	// Active connections should only be new ones.
	if count := dm.ActiveConnections(cluster); count != 3 {
		t.Errorf("expected 3 remaining connections (new cluster), got %d", count)
	}
}

func TestDrainManager_DrainDoesNotAffectOtherBUs(t *testing.T) {
	dm := NewDrainManager(30 * time.Second)

	// BU-A connections.
	cA1 := newDummyConn()
	dm.Register("bu-a.proxy..com", cA1, "primary")

	// BU-B connections.
	cB1 := newDummyConn()
	dm.Register("bu-b.proxy..com", cB1, "primary")

	// Start drain only on BU-A.
	dm.StartDrain("bu-a.proxy..com", "primary", "secondary", 50*time.Millisecond)
	time.Sleep(200 * time.Millisecond)

	// BU-A connection should be force-closed.
	if !cA1.closed.Load() {
		t.Error("bu-a connection should be force-closed")
	}

	// BU-B connection should NOT be affected.
	if cB1.closed.Load() {
		t.Error("bu-b connection should NOT be force-closed")
	}
}

func TestDrainManager_AllConnectionsDrainNaturally(t *testing.T) {
	dm := NewDrainManager(30 * time.Second)
	cluster := "natural-drain.proxy..com"

	c := newDummyConn()
	id := dm.Register(cluster, c, "primary")

	if count := dm.ActiveConnections(cluster); count != 1 {
		t.Fatalf("expected 1 connection, got %d", count)
	}

	// Start drain.
	dm.StartDrain(cluster, "primary", "secondary", 200*time.Millisecond)

	// Connection drains naturally before timeout.
	dm.Unregister(cluster, id)

	// Wait for timeout.
	time.Sleep(300 * time.Millisecond)

	// Connection should NOT have been force-closed (already unregistered).
	if c.closed.Load() {
		t.Error("connections that drain naturally should not be force-closed")
	}

	// Active count should be 0.
	if count := dm.ActiveConnections(cluster); count != 0 {
		t.Errorf("expected 0 connections, got %d", count)
	}
}

func TestDrainManager_MultipleStartDrainCancelsPrevious(t *testing.T) {
	dm := NewDrainManager(30 * time.Second)
	cluster := "multi-drain.proxy..com"

	c := newDummyConn()
	dm.Register(cluster, c, "primary")

	// Start first drain with a long timeout.
	dm.StartDrain(cluster, "primary", "secondary", 5*time.Second)

	// Start second drain (should cancel first).
	dm.StartDrain(cluster, "primary", "secondary", 50*time.Millisecond)

	time.Sleep(200 * time.Millisecond)

	// Connection should be force-closed by the second drain's short timeout.
	if !c.closed.Load() {
		t.Error("connection should be force-closed by the second drain")
	}
}

func TestDrainManager_ForceCloseUsesDefaultTimeout(t *testing.T) {
	dm := NewDrainManager(30 * time.Second) // Default: 30s

	// Override for testing.
	dm.DefaultDrainTimeout = 50 * time.Millisecond

	cluster := "default-timeout.proxy..com"
	c := newDummyConn()
	dm.Register(cluster, c, "primary")

	// Start drain with timeout=0 → uses DefaultDrainTimeout.
	dm.StartDrain(cluster, "primary", "secondary", 0)

	time.Sleep(200 * time.Millisecond)

	if !c.closed.Load() {
		t.Error("connection should be force-closed using default timeout")
	}
}

func TestDrainManager_DrainStateSnapshot(t *testing.T) {
	dm := NewDrainManager(30 * time.Second)
	cluster := "state-test.proxy..com"

	dm.StartDrain(cluster, "primary", "secondary", 5*time.Second)

	ds := dm.DrainState(cluster)
	if ds == nil {
		t.Fatal("expected non-nil drain state")
	}
	if ds.OldActive != "primary" {
		t.Errorf("oldActive: expected 'primary', got %q", ds.OldActive)
	}
	if ds.NewActive != "secondary" {
		t.Errorf("newActive: expected 'secondary', got %q", ds.NewActive)
	}
	if ds.StartedAt.IsZero() {
		t.Error("StartedAt should not be zero")
	}

	// Non-draining BU returns nil.
	if ds := dm.DrainState("nonexistent"); ds != nil {
		t.Error("expected nil drain state for non-draining BU")
	}
}

func TestDrainManager_ConfigChangeHandler(t *testing.T) {
	dm := NewDrainManager(30 * time.Second)
	dm.DefaultDrainTimeout = 50 * time.Millisecond

	// This tests the callback shape - actual integration would need real configs.
	handler := dm.ConfigChangeHandler()
	if handler == nil {
		t.Fatal("expected non-nil handler")
	}
	// Handler is a valid function (no panics on invocation with empty configs).
	// Full integration test would need real *config.Config values.
}

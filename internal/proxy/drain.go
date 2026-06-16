// Package proxy implements the Kafka L7 proxy with graceful connection
// draining for Disaster Recovery (DR) transitions.
//
// The DrainManager tracks active connections per Business Unit (BU) and
// supports a drain state machine: when the active cluster changes (e.g.
// primary → secondary), new connections route to the target while
// existing connections complete on the old cluster up to an idle timeout,
// after which remaining connections are force-closed.
package proxy

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vfolgosa/bifrost-proxy/internal/config"
	"github.com/vfolgosa/bifrost-proxy/internal/logger"
)

// DrainCompleteCallback is invoked when a drain operation finishes (either
// naturally — all connections drained before timeout — or forcibly via the
// timer).  It receives the cluster name, old active target, and new active
// target.  Implementations must not block for long periods.
type DrainCompleteCallback func(clusterName, oldActive, newActive string)

// DrainManager manages graceful connection draining during DR transitions.
// It tracks active connections per BU, supports starting a drain for a
// specific cluster, and force-closes stale connections after the drain
// timeout. It is designed to be registered as an OnReload callback on a
// config.Reloader to automatically detect active field changes.
type DrainManager struct {
	mu      sync.RWMutex
	buConns map[string]*buConnTracker // clusterName → connection tracker
	drains  map[string]*buDrain       // clusterName → active drain

	// DefaultDrainTimeout is the idle timeout used when StartDrain is
	// called with timeout <= 0. It should match the configured
	// connection_pool.idle_timeout (default 30s).
	DefaultDrainTimeout time.Duration

	// drainCompleteCallbacks are invoked after every drain finishes.
	drainCompleteCallbacks []DrainCompleteCallback
}

// buConnTracker tracks active connections for a single BU.
type buConnTracker struct {
	count  atomic.Int64
	mu     sync.Mutex
	conns  map[uint64]*trackedConn
	nextID atomic.Uint64
}

// trackedConn represents a single tracked client connection.
type trackedConn struct {
	id     uint64
	conn   net.Conn
	target string // "primary" or "secondary"
}

// buDrain represents an active drain operation for a single BU.
type buDrain struct {
	OldActive string
	NewActive string
	StartedAt time.Time
	timer     *time.Timer
}

// NewDrainManager creates a new DrainManager with the given default
// drain timeout (should match connection_pool.idle_timeout).
func NewDrainManager(defaultTimeout time.Duration) *DrainManager {
	return &DrainManager{
		buConns:             make(map[string]*buConnTracker),
		drains:              make(map[string]*buDrain),
		DefaultDrainTimeout: defaultTimeout,
	}
}

// Register adds a client connection to the active connection tracker for
// the given BU and returns a registration ID the caller uses to unregister.
// targetCluster is the cluster the connection is currently targeting
// ("primary" or "secondary").
func (dm *DrainManager) Register(clusterName string, conn net.Conn, targetCluster string) uint64 {
	dm.mu.Lock()
	tracker, ok := dm.buConns[clusterName]
	if !ok {
		tracker = &buConnTracker{conns: make(map[uint64]*trackedConn)}
		dm.buConns[clusterName] = tracker
	}
	dm.mu.Unlock()

	id := tracker.nextID.Add(1)
	tracker.count.Add(1)

	tc := &trackedConn{
		id:     id,
		conn:   conn,
		target: targetCluster,
	}

	tracker.mu.Lock()
	tracker.conns[id] = tc
	tracker.mu.Unlock()

	return id
}

// Unregister removes a connection from the active connection tracker.
// Safe to call multiple times; duplicates are silently ignored.
func (dm *DrainManager) Unregister(clusterName string, id uint64) {
	dm.mu.RLock()
	tracker, ok := dm.buConns[clusterName]
	dm.mu.RUnlock()
	if !ok {
		return
	}

	tracker.mu.Lock()
	if _, exists := tracker.conns[id]; exists {
		delete(tracker.conns, id)
		tracker.count.Add(-1)
	}
	tracker.mu.Unlock()
}

// ActiveConnections returns the number of actively tracked connections for
// a given BU.
func (dm *DrainManager) ActiveConnections(clusterName string) int64 {
	dm.mu.RLock()
	tracker, ok := dm.buConns[clusterName]
	dm.mu.RUnlock()
	if !ok {
		return 0
	}
	return tracker.count.Load()
}

// TotalActiveConnections returns the sum of active connections across all BUs.
func (dm *DrainManager) TotalActiveConnections() int64 {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	var total int64
	for _, tracker := range dm.buConns {
		total += tracker.count.Load()
	}
	return total
}

// StartDrain begins a graceful drain for a cluster. After the drain
// timeout, any remaining connections on oldActive will be force-closed.
// If timeout <= 0, DefaultDrainTimeout is used.
//
// New connections are unaffected — they route to the new cluster based
// on the updated config. Only connections previously established on
// oldActive are subject to the drain timeout.
func (dm *DrainManager) StartDrain(clusterName string, oldActive, newActive string, timeout time.Duration) {
	if timeout <= 0 {
		timeout = dm.DefaultDrainTimeout
	}

	dm.mu.Lock()
	// Cancel any existing drain for this cluster.
	if existing, ok := dm.drains[clusterName]; ok {
		if existing.timer != nil {
			existing.timer.Stop()
		}
	}

	drain := &buDrain{
		OldActive: oldActive,
		NewActive: newActive,
		StartedAt: time.Now(),
	}

	drain.timer = time.AfterFunc(timeout, func() {
		dm.forceCloseOld(clusterName, oldActive)
	})

	dm.drains[clusterName] = drain
	dm.mu.Unlock()

	logger.Default().Info("drain started",
		"cluster", clusterName,
		"old_active", oldActive,
		"new_active", newActive,
		"timeout", timeout.String())
}

// IsDraining returns true if the given cluster is currently in a drain state.
func (dm *DrainManager) IsDraining(clusterName string) bool {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	_, ok := dm.drains[clusterName]
	return ok
}

// DrainState returns the active drain state for a cluster, or nil if not
// draining. The caller receives a snapshot; the underlying state may change.
func (dm *DrainManager) DrainState(clusterName string) *buDrain {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	return dm.drains[clusterName]
}

// OnDrainComplete registers a callback that fires after every drain
// operation finishes (timeout or natural drain). Callbacks fire in
// registration order and must not block for long periods.
func (dm *DrainManager) OnDrainComplete(fn DrainCompleteCallback) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.drainCompleteCallbacks = append(dm.drainCompleteCallbacks, fn)
}

// forceCloseOld closes all tracked connections that are still targeting
// oldActive. Called by the drain timer when the timeout fires.
func (dm *DrainManager) forceCloseOld(clusterName string, oldActive string) {
	dm.mu.RLock()
	tracker, ok := dm.buConns[clusterName]
	dm.mu.RUnlock()

	if !ok {
		dm.mu.Lock()
		oldDrain := dm.drains[clusterName]
		delete(dm.drains, clusterName)
		dm.mu.Unlock()
		dm.fireDrainComplete(clusterName, oldActive, oldDrain)
		return
	}

	tracker.mu.Lock()
	var toClose []*trackedConn
	for _, tc := range tracker.conns {
		if tc.target == oldActive {
			toClose = append(toClose, tc)
		}
	}
	// Remove from the tracker so they're not double-counted.
	for _, tc := range toClose {
		delete(tracker.conns, tc.id)
	}
	tracker.mu.Unlock()

	for _, tc := range toClose {
		logger.Default().Warn("force-closing connection",
			"cluster", clusterName,
			"target", oldActive,
			"reason", "drain timeout exceeded")
		tc.conn.Close()
		tracker.count.Add(-1)
	}

	dm.mu.Lock()
	oldDrain := dm.drains[clusterName]
	delete(dm.drains, clusterName)
	dm.mu.Unlock()

	if len(toClose) > 0 {
		logger.Default().Warn("force-closed connections",
			"count", len(toClose),
			"cluster", clusterName,
			"old_active", oldActive)
	} else {
		logger.Default().Info("drain completed naturally",
			"cluster", clusterName,
			"old_active", oldActive)
	}

	dm.fireDrainComplete(clusterName, oldActive, oldDrain)
}

// fireDrainComplete invokes all registered drain-complete callbacks.
// Must be called outside any locks held by the caller.
func (dm *DrainManager) fireDrainComplete(clusterName, oldActive string, drain *buDrain) {
	var newActive string
	if drain != nil {
		newActive = drain.NewActive
	}
	dm.mu.RLock()
	cbs := make([]DrainCompleteCallback, len(dm.drainCompleteCallbacks))
	copy(cbs, dm.drainCompleteCallbacks)
	dm.mu.RUnlock()

	for _, fn := range cbs {
		fn(clusterName, oldActive, newActive)
	}
}

// ConfigChangeHandler returns an OnReload callback suitable for
// registration with config.Reloader.OnReload. When the config is
// reloaded and a cluster's active field changes (active_passive mode
// only), it automatically calls StartDrain.
func (dm *DrainManager) ConfigChangeHandler() func(oldCfg, newCfg *config.Config) {
	return func(oldCfg, newCfg *config.Config) {
		changes := config.DiffClusters(oldCfg, newCfg)
		for _, ch := range changes {
			if ch.ActiveChanged {
				oldCluster := oldCfg.Clusters[ch.Name]
				newCluster := newCfg.Clusters[ch.Name]
				if newCluster.Mode == config.ModeActivePassive {
					dm.StartDrain(ch.Name, oldCluster.Active, newCluster.Active, 0)
				}
			}
		}
	}
}

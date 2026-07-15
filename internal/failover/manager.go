package failover

import (
	"sync"
	"time"

	"github.com/vfolgosa/bifrost-proxy/internal/config"
	"github.com/vfolgosa/bifrost-proxy/internal/health"
	"github.com/vfolgosa/bifrost-proxy/internal/logger"
)

// DrainStateReader exposes per-BU drain destination for routing status.
type DrainStateReader interface {
	DrainNewActive(bu string) (newActive string, ok bool)
}

// MetricsRecorder receives failover and circuit-breaker events.
type MetricsRecorder interface {
	RecordFailover()
	RecordCircuitBreaker(bu, state string)
}

// Manager wires health check results into per-BU failover Controllers and
// records metrics when autonomous failover actions occur.
type Manager struct {
	mu          sync.RWMutex
	sm          *StateMachine
	dm          DrainStateReader
	controllers map[string]*Controller
	checker     healthSnapshotProvider
	metrics     MetricsRecorder
	stopCh      chan struct{}
	doneCh      chan struct{}
}

type healthSnapshotProvider interface {
	Health() map[string]health.ClusterHealth
}

// NewManager creates failover controllers for active_passive clusters with
// auto_failover enabled.
func NewManager(sm *StateMachine, clusters map[string]config.ClusterConfig) *Manager {
	m := &Manager{
		sm:          sm,
		controllers: make(map[string]*Controller),
		stopCh:      make(chan struct{}),
		doneCh:      make(chan struct{}),
	}
	m.Reconfigure(clusters)
	return m
}

// SetDrainReader attaches the drain manager for effective-active reporting.
func (m *Manager) SetDrainReader(dm DrainStateReader) {
	m.mu.Lock()
	m.dm = dm
	m.mu.Unlock()
}

// SetChecker updates the health snapshot source (e.g. after hot reload).
func (m *Manager) SetChecker(checker healthSnapshotProvider) {
	m.mu.Lock()
	m.checker = checker
	m.mu.Unlock()
}

// SetMetrics attaches the metrics recorder.
func (m *Manager) SetMetrics(metrics MetricsRecorder) {
	m.mu.Lock()
	m.metrics = metrics
	m.mu.Unlock()
}

// Reconfigure rebuilds controllers from the current cluster map.
func (m *Manager) Reconfigure(clusters map[string]config.ClusterConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.controllers = make(map[string]*Controller)
	for name, c := range clusters {
		if c.Mode != config.ModeActivePassive || !c.HealthCheck.AutoFailover {
			continue
		}
		m.controllers[name] = NewController(name, c.Active, c.Mode, c.HealthCheck, m.sm)
	}
}

// Controller returns the controller for a BU, if any.
func (m *Manager) Controller(bu string) *Controller {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.controllers[bu]
}

// Start launches the background health sync loop.
func (m *Manager) Start(checker healthSnapshotProvider) {
	m.mu.Lock()
	m.checker = checker
	m.mu.Unlock()

	go m.loop()
}

// Stop shuts down the background loop.
func (m *Manager) Stop() {
	select {
	case <-m.stopCh:
		return
	default:
	}
	close(m.stopCh)
	<-m.doneCh
}

// EffectiveActive returns the routing-active cluster ("primary"/"secondary")
// derived from the DR state machine. Returns ("", false) when the SM has no
// opinion (unknown BU or non-failover cluster).
func (m *Manager) EffectiveActive(bu string) (string, bool) {
	state := m.sm.State(bu)
	switch state {
	case StatePrimary:
		return config.ActivePrimary, true
	case StateSecondary:
		return config.ActiveSecondary, true
	case StateDraining:
		m.mu.RLock()
		dm := m.dm
		m.mu.RUnlock()
		if dm != nil {
			if newActive, ok := dm.DrainNewActive(bu); ok {
				return newActive, true
			}
		}
		return "", false
	case StateBothDown:
		m.mu.RLock()
		ctrl := m.controllers[bu]
		m.mu.RUnlock()
		if ctrl == nil {
			return "", false
		}
		switch ctrl.BothDownActiveCluster() {
		case string(StatePrimary):
			return config.ActivePrimary, true
		case string(StateSecondary):
			return config.ActiveSecondary, true
		}
	}
	return "", false
}

func (m *Manager) loop() {
	defer close(m.doneCh)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	m.sync()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.sync()
		}
	}
}

func (m *Manager) sync() {
	m.mu.RLock()
	checker := m.checker
	controllers := make(map[string]*Controller, len(m.controllers))
	for k, v := range m.controllers {
		controllers[k] = v
	}
	metrics := m.metrics
	m.mu.RUnlock()

	if checker == nil {
		return
	}

	healthMap := checker.Health()
	for bu, ctrl := range controllers {
		ch, ok := healthMap[bu]
		if !ok {
			continue
		}

		ctrl.RecordHealthResult("primary", ch.Primary.Healthy, ch.Primary.LastCheckLatency)
		ctrl.RecordHealthResult("secondary", ch.Secondary.Healthy, ch.Secondary.LastCheckLatency)

		action := ctrl.Evaluate()
		if action != ActionNone && metrics != nil {
			metrics.RecordFailover()
			logger.L().Info("autonomous failover action",
				"bu", bu, "action", string(action))
		}

		if metrics != nil {
			if ctrl.IsCircuitBroken() {
				metrics.RecordCircuitBreaker(bu, "broken")
			} else {
				metrics.RecordCircuitBreaker(bu, "ok")
			}
		}
	}
}

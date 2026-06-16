// Package health provides health state tracking and autonomous health checks
// for the Kafka proxy's upstream Confluent Cloud clusters.
//
// Each cluster endpoint (primary, secondary) gets its own HealthState,
// which records the outcome of periodic health checks (Metadata requests)
// and transitions between UP and DOWN based on configurable consecutive-success
// and consecutive-failure thresholds.
//
// The Checker drives the health-check goroutines, opening dedicated short-lived
// TLS connections (separate from the data-plane pool) to avoid competing with
// client traffic.
//
// All types and methods in this file are safe for concurrent use.
package health

import (
	"sync"
	"time"
)

// ── Status constants ────────────────────────────────────────────────────

// Status describes the health of a cluster endpoint at a point in time.
type Status string

const (
	StatusHealthy     Status = "healthy"
	StatusDegraded    Status = "degraded"
	StatusUnreachable Status = "unreachable"
)

// ── HealthState ──────────────────────────────────────────────────────────

// HealthState tracks the accumulated health of a single cluster endpoint
// (primary or secondary).  It is updated by the checker goroutine and
// read by external consumers (metrics, status endpoint, failover controller).
//
// Transitions:
//   - DOWN when consecutive failures reach failureThreshold.
//   - UP when consecutive successes reach recoveryThreshold.
//
// When created, the state starts as healthy (UP) with UpSince set to now.
type HealthState struct {
	mu sync.RWMutex

	healthy              bool
	consecutiveFailures  int
	consecutiveSuccesses int

	lastCheckAt      time.Time
	lastCheckLatency time.Duration

	// lastStatus is the health signal from the most recent check.
	lastStatus Status

	// lastError holds the error string from the most recent failed check.
	// Empty when the last check succeeded.
	lastError string

	// upSince records when the endpoint was last declared Healthy.
	// Zero when currently unhealthy.
	upSince time.Time

	// bootstrap is the upstream address being checked.
	bootstrap string
}

// NewHealthState creates a new HealthState, initially healthy.
// bootstrap is the upstream address being monitored.
func NewHealthState(bootstrap string) *HealthState {
	return &HealthState{
		healthy:   true,
		upSince:   time.Now(),
		bootstrap: bootstrap,
	}
}

// ── Record methods (called by checker goroutine) ────────────────────────

// RecordSuccess records a successful health check.  Resets consecutive
// failures to zero, increments consecutive successes, and transitions to
// UP when consecutive successes reach recoveryThreshold.
//
// status should be StatusHealthy or StatusDegraded based on latency.
func (h *HealthState) RecordSuccess(latency time.Duration, status Status, failureThreshold, recoveryThreshold int) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.consecutiveFailures = 0
	h.consecutiveSuccesses++
	h.lastCheckAt = time.Now()
	h.lastCheckLatency = latency
	h.lastStatus = status
	h.lastError = ""

	if !h.healthy && h.consecutiveSuccesses >= recoveryThreshold {
		h.healthy = true
		h.upSince = time.Now()
	}
}

// RecordFailure records a failed health check.  Resets consecutive
// successes to zero, increments consecutive failures, and transitions to
// DOWN when consecutive failures reach failureThreshold.
func (h *HealthState) RecordFailure(latency time.Duration, status Status, errMsg string, failureThreshold int) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.consecutiveSuccesses = 0
	h.consecutiveFailures++
	h.lastCheckAt = time.Now()
	h.lastCheckLatency = latency
	h.lastStatus = status
	h.lastError = errMsg

	if h.healthy && h.consecutiveFailures >= failureThreshold {
		h.healthy = false
		h.upSince = time.Time{}
	}
}

// ── Read methods ────────────────────────────────────────────────────────

// IsHealthy returns whether the endpoint is currently considered healthy (UP).
func (h *HealthState) IsHealthy() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.healthy
}

// Snapshot is an atomic, point-in-time snapshot of all tracked fields.
// Useful for status endpoints and logging without holding the lock.
type Snapshot struct {
	Healthy              bool
	ConsecutiveFailures  int
	ConsecutiveSuccesses int
	LastCheckAt          time.Time
	LastCheckLatency     time.Duration
	LastStatus           Status
	LastError            string
	UpSince              time.Time
	Bootstrap            string
}

// Snapshot returns a consistent snapshot of the current health state.
func (h *HealthState) Snapshot() Snapshot {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return Snapshot{
		Healthy:              h.healthy,
		ConsecutiveFailures:  h.consecutiveFailures,
		ConsecutiveSuccesses: h.consecutiveSuccesses,
		LastCheckAt:          h.lastCheckAt,
		LastCheckLatency:     h.lastCheckLatency,
		LastStatus:           h.lastStatus,
		LastError:            h.lastError,
		UpSince:              h.upSince,
		Bootstrap:            h.bootstrap,
	}
}

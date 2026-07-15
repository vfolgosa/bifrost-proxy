// Package server provides adapters to bridge the health package's types
// to the server's HealthProvider interface.
package server

import (
	"github.com/vfolgosa/bifrost-proxy/internal/health"
)

// HealthCheckerAdapter adapts a health.Checker to the server.HealthProvider interface.
// It converts health.ClusterHealth (which uses health.Snapshot) into
// server.ClusterHealthSnapshot (which uses server.EndpointHealth).
type HealthCheckerAdapter struct {
	checker *health.Checker
}

// NewHealthCheckerAdapter creates an adapter wrapping a health.Checker.
func NewHealthCheckerAdapter(checker *health.Checker) *HealthCheckerAdapter {
	return &HealthCheckerAdapter{checker: checker}
}

// UpdateChecker replaces the underlying health checker (e.g. after hot reload).
func (a *HealthCheckerAdapter) UpdateChecker(checker *health.Checker) {
	a.checker = checker
}

// Health returns per-cluster health snapshots in the server's format.
func (a *HealthCheckerAdapter) Health() map[string]ClusterHealthSnapshot {
	if a.checker == nil {
		return nil
	}

	raw := a.checker.Health()
	result := make(map[string]ClusterHealthSnapshot, len(raw))

	for name, ch := range raw {
		result[name] = ClusterHealthSnapshot{
			Name:       ch.Name,
			Primary:   convertSnapshot(ch.Primary),
			Secondary: convertSnapshot(ch.Secondary),
		}
	}

	return result
}

// convertSnapshot converts a health.Snapshot to a server.EndpointHealth.
func convertSnapshot(s health.Snapshot) EndpointHealth {
	return EndpointHealth{
		Healthy:              s.Healthy,
		ConsecutiveFailures:  s.ConsecutiveFailures,
		ConsecutiveSuccesses: s.ConsecutiveSuccesses,
		LastCheckLatency:     s.LastCheckLatency,
		LastStatus:           string(s.LastStatus),
		LastError:            s.LastError,
		UpSince:              s.UpSince,
		Bootstrap:            s.Bootstrap,
	}
}

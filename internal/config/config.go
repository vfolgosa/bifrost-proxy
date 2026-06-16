// Package config provides configuration parsing for the Kafka L7 proxy.
//
// It defines the full configuration schema (proxy listener, connection pooling,
// per-BU cluster routing via dedicated ports, and health checks) and supports
// both active_passive and load_balance cluster modes as specified in docs/proxy-spec.md.
//
// Routing: each BU gets a dedicated port. Clients connect to <proxy>:<port> and
// the proxy routes by port number. No TLS/SNI required.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// ── Mode & Active constants ──────────────────────────────────────────

const (
	ModeActivePassive = "active_passive"
	ModeLoadBalance   = "load_balance"
	ModeSingle        = "single"
)

const (
	ActivePrimary   = "primary"
	ActiveSecondary = "secondary"
)

// ── Top-level ────────────────────────────────────────────────────────

// Config is the root configuration object parsed from config.yaml.
type Config struct {
	Proxy    ProxyConfig              `yaml:"proxy"`
	Clusters map[string]ClusterConfig `yaml:"clusters"`
}

// ── Proxy ─────────────────────────────────────────────────────────────

// ProxyConfig holds the listener and pool settings for the proxy process.
// There is no global port — each cluster gets its own port.
type ProxyConfig struct {
	BindAddress    string               `yaml:"bind_address"`
	ConnectionPool ConnectionPoolConfig `yaml:"connection_pool"`
	MetricsPort    int                  `yaml:"metrics_port"`
}

// ConnectionPoolConfig controls the upstream connection pool behaviour.
type ConnectionPoolConfig struct {
	MaxConnectionsPerBroker int      `yaml:"max_connections_per_broker"`
	IdleTimeout             Duration `yaml:"idle_timeout"`
	KeepAliveInterval       Duration `yaml:"keep_alive_interval"`
}

// ── Clusters ──────────────────────────────────────────────────────────

// ClusterConfig describes one Business Unit's cluster topology and health
// check parameters. Each cluster gets a dedicated port for client connections.
//
//	active_passive:   primary / secondary are plain bootstrap strings.
//	load_balance:     primary / secondary are objects with bootstrap+weight.
//
// ClusterEndpoint.UnmarshalYAML accepts both forms transparently.
type ClusterConfig struct {
	Port        int               `yaml:"port"` // dedicated port for this BU
	Mode        string            `yaml:"mode"`
	Active      string            `yaml:"active"`
	Primary    ClusterEndpoint   `yaml:"primary"`
	Secondary  ClusterEndpoint   `yaml:"secondary"`
	HealthCheck HealthCheckConfig `yaml:"health_check"`
}

// ClusterEndpoint is a backend Kafka bootstrap server plus an
// optional weight used in load_balance mode.
type ClusterEndpoint struct {
	Bootstrap string `yaml:"bootstrap"`
	Weight    int    `yaml:"weight"`
}

// UnmarshalYAML allows the YAML parser to accept ClusterEndpoint as either a
// plain string (active_passive) or an object with bootstrap and weight fields
// (load_balance).
func (e *ClusterEndpoint) UnmarshalYAML(value *yaml.Node) error {
	switch value.Kind {
	case yaml.ScalarNode:
		e.Bootstrap = value.Value
		return nil
	case yaml.MappingNode:
		type rawEndpoint ClusterEndpoint
		return value.Decode((*rawEndpoint)(e))
	}
	return nil
}

// ── Validation ───────────────────────────────────────────────────────

// LoadConfig reads and parses a YAML configuration file, applies defaults,
// and validates the result.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file %q: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config file %q: %w", path, err)
	}

	applyDefaults(&cfg)

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	return &cfg, nil
}

// Validate checks the configuration for correctness.
func (c *Config) Validate() error {
	if c.Proxy.BindAddress == "" {
		c.Proxy.BindAddress = "0.0.0.0"
	}

	if len(c.Clusters) == 0 {
		return fmt.Errorf("at least one cluster is required")
	}

	seenPorts := make(map[int]string)
	for name, cluster := range c.Clusters {
		if cluster.Port <= 0 {
			return fmt.Errorf("cluster %q: port is required", name)
		}
		if existing, ok := seenPorts[cluster.Port]; ok {
			return fmt.Errorf("port %d is used by both %q and %q", cluster.Port, existing, name)
		}
		seenPorts[cluster.Port] = name

		if err := cluster.Validate(); err != nil {
			return fmt.Errorf("cluster %q: %w", name, err)
		}
	}

	return nil
}

// Validate checks a single cluster configuration.
func (c *ClusterConfig) Validate() error {
	if c.Mode == "" {
		return fmt.Errorf("mode is required")
	}

	switch c.Mode {
	case ModeActivePassive:
		if c.Primary.Bootstrap == "" {
			return fmt.Errorf("primary bootstrap is required for active_passive mode")
		}
		if c.Secondary.Bootstrap == "" {
			return fmt.Errorf("secondary bootstrap is required for active_passive mode")
		}
		if c.Active != ActivePrimary && c.Active != ActiveSecondary {
			return fmt.Errorf("active must be %q or %q for active_passive mode, got %q",
				ActivePrimary, ActiveSecondary, c.Active)
		}

	case ModeLoadBalance:
		if c.Primary.Bootstrap == "" {
			return fmt.Errorf("primary.bootstrap is required for load_balance mode")
		}
		if c.Secondary.Bootstrap == "" {
			return fmt.Errorf("secondary.bootstrap is required for load_balance mode")
		}
		if c.Primary.Weight+c.Secondary.Weight != 100 {
			return fmt.Errorf("load_balance weights must sum to 100, got primary=%d + secondary=%d = %d",
				c.Primary.Weight, c.Secondary.Weight, c.Primary.Weight+c.Secondary.Weight)
		}

	case ModeSingle:
		if c.Primary.Bootstrap == "" {
			return fmt.Errorf("primary bootstrap is required for single mode")
		}

	default:
		return fmt.Errorf("unknown mode %q (must be %q or %q)",
			c.Mode, ModeActivePassive, ModeLoadBalance)
	}

	return nil
}

// ── Health Check ──────────────────────────────────────────────────────

// HealthCheckConfig controls autonomous health monitoring and failover
// behaviour for a single cluster.
type HealthCheckConfig struct {
	Enabled                    bool     `yaml:"enabled"`
	Interval                   Duration `yaml:"interval"`
	FailureThreshold           int      `yaml:"failure_threshold"`
	RecoveryThreshold          int      `yaml:"recovery_threshold"`
	RecoveryMinUptime          Duration `yaml:"recovery_min_uptime"`
	MinTimeBetweenFailovers    Duration `yaml:"min_time_between_failovers"`
	AutoFailover               bool     `yaml:"auto_failover"`
	AutoFailback               bool     `yaml:"auto_failback"`
	AutoRebalance              bool     `yaml:"auto_rebalance"`
	RequireTargetHealthy       bool     `yaml:"require_target_healthy"`
	CircuitBreakerMaxFailovers int      `yaml:"circuit_breaker_max_failovers"`
	CircuitBreakerWindow       Duration `yaml:"circuit_breaker_window"`
	SaslUsername               string   `yaml:"sasl_username"` // credentials for health check SASL auth
	SaslPassword               string   `yaml:"sasl_password"`
}

// ── Duration helper ───────────────────────────────────────────────────

// Duration is a time.Duration wrapper that implements YAML unmarshaling from
// Go duration strings (e.g. "30s", "5m").
type Duration time.Duration

// UnmarshalYAML decodes a YAML string like "30s" into a Duration.
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	dur, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(dur)
	return nil
}

// Duration returns the underlying time.Duration value.
func (d Duration) Duration() time.Duration {
	return time.Duration(d)
}

// ── Defaults ──────────────────────────────────────────────────────────

const (
	defaultBindAddress            = "0.0.0.0"
	defaultMaxConnectionsPerBroker = 50
	defaultIdleTimeout            = 30 * time.Second
	defaultKeepAliveInterval      = 30 * time.Second
	DefaultFailureThreshold        = 3
	DefaultRecoveryThreshold       = 2
	defaultRecoveryMinUptime       = 120 * time.Second
)

// applyDefaults fills in zero-value fields with documented defaults.
func applyDefaults(cfg *Config) {
	if cfg.Proxy.BindAddress == "" {
		cfg.Proxy.BindAddress = defaultBindAddress
	}

	if cfg.Proxy.ConnectionPool.MaxConnectionsPerBroker == 0 {
		cfg.Proxy.ConnectionPool.MaxConnectionsPerBroker = defaultMaxConnectionsPerBroker
	}
	if cfg.Proxy.ConnectionPool.IdleTimeout == 0 {
		cfg.Proxy.ConnectionPool.IdleTimeout = Duration(defaultIdleTimeout)
	}
	if cfg.Proxy.ConnectionPool.KeepAliveInterval == 0 {
		cfg.Proxy.ConnectionPool.KeepAliveInterval = Duration(defaultKeepAliveInterval)
	}

	for name, cluster := range cfg.Clusters {
		hc := &cluster.HealthCheck

		if hc.FailureThreshold == 0 {
			hc.FailureThreshold = DefaultFailureThreshold
		}
		if hc.RecoveryThreshold == 0 {
			hc.RecoveryThreshold = DefaultRecoveryThreshold
		}
		if hc.RecoveryMinUptime == 0 {
			hc.RecoveryMinUptime = Duration(defaultRecoveryMinUptime)
		}

		cfg.Clusters[name] = cluster
	}
}

// BuildPortMap returns a map from port number to cluster name for routing.
func (c *Config) BuildPortMap() map[int]string {
	m := make(map[int]string, len(c.Clusters))
	for name, cluster := range c.Clusters {
		m[cluster.Port] = name
	}
	return m
}

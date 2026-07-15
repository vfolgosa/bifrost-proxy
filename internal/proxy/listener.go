package proxy

import (
	"fmt"
	"net"
	"sync"

	"github.com/vfolgosa/bifrost-proxy/internal/config"
	"github.com/vfolgosa/bifrost-proxy/internal/health"
	"github.com/vfolgosa/bifrost-proxy/internal/logger"
	"github.com/vfolgosa/bifrost-proxy/internal/pool"
	"github.com/vfolgosa/bifrost-proxy/internal/routing"
)

// Listener accepts plain TCP connections on multiple ports — one per
// configured cluster (BU). The port number determines the target cluster.
// Every accepted connection spawns a dedicated goroutine.
type Listener struct {
	reloader      *config.Reloader
	cfg           *config.Config
	listeners     []net.Listener        // one per cluster port
	portMap       map[int]string        // port → cluster name
	drainMgr      *DrainManager
	drCoordinator *DRCoordinator
	leaderCache   *pool.PartitionLeaderCache
	router        *routing.Router
	rebalancer    *routing.Rebalancer
	healthChecker *health.Checker
	log           *logger.Logger
	mu            sync.Mutex
	closeOnce     sync.Once
	shutdownCh    chan struct{}
}

// SetDrainManager assigns a DrainManager for active-connection tracking.
func (l *Listener) SetDrainManager(dm *DrainManager) { l.drainMgr = dm }

// SetDRCoordinator assigns a DRCoordinator for state-aware routing.
func (l *Listener) SetDRCoordinator(c *DRCoordinator) { l.drCoordinator = c }

// NewListener creates a Listener from a static proxy configuration.
func NewListener(cfg *config.Config) (*Listener, error) {
	l := &Listener{
		cfg:        cfg,
		portMap:    cfg.BuildPortMap(),
		log:        logger.Default(),
		shutdownCh: make(chan struct{}),
	}
	l.initRouting(cfg)
	return l, nil
}

// NewListenerWithReloader creates a Listener backed by a config.Reloader
// for hot reload support.
func NewListenerWithReloader(reloader *config.Reloader) (*Listener, error) {
	cfg := reloader.Config()
	l := &Listener{
		reloader:   reloader,
		portMap:    cfg.BuildPortMap(),
		log:        logger.Default(),
		shutdownCh: make(chan struct{}),
	}
	l.initRouting(cfg)
	return l, nil
}

func (l *Listener) getConfig() *config.Config {
	if l.reloader != nil {
		return l.reloader.Config()
	}
	return l.cfg
}

// Start begins listening on each cluster's configured port. Each accepted
// connection runs in its own goroutine via handleConnection. Blocks until
// all listeners are closed.
func (l *Listener) Start() error {
	cfg := l.getConfig()
	bindAddr := cfg.Proxy.BindAddress

	var listeners []net.Listener
	for name, cluster := range cfg.Clusters {
		addr := net.JoinHostPort(bindAddr, fmt.Sprintf("%d", cluster.Port))
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			// Close any already-opened listeners on failure.
			for _, prev := range listeners {
				prev.Close()
			}
			return fmt.Errorf("failed to listen on %s (cluster %q): %w", addr, name, err)
		}
		listeners = append(listeners, ln)
		l.log.Info("listener started", "cluster", name, "port", cluster.Port, "bind_addr", addr)

		// Capture in closure for goroutine.
		go func(clusterName string, ln net.Listener) {
			for {
				conn, err := ln.Accept()
				if err != nil {
					return
				}
				go l.handleConnection(conn, clusterName)
			}
		}(name, ln)
	}

	l.mu.Lock()
	l.listeners = listeners
	l.mu.Unlock()

	// Start auto-rebalance if configured.
	if l.rebalancer != nil {
		l.rebalancer.Start()
	}

	// Block until Close() signals shutdown.
	<-l.shutdownCh
	return nil
}

// EffectiveWeights returns post-rebalance weights for load_balance clusters.
func (l *Listener) EffectiveWeights(clusterName string) (primary, secondary int, ok bool) {
	cfg := l.getConfig()
	clusterCfg, exists := cfg.Clusters[clusterName]
	if !exists || l.router == nil || clusterCfg.Mode != config.ModeLoadBalance {
		return 0, 0, false
	}
	p, s := l.router.GetEffectiveWeights(clusterName, clusterCfg)
	return p, s, true
}

// SetHealthChecker attaches a health.Checker for auto-rebalance and stores
// the reference for hot-reload updates.
func (l *Listener) SetHealthChecker(checker *health.Checker) {
	l.healthChecker = checker
	if l.router == nil || checker == nil {
		return
	}

	cfg := l.getConfig()
	hasAutoRebalance := false
	for _, c := range cfg.Clusters {
		if c.Mode == config.ModeLoadBalance && c.HealthCheck.AutoRebalance {
			hasAutoRebalance = true
			break
		}
	}
	if !hasAutoRebalance {
		return
	}

	if l.rebalancer != nil {
		l.rebalancer.Stop()
	}
	l.rebalancer = routing.NewRebalancer(l.router, checker, cfg)
}

// Close gracefully shuts down all listeners and the rebalancer.
func (l *Listener) Close() error {
	if l.rebalancer != nil {
		l.rebalancer.Stop()
	}
	l.mu.Lock()
	for _, ln := range l.listeners {
		ln.Close()
	}
	l.mu.Unlock()

	l.closeOnce.Do(func() { close(l.shutdownCh) })
	return nil
}

func (l *Listener) initRouting(cfg *config.Config) {
	l.leaderCache = pool.NewPartitionLeaderCache() // plain TCP upstream, no TLS
	l.leaderCache.ConfigureSASL(cfg)
	l.router = routing.NewRouter(cfg, l.leaderCache)

	for _, bootstrap := range allConfigBootstraps(cfg) {
		l.leaderCache.StartBackgroundRefresh(bootstrap)
	}
}

// RefreshClusters syncs routing state after a hot reload.
func (l *Listener) RefreshClusters(newCfg *config.Config) {
	if l.router != nil {
		l.router.UpdateConfig(newCfg)
	}
	if l.rebalancer != nil {
		l.rebalancer.UpdateConfig(newCfg)
	}
	if l.healthChecker != nil {
		hasAutoRebalance := false
		for _, c := range newCfg.Clusters {
			if c.Mode == config.ModeLoadBalance && c.HealthCheck.AutoRebalance {
				hasAutoRebalance = true
				break
			}
		}
		if hasAutoRebalance && l.rebalancer == nil {
			l.rebalancer = routing.NewRebalancer(l.router, l.healthChecker, newCfg)
			if l.listeners != nil {
				l.rebalancer.Start()
			}
		} else if !hasAutoRebalance && l.rebalancer != nil {
			l.rebalancer.Stop()
			l.rebalancer = nil
		}
	}

	if l.leaderCache == nil {
		return
	}

	l.leaderCache.ConfigureSASL(newCfg)

	oldBootstraps := l.leaderCache.ActiveClusters()
	oldSet := make(map[string]bool, len(oldBootstraps))
	for _, b := range oldBootstraps {
		oldSet[b] = true
	}

	for _, bootstrap := range allConfigBootstraps(newCfg) {
		if !oldSet[bootstrap] {
			l.leaderCache.StartBackgroundRefresh(bootstrap)
		}
	}

	newSet := make(map[string]bool)
	for _, bootstrap := range allConfigBootstraps(newCfg) {
		newSet[bootstrap] = true
	}
	for _, bootstrap := range oldBootstraps {
		if !newSet[bootstrap] {
			l.leaderCache.StopBackgroundRefresh(bootstrap)
			l.leaderCache.Invalidate(bootstrap)
		}
	}
}

// allConfigBootstraps returns the deduplicated bootstrap addresses that need
// leader-cache entries across all configured clusters.
func allConfigBootstraps(cfg *config.Config) []string {
	seen := make(map[string]bool)
	var bootstraps []string
	for _, clusterCfg := range cfg.Clusters {
		for _, b := range clusterBootstraps(clusterCfg) {
			if b == "" || seen[b] {
				continue
			}
			seen[b] = true
			bootstraps = append(bootstraps, b)
		}
	}
	return bootstraps
}

// clusterBootstraps returns every upstream bootstrap that may receive
// partition-aware Produce/Fetch traffic for a cluster configuration.
func clusterBootstraps(cfg config.ClusterConfig) []string {
	switch cfg.Mode {
	case config.ModeLoadBalance, config.ModeActivePassive:
		var bootstraps []string
		if cfg.Primary.Bootstrap != "" {
			bootstraps = append(bootstraps, cfg.Primary.Bootstrap)
		}
		if cfg.Secondary.Bootstrap != "" {
			bootstraps = append(bootstraps, cfg.Secondary.Bootstrap)
		}
		return bootstraps
	case config.ModeSingle:
		if cfg.Primary.Bootstrap != "" {
			return []string{cfg.Primary.Bootstrap}
		}
		return nil
	default:
		return nil
	}
}

// handleConnection manages the lifecycle of a single client TCP connection.
// The clusterName is determined by the port the connection arrived on.
func (l *Listener) handleConnection(conn net.Conn, clusterName string) {
	defer conn.Close()

	cfg := l.getConfig()
	clusterCfg, ok := cfg.Clusters[clusterName]
	if !ok {
		l.log.Error("cluster not found in config", "cluster", clusterName)
		return
	}

	connLog := l.log.WithBU(clusterName)
	connLog.Info("connection accepted",
		"cluster", clusterName, "mode", clusterCfg.Mode, "active", clusterCfg.Active)

	advertiseHost := cfg.Proxy.ResolvedAdvertiseHost()
	handleConnection(connLog, conn, clusterName, clusterCfg, int32(clusterCfg.Port), advertiseHost, l.drainMgr, l.router, l.drCoordinator)
}

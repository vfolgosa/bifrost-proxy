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
	log           *logger.Logger
	mu            sync.Mutex
}

// SetDrainManager assigns a DrainManager for active-connection tracking.
func (l *Listener) SetDrainManager(dm *DrainManager) { l.drainMgr = dm }

// SetDRCoordinator assigns a DRCoordinator for state-aware routing.
func (l *Listener) SetDRCoordinator(c *DRCoordinator) { l.drCoordinator = c }

// NewListener creates a Listener from a static proxy configuration.
func NewListener(cfg *config.Config) (*Listener, error) {
	l := &Listener{
		cfg:     cfg,
		portMap: cfg.BuildPortMap(),
		log:     logger.Default(),
	}
	l.initRouting(cfg)
	return l, nil
}

// NewListenerWithReloader creates a Listener backed by a config.Reloader
// for hot reload support.
func NewListenerWithReloader(reloader *config.Reloader) (*Listener, error) {
	cfg := reloader.Config()
	l := &Listener{
		reloader: reloader,
		portMap:  cfg.BuildPortMap(),
		log:      logger.Default(),
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

	// Block forever (listeners run in goroutines).
	select {}
}

// SetHealthChecker attaches a health.Checker for auto-rebalance.
func (l *Listener) SetHealthChecker(checker *health.Checker) {
	if l.router != nil && checker != nil {
		l.rebalancer = routing.NewRebalancer(l.router, checker, l.getConfig())
	}
}

// Close gracefully shuts down all listeners and the rebalancer.
func (l *Listener) Close() error {
	if l.rebalancer != nil {
		l.rebalancer.Stop()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, ln := range l.listeners {
		ln.Close()
	}
	return nil
}

func (l *Listener) initRouting(cfg *config.Config) {
	l.leaderCache = pool.NewPartitionLeaderCache() // plain TCP upstream, no TLS
	l.router = routing.NewRouter(cfg, l.leaderCache)

	for _, clusterCfg := range cfg.Clusters {
		l.leaderCache.StartBackgroundRefresh(clusterCfg.Primary.Bootstrap)
	}
}

// RefreshClusters syncs the leader cache after a hot reload.
func (l *Listener) RefreshClusters(newCfg *config.Config) {
	if l.leaderCache == nil {
		return
	}

	oldClusters := l.leaderCache.ActiveClusters()
	oldSet := make(map[string]bool, len(oldClusters))
	for _, c := range oldClusters {
		oldSet[c] = true
	}

	for _, clusterCfg := range newCfg.Clusters {
		bootstrap := clusterCfg.Primary.Bootstrap
		if !oldSet[bootstrap] {
			l.leaderCache.StartBackgroundRefresh(bootstrap)
		}
	}

	newSet := make(map[string]bool, len(newCfg.Clusters))
	for _, clusterCfg := range newCfg.Clusters {
		newSet[clusterCfg.Primary.Bootstrap] = true
	}
	for _, bootstrap := range oldClusters {
		if !newSet[bootstrap] {
			l.leaderCache.StopBackgroundRefresh(bootstrap)
			l.leaderCache.Invalidate(bootstrap)
		}
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

	handleConnection(connLog, conn, clusterName, clusterCfg, int32(clusterCfg.Port), l.drainMgr, l.router, l.drCoordinator)
}

package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vfolgosa/bifrost-proxy/internal/config"
	"github.com/vfolgosa/bifrost-proxy/internal/failover"
	"github.com/vfolgosa/bifrost-proxy/internal/health"
	"github.com/vfolgosa/bifrost-proxy/internal/logger"
	"github.com/vfolgosa/bifrost-proxy/internal/proxy"
	"github.com/vfolgosa/bifrost-proxy/internal/server"
)

const shutdownDrainTimeout = 30 * time.Second

func main() {
	configPath := flag.String("config", "config.yaml", "Path to config.yaml")
	flag.Parse()

	logr := logger.L()

	logr.Info("kafkaproxy starting", "config", *configPath)

	reloader, err := config.NewReloader(*configPath)
	if err != nil {
		logr.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	defer reloader.Stop()

	cfg := reloader.Config()
	logr.Info("config loaded", "clusters", len(cfg.Clusters))
	for name, c := range cfg.Clusters {
		logr.Info("cluster configured", "name", name, "mode", c.Mode, "port", c.Port, "active", c.Active)
	}

	healthChecker := health.New(cfg.Clusters)
	healthChecker.Start()
	defer healthChecker.Stop()

	idleTimeout := time.Duration(cfg.Proxy.ConnectionPool.IdleTimeout)
	if idleTimeout <= 0 {
		idleTimeout = 30 * time.Second
	}
	drainMgr := proxy.NewDrainManager(idleTimeout)

	sm := failover.NewStateMachine()
	for name, cluster := range cfg.Clusters {
		if cluster.Mode != config.ModeActivePassive {
			continue
		}
		var initState failover.BUState
		switch cluster.Active {
		case config.ActivePrimary:
			initState = failover.StatePrimary
		case config.ActiveSecondary:
			initState = failover.StateSecondary
		default:
			initState = failover.StatePrimary
		}
		sm.Initialize(name, initState)
		logr.Info("DR state machine initialised", "bu", name, "state", initState)
	}

	drCoord := proxy.NewDRCoordinator(sm, drainMgr)
	drCoord.Wire()

	failoverM := server.NewFailoverMetrics()
	failoverMgr := failover.NewManager(sm, cfg.Clusters)
	failoverMgr.SetDrainReader(drCoord)
	failoverMgr.SetMetrics(failoverM)
	failoverMgr.Start(healthChecker)
	defer failoverMgr.Stop()

	drCoord.SetControllerLookup(failoverMgr.Controller)

	healthAdapter := server.NewHealthCheckerAdapter(healthChecker)

	var listener *proxy.Listener

	reloader.OnReload(func(oldCfg, newCfg *config.Config) {
		changes := config.DiffClusters(oldCfg, newCfg)
		if len(changes) == 0 {
			return
		}
		logr.Info("config reloaded", "changed_clusters", len(changes))
		for _, ch := range changes {
			logr.Info("cluster change", "name", ch.Name,
				"active_changed", ch.ActiveChanged,
				"bootstrap_changed", ch.BootstrapChanged,
				"mode_changed", ch.ModeChanged)

			if ch.ActiveChanged {
				oldCluster := oldCfg.Clusters[ch.Name]
				newCluster := newCfg.Clusters[ch.Name]
				if newCluster.Mode == config.ModeActivePassive {
					logr.Info("active cluster changed, entering DRAINING",
						"bu", ch.Name,
						"old_active", oldCluster.Active,
						"new_active", newCluster.Active)
					_ = sm.Transition(ch.Name, failover.StateDraining, failover.ReasonConfigChange)
				}
			}
		}

		healthChecker.Stop()
		healthChecker = health.New(newCfg.Clusters)
		healthChecker.Start()
		healthAdapter.UpdateChecker(healthChecker)

		failoverMgr.Reconfigure(newCfg.Clusters)
		failoverMgr.SetChecker(healthChecker)

		if listener != nil {
			listener.SetHealthChecker(healthChecker)
			listener.RefreshClusters(newCfg)
		}
	})

	ctx, cancelWatcher := context.WithCancel(context.Background())
	defer cancelWatcher()

	go func() {
		if err := reloader.Watch(ctx); err != nil && err != context.Canceled {
			logr.Error("config watcher stopped", "error", err)
		}
	}()

	metricsPort := cfg.Proxy.MetricsPort
	if metricsPort <= 0 {
		metricsPort = 8080
	}

	obsSrv := server.New(metricsPort, healthAdapter, drainMgr, reloader, failoverM)
	obsSrv.SetFailoverStateProvider(failoverMgr)

	serverCtx, cancelServer := context.WithCancel(context.Background())
	defer cancelServer()

	go func() {
		if err := obsSrv.Start(serverCtx); err != nil {
			logr.Error("observability server error", "error", err)
		}
	}()

	listener, err = proxy.NewListenerWithReloader(reloader)
	if err != nil {
		logr.Error("failed to create listener", "error", err)
		os.Exit(1)
	}

	listener.SetHealthChecker(healthChecker)
	listener.SetDrainManager(drainMgr)
	listener.SetDRCoordinator(drCoord)
	obsSrv.SetWeightProvider(listener)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		logr.Info("graceful shutdown starting")
		cancelServer()
		cancelWatcher()
		if err := listener.Close(); err != nil {
			logr.Error("error closing listener", "error", err)
		}
		drainMgr.WaitIdle(shutdownDrainTimeout)
		reloader.Stop()
		healthChecker.Stop()
		failoverMgr.Stop()
		os.Exit(0)
	}()

	logr.Info("starting proxy listener")
	if err := listener.Start(); err != nil {
		logr.Error("listener failed", "error", err)
		os.Exit(1)
	}
}

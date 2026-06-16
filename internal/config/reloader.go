// Package config provides atomic hot-reload for the Kafka L7 proxy configuration.
//
// The Reloader wraps a *Config behind a sync.RWMutex, allowing lock-free reads
// via Config() while guaranteeing that a concurrent Reload() atomically swaps
// the pointer after parsing and validating the new configuration file.
//
// Integration:
//   - An fsnotify watcher monitors the YAML file for writes.
//   - On each event, Reload() is called: parse → validate → swap under write lock.
//   - Registered OnReload callbacks receive the old and new *Config so that
//     active connections can diff their cluster configs and reopen if needed.
package config

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/vfolgosa/bifrost-proxy/internal/logger"
)

// Reloader provides atomic, lock-free reads of the current configuration
// with hot-reload driven by an fsnotify file watcher.
type Reloader struct {
	mu      sync.RWMutex
	config  *Config
	path    string

	watcher  *fsnotify.Watcher
	stopCh   chan struct{}

	onReload []func(oldCfg, newCfg *Config)
}

// NewReloader loads the initial configuration from path, creates an fsnotify
// watcher on the file's parent directory, and returns a ready Reloader.
func NewReloader(path string) (*Reloader, error) {
	cfg, err := LoadConfig(path)
	if err != nil {
		return nil, fmt.Errorf("initial config load: %w", err)
	}

	// Watch the *directory* containing the config file, not the file itself,
	// because editors often rename-then-recreate (atomic write) which would
	// lose the watch on the inode.
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("creating fsnotify watcher: %w", err)
	}

	dir := parentDir(path)
	if err := watcher.Add(dir); err != nil {
		watcher.Close()
		return nil, fmt.Errorf("watching directory %s: %w", dir, err)
	}

	logger.Default().Info("config watcher started", "path", path)

	return &Reloader{
		config:  cfg,
		path:    path,
		watcher: watcher,
		stopCh:  make(chan struct{}),
	}, nil
}

// Config returns the current configuration pointer.
// The caller receives a snapshot; the pointer is never mutated.
func (r *Reloader) Config() *Config {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.config
}

// Reload reads, parses, validates the config file and atomically swaps
// the pointer under write lock. On success, registered OnReload callbacks
// are invoked with the old and new configurations.
func (r *Reloader) Reload() error {
	newCfg, err := LoadConfig(r.path)
	if err != nil {
		return fmt.Errorf("reload config: %w", err)
	}

	r.mu.Lock()
	oldCfg := r.config
	r.config = newCfg
	r.mu.Unlock()

	logger.Default().Info("configuration reloaded", "clusters", len(newCfg.Clusters))

	// Notify callbacks outside the lock so they can safely call Config().
	for _, fn := range r.onReload {
		fn(oldCfg, newCfg)
	}

	return nil
}

// OnReload registers a callback that is invoked after every successful
// reload. The callback receives the old and new *Config so it can diff
// cluster changes (active target, bootstrap addresses, mode).
func (r *Reloader) OnReload(fn func(oldCfg, newCfg *Config)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onReload = append(r.onReload, fn)
}

// Watch starts the fsnotify event loop. It debounces rapid successive
// events (e.g. from atomic-save editors) and calls Reload() when the
// watched config file has been written. Blocks until ctx is cancelled
// or Stop() is called.
func (r *Reloader) Watch(ctx context.Context) error {
	debounce := 200 * time.Millisecond
	var timer *time.Timer

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-r.stopCh:
			return nil

		case event, ok := <-r.watcher.Events:
			if !ok {
				return nil // channel closed
			}

			// Only react to writes or creates on the config file itself.
			if !isRelevantEvent(event, r.path) {
				continue
			}

			// Debounce: reset the timer on each event within the window.
			if timer != nil {
				timer.Stop()
			}
			timer = time.AfterFunc(debounce, func() {
				if err := r.Reload(); err != nil {
				logger.Default().Error("config reload failed", "error", err)
				}
			})

		case err, ok := <-r.watcher.Errors:
			if !ok {
				return nil
			}
			logger.Default().Error("config watcher error", "error", err)
		}
	}
}

// Stop closes the fsnotify watcher and signals Watch() to return.
func (r *Reloader) Stop() error {
	close(r.stopCh)
	return r.watcher.Close()
}

// ── Helpers ────────────────────────────────────────────────────────────

// parentDir returns the directory containing path, or "." for empty.
func parentDir(path string) string {
	dir := path
	for i := len(dir) - 1; i >= 0; i-- {
		if dir[i] == os.PathSeparator {
			return dir[:i]
		}
	}
	return "."
}

// isRelevantEvent returns true when the fsnotify event is a Write or
// Create on the exact config file path (not a sibling or temp file).
func isRelevantEvent(event fsnotify.Event, configPath string) bool {
	if event.Name != configPath {
		return false
	}
	return event.Has(fsnotify.Write) || event.Has(fsnotify.Create)
}

// DiffClusterChanges compares old and new cluster configs and reports
// whether each cluster's active target or bootstrap address changed.
// This is a convenience helper for OnReload callbacks that manage active
// connections.
type ClusterChange struct {
	Name         string
	ActiveChanged bool   // active field changed (primary ↔ secondary)
	BootstrapChanged map[string]bool // "primary" or "secondary" → true if bootstrap address changed
	ModeChanged  bool
}

// DiffClusters compares two configs and returns a list of clusters whose
// configuration changed in a way that may require connection re-opening.
func DiffClusters(oldCfg, newCfg *Config) []ClusterChange {
	var changes []ClusterChange

	for name, newCluster := range newCfg.Clusters {
		oldCluster, existed := oldCfg.Clusters[name]
		if !existed {
			// New cluster: all connections targeting it need to be opened.
			changes = append(changes, ClusterChange{
				Name:            name,
				ActiveChanged:   true,
				BootstrapChanged: map[string]bool{"primary": true, "secondary": true},
				ModeChanged:     true,
			})
			continue
		}

		bc := map[string]bool{}
		if oldCluster.Primary.Bootstrap != newCluster.Primary.Bootstrap {
			bc["primary"] = true
		}
		if oldCluster.Secondary.Bootstrap != newCluster.Secondary.Bootstrap {
			bc["secondary"] = true
		}

		changed := false
		if oldCluster.Active != newCluster.Active {
			changed = true
		}
		if oldCluster.Mode != newCluster.Mode {
			changed = true
		}
		if len(bc) > 0 {
			changed = true
		}

		if changed {
			changes = append(changes, ClusterChange{
				Name:             name,
				ActiveChanged:    oldCluster.Active != newCluster.Active,
				BootstrapChanged: bc,
				ModeChanged:      oldCluster.Mode != newCluster.Mode,
			})
		}
	}

	// Clusters removed from config also need attention.
	for name := range oldCfg.Clusters {
		if _, exists := newCfg.Clusters[name]; !exists {
			changes = append(changes, ClusterChange{
				Name:         name,
				ActiveChanged: true, // force connections to close
			})
		}
	}

	return changes
}

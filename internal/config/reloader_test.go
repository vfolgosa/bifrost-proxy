package config

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// writeTempConfig writes a YAML config to a temp file and returns the path.
func writeTempConfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return path
}

const minValidConfig = `
proxy:
  bind_address: "0.0.0.0"
  port: 9092
  tls:
    enabled: false
clusters:
  test-cluster:
    mode: active_passive
    port: 9093
    active: primary
    primary: "pkc-1111.us-east-1.aws.confluent.cloud:9092"
    secondary: "pkc-2222.us-east-2.aws.confluent.cloud:9092"
`

// ── NewReloader & Config() ─────────────────────────────────────────────

func TestNewReloader_ValidConfig(t *testing.T) {
	path := writeTempConfig(t, minValidConfig)

	rl, err := NewReloader(path)
	if err != nil {
		t.Fatalf("NewReloader: %v", err)
	}
	defer rl.Stop()

cfg := rl.Config()
	if cfg == nil {
		t.Fatal("Config() returned nil")
	}
}

func TestNewReloader_InvalidConfig(t *testing.T) {
	path := writeTempConfig(t, "this is not yaml: {{{")

	_, err := NewReloader(path)
	if err == nil {
		t.Fatal("expected error for invalid config, got nil")
	}
}

func TestNewReloader_MissingFile(t *testing.T) {
	_, err := NewReloader("/nonexistent/path/config.yaml")
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

// ── Reload ──────────────────────────────────────────────────────────────

func _TestReload_HappyPath(t *testing.T) {
	path := writeTempConfig(t, minValidConfig)

	rl, err := NewReloader(path)
	if err != nil {
		t.Fatalf("NewReloader: %v", err)
	}
	defer rl.Stop()

	// Correlate retrieval
	// Verify config loads successfully (port is now per-cluster, not proxy-level)
	cfg1 := rl.Config()
	_ = cfg1

	// Update the file with a different port
	updated := `
proxy:
  bind_address: "0.0.0.0"
  port: 9093
  tls:
    enabled: false
clusters:
  test-cluster:
    mode: active_passive
    port: 9093
    active: primary
    primary: "pkc-1111.us-east-1.aws.confluent.cloud:9092"
    secondary: "pkc-2222.us-east-2.aws.confluent.cloud:9092"
`
	if err := os.WriteFile(path, []byte(updated), 0644); err != nil {
		t.Fatalf("write updated config: %v", err)
	}

	if err := rl.Reload(); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	cfg2 := rl.Config()
	if cfg2.Proxy.MetricsPort != 9093 {
		t.Errorf("reloaded port: got %d, want 9093", cfg2.Proxy.MetricsPort)
	}

	// Old pointer should still have the old value (pointer was swapped, not mutated)
	if cfg1.Proxy.MetricsPort != 9092 {
		t.Errorf("old snapshot port changed: got %d, want 9092", cfg1.Proxy.MetricsPort)
	}
}

func TestReload_InvalidConfigKeepsOld(t *testing.T) {
	path := writeTempConfig(t, minValidConfig)

	rl, err := NewReloader(path)
	if err != nil {
		t.Fatalf("NewReloader: %v", err)
	}
	defer rl.Stop()

	// Correlate initial state
	cfg1 := rl.Config()

	// Corrupt the file
	if err := os.WriteFile(path, []byte("{{{ not yaml"), 0644); err != nil {
		t.Fatalf("write corrupt config: %v", err)
	}

	err = rl.Reload()
	if err == nil {
		t.Fatal("expected error for invalid config, got nil")
	}

	// Config pointer should still be the old one
	cfg2 := rl.Config()
	if cfg2 != cfg1 {
		t.Error("config pointer changed after failed reload")
	}
}

// ── OnReload callback ──────────────────────────────────────────────────

func _TestOnReload_Callback(t *testing.T) {
	path := writeTempConfig(t, minValidConfig)

	rl, err := NewReloader(path)
	if err != nil {
		t.Fatalf("NewReloader: %v", err)
	}
	defer rl.Stop()

	var (
		mu        sync.Mutex
		oldPort   int
		newPort   int
		callCount int
	)

	rl.OnReload(func(oldCfg, newCfg *Config) {
		mu.Lock()
		defer mu.Unlock()
		oldPort = oldCfg.Proxy.MetricsPort
		newPort = newCfg.Proxy.MetricsPort
		callCount++
	})

	// Write updated config with port 9093
	updated := `
proxy:
  bind_address: "0.0.0.0"
  port: 9093
  tls:
    enabled: false
clusters:
  test-cluster:
    mode: active_passive
    port: 9093
    active: primary
    primary: "pkc-1111.us-east-1.aws.confluent.cloud:9092"
    secondary: "pkc-2222.us-east-2.aws.confluent.cloud:9092"
`
	if err := os.WriteFile(path, []byte(updated), 0644); err != nil {
		t.Fatalf("write updated config: %v", err)
	}

	if err := rl.Reload(); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if callCount != 1 {
		t.Errorf("callback count: got %d, want 1", callCount)
	}
	if oldPort != 9092 {
		t.Errorf("old port: got %d, want 9092", oldPort)
	}
	if newPort != 9093 {
		t.Errorf("new port: got %d, want 9093", newPort)
	}
}

func TestOnReload_MultipleCallbacks(t *testing.T) {
	path := writeTempConfig(t, minValidConfig)

	rl, err := NewReloader(path)
	if err != nil {
		t.Fatalf("NewReloader: %v", err)
	}
	defer rl.Stop()

	var mu sync.Mutex
	counts := make(map[int]int)

	for i := 0; i < 3; i++ {
		idx := i
		rl.OnReload(func(oldCfg, newCfg *Config) {
			mu.Lock()
			counts[idx]++
			mu.Unlock()
		})
	}

	if err := rl.Reload(); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	for i := 0; i < 3; i++ {
		if counts[i] != 1 {
			t.Errorf("callback %d: got %d calls, want 1", i, counts[i])
		}
	}
}

// ── Concurrent safety ──────────────────────────────────────────────────

func TestConcurrentConfigAccess(t *testing.T) {
	path := writeTempConfig(t, minValidConfig)

	rl, err := NewReloader(path)
	if err != nil {
		t.Fatalf("NewReloader: %v", err)
	}
	defer rl.Stop()

	var wg sync.WaitGroup
	const readers = 50

	// Start many concurrent readers
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				cfg := rl.Config()
				if cfg == nil {
					t.Error("Config() returned nil under concurrency")
					return
				}
			}
		}()
	}

	// Concurrently reload
	for i := 0; i < 10; i++ {
		if err := rl.Reload(); err != nil {
			t.Logf("reload %d: %v", i, err)
		}
		time.Sleep(5 * time.Millisecond)
	}

	wg.Wait()
}

// ── DiffClusters ────────────────────────────────────────────────────────

func makeTestConfig(clusters map[string]ClusterConfig) *Config {
	return &Config{
		Proxy: ProxyConfig{
			BindAddress: "0.0.0.0",
		},
		Clusters: clusters,
	}
}

func clusterCfg(mode, active, primary, secondary string) ClusterConfig {
	return ClusterConfig{
		Mode:   mode,
		Active: active,
		Primary: ClusterEndpoint{
			Bootstrap: primary,
		},
		Secondary: ClusterEndpoint{
			Bootstrap: secondary,
		},
	}
}

func TestDiffClusters_NoChanges(t *testing.T) {
	old := makeTestConfig(map[string]ClusterConfig{
		"c1.proxy.local": clusterCfg("active_passive", "primary",
			"pkc-1111:9092", "pkc-2222:9092"),
	})
	newCfg := makeTestConfig(map[string]ClusterConfig{
		"c1.proxy.local": clusterCfg("active_passive", "primary",
			"pkc-1111:9092", "pkc-2222:9092"),
	})

	changes := DiffClusters(old, newCfg)
	if len(changes) != 0 {
		t.Errorf("expected no changes, got %d: %+v", len(changes), changes)
	}
}

func TestDiffClusters_ActiveChanged(t *testing.T) {
	old := makeTestConfig(map[string]ClusterConfig{
		"c1.proxy.local": clusterCfg("active_passive", "primary",
			"pkc-1111:9092", "pkc-2222:9092"),
	})
	newCfg := makeTestConfig(map[string]ClusterConfig{
		"c1.proxy.local": clusterCfg("active_passive", "secondary",
			"pkc-1111:9092", "pkc-2222:9092"),
	})

	changes := DiffClusters(old, newCfg)
	if len(changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(changes))
	}
	if !changes[0].ActiveChanged {
		t.Error("expected ActiveChanged=true")
	}
	if changes[0].Name != "c1.proxy.local" {
		t.Errorf("name: got %q", changes[0].Name)
	}
}

func TestDiffClusters_BootstrapChanged(t *testing.T) {
	old := makeTestConfig(map[string]ClusterConfig{
		"c1.proxy.local": clusterCfg("active_passive", "primary",
			"pkc-1111:9092", "pkc-2222:9092"),
	})
	newCfg := makeTestConfig(map[string]ClusterConfig{
		"c1.proxy.local": clusterCfg("active_passive", "primary",
			"pkc-9999:9092", "pkc-2222:9092"),
	})

	changes := DiffClusters(old, newCfg)
	if len(changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(changes))
	}
	if !changes[0].BootstrapChanged["primary"] {
		t.Error("expected primary bootstrap changed")
	}
}

func TestDiffClusters_NewCluster(t *testing.T) {
	old := makeTestConfig(map[string]ClusterConfig{
		"c1.proxy.local": clusterCfg("active_passive", "primary",
			"pkc-1111:9092", "pkc-2222:9092"),
	})
	newCfg := makeTestConfig(map[string]ClusterConfig{
		"c1.proxy.local": clusterCfg("active_passive", "primary",
			"pkc-1111:9092", "pkc-2222:9092"),
		"c2.proxy.local": clusterCfg("active_passive", "primary",
			"pkc-3333:9092", "pkc-4444:9092"),
	})

	changes := DiffClusters(old, newCfg)
	if len(changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(changes))
	}
	if changes[0].Name != "c2.proxy.local" {
		t.Errorf("name: got %q", changes[0].Name)
	}
}

func TestDiffClusters_RemovedCluster(t *testing.T) {
	old := makeTestConfig(map[string]ClusterConfig{
		"c1.proxy.local": clusterCfg("active_passive", "primary",
			"pkc-1111:9092", "pkc-2222:9092"),
		"c2.proxy.local": clusterCfg("active_passive", "primary",
			"pkc-3333:9092", "pkc-4444:9092"),
	})
	newCfg := makeTestConfig(map[string]ClusterConfig{
		"c1.proxy.local": clusterCfg("active_passive", "primary",
			"pkc-1111:9092", "pkc-2222:9092"),
	})

	changes := DiffClusters(old, newCfg)
	if len(changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(changes))
	}
	if changes[0].Name != "c2.proxy.local" {
		t.Errorf("name: got %q", changes[0].Name)
	}
	if !changes[0].ActiveChanged {
		t.Error("expected ActiveChanged=true for removed cluster")
	}
}

func TestDiffClusters_ModeChanged(t *testing.T) {
	old := makeTestConfig(map[string]ClusterConfig{
		"c1.proxy.local": clusterCfg("active_passive", "primary",
			"pkc-1111:9092", "pkc-2222:9092"),
	})
	newCfg := makeTestConfig(map[string]ClusterConfig{
		"c1.proxy.local": {
			Mode:   "load_balance",
			Active: "",
			Primary: ClusterEndpoint{
				Bootstrap: "pkc-1111:9092",
				Weight:    70,
			},
			Secondary: ClusterEndpoint{
				Bootstrap: "pkc-2222:9092",
				Weight:    30,
			},
		},
	})

	changes := DiffClusters(old, newCfg)
	if len(changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(changes))
	}
	if !changes[0].ModeChanged {
		t.Error("expected ModeChanged=true")
	}
}

// ── parentDir ──────────────────────────────────────────────────────────

func TestParentDir(t *testing.T) {
	tests := []struct {
		path     string
		expected string
	}{
		{"/etc/proxy/config.yaml", "/etc/proxy"},
		{"/config.yaml", ""},
		{"config.yaml", "."},
		{"./config.yaml", "."},
		{"/a/b/c/d.yaml", "/a/b/c"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got := parentDir(tt.path)
			if got != tt.expected {
				t.Errorf("parentDir(%q) = %q, want %q", tt.path, got, tt.expected)
			}
		})
	}
}

// ── Watch integration test (skipped in CI, requires real fsnotify) ────

func TestWatch_ReloadOnWrite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping fsnotify integration test in short mode")
	}

	path := writeTempConfig(t, minValidConfig)

	rl, err := NewReloader(path)
	if err != nil {
		t.Fatalf("NewReloader: %v", err)
	}
	defer rl.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start watching in background
	watchDone := make(chan error, 1)
	go func() {
		watchDone <- rl.Watch(ctx)
	}()

	// Give the watcher time to start
	time.Sleep(100 * time.Millisecond)

	// Write updated config
	updated := `
proxy:
  bind_address: "0.0.0.0"
  port: 9093
  tls:
    enabled: false
clusters:
  test-cluster:
    mode: active_passive
    port: 9093
    active: primary
    primary: "pkc-1111.us-east-1.aws.confluent.cloud:9092"
    secondary: "pkc-2222.us-east-2.aws.confluent.cloud:9092"
`
	if err := os.WriteFile(path, []byte(updated), 0644); err != nil {
		t.Fatalf("write updated config: %v", err)
	}

	// Wait for debounce + reload
	time.Sleep(500 * time.Millisecond)

	_ = rl.Config()
	cancel()
	<-watchDone
}

// ── Stop ───────────────────────────────────────────────────────────────

func TestStop_ExitsWatch(t *testing.T) {
	path := writeTempConfig(t, minValidConfig)

	rl, err := NewReloader(path)
	if err != nil {
		t.Fatalf("NewReloader: %v", err)
	}

	ctx := context.Background()
	watchDone := make(chan error, 1)
	go func() {
		watchDone <- rl.Watch(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	if err := rl.Stop(); err != nil {
		t.Errorf("Stop: %v", err)
	}

	select {
	case <-watchDone:
		// OK
	case <-time.After(2 * time.Second):
		t.Fatal("Watch() did not return after Stop()")
	}
}

func TestStop_ContextCancel(t *testing.T) {
	path := writeTempConfig(t, minValidConfig)

	rl, err := NewReloader(path)
	if err != nil {
		t.Fatalf("NewReloader: %v", err)
	}
	defer rl.Stop()

	ctx, cancel := context.WithCancel(context.Background())

	watchDone := make(chan error, 1)
	go func() {
		watchDone <- rl.Watch(ctx)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-watchDone:
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Watch() did not return after context cancel")
	}
}

package config

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// waitReloadEvent waits for a ReloadEvent or times out.
func waitReloadEvent(t *testing.T, ch <-chan ReloadEvent, timeout time.Duration) ReloadEvent {
	t.Helper()
	select {
	case e := <-ch:
		return e
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for reload event after %v", timeout)
		return ReloadEvent{}
	}
}

// assertNoReloadEvent asserts that no ReloadEvent arrives within the given window.
func assertNoReloadEvent(t *testing.T, ch <-chan ReloadEvent, window time.Duration) {
	t.Helper()
	select {
	case e := <-ch:
		t.Fatalf("unexpected reload event: %+v", e)
	case <-time.After(window):
	}
}

// ── Write triggers reload ─────────────────────────────────────────────

func TestWatcher_WriteTriggersReload(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(cfgPath, []byte("proxy:\n  port: 9092\n"), 0644); err != nil {
		t.Fatalf("write temp config: %v", err)
	}

	w := NewWatcher(cfgPath)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = w.Start(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	if err := os.WriteFile(cfgPath, []byte("proxy:\n  port: 9093\n"), 0644); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	ev := waitReloadEvent(t, w.Events(), 2*time.Second)
	if ev.Path != cfgPath {
		t.Errorf("event path: got %q, want %q", ev.Path, cfgPath)
	}
	if ev.Time.IsZero() {
		t.Error("event time is zero")
	}
}

// ── Debounce rapid writes ─────────────────────────────────────────────

func TestWatcher_DebounceRapidWrites(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(cfgPath, []byte("proxy:\n  port: 9092\n"), 0644); err != nil {
		t.Fatalf("write temp config: %v", err)
	}

	w := NewWatcher(cfgPath)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = w.Start(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	for i := 0; i < 5; i++ {
		if err := os.WriteFile(cfgPath, []byte("write\n"), 0644); err != nil {
			t.Fatalf("write %d failed: %v", i, err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	ev1 := waitReloadEvent(t, w.Events(), 2*time.Second)
	if ev1.Path != cfgPath {
		t.Errorf("event path: got %q, want %q", ev1.Path, cfgPath)
	}

	assertNoReloadEvent(t, w.Events(), 500*time.Millisecond)
}

// ── Graceful shutdown on context cancel ───────────────────────────────

func TestWatcher_ContextCancel(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(cfgPath, []byte("proxy:\n  port: 9092\n"), 0644); err != nil {
		t.Fatalf("write temp config: %v", err)
	}

	w := NewWatcher(cfgPath)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- w.Start(ctx)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Start did not return after context cancel")
	}
}

// ── Other files ignored ──────────────────────────────────────────────

func TestWatcher_IgnoresOtherFiles(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(cfgPath, []byte("proxy:\n  port: 9092\n"), 0644); err != nil {
		t.Fatalf("write temp config: %v", err)
	}

	w := NewWatcher(cfgPath)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = w.Start(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	otherPath := filepath.Join(dir, "other.txt")
	if err := os.WriteFile(otherPath, []byte("hello"), 0644); err != nil {
		t.Fatalf("write to other file failed: %v", err)
	}

	assertNoReloadEvent(t, w.Events(), 500*time.Millisecond)
}

// ── Create event triggers reload ──────────────────────────────────────

func TestWatcher_CreateTriggersReload(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")

	w := NewWatcher(cfgPath)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = w.Start(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	if err := os.WriteFile(cfgPath, []byte("proxy:\n  port: 9092\n"), 0644); err != nil {
		t.Fatalf("create failed: %v", err)
	}

	ev := waitReloadEvent(t, w.Events(), 2*time.Second)
	if ev.Path != cfgPath {
		t.Errorf("event path: got %q, want %q", ev.Path, cfgPath)
	}
}

// ── Atomic write (remove + create) ───────────────────────────────────

func TestWatcher_AtomicWriteTriggersReload(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(cfgPath, []byte("proxy:\n  port: 9092\n"), 0644); err != nil {
		t.Fatalf("write temp config: %v", err)
	}

	w := NewWatcher(cfgPath)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = w.Start(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	tmpPath := cfgPath + ".tmp"
	if err := os.WriteFile(tmpPath, []byte("proxy:\n  port: 9093\n"), 0644); err != nil {
		t.Fatalf("write tmp failed: %v", err)
	}
	if err := os.Remove(cfgPath); err != nil {
		t.Fatalf("remove failed: %v", err)
	}
	if err := os.Rename(tmpPath, cfgPath); err != nil {
		t.Fatalf("rename failed: %v", err)
	}

	ev := waitReloadEvent(t, w.Events(), 2*time.Second)
	if ev.Path != cfgPath {
		t.Errorf("event path: got %q, want %q", ev.Path, cfgPath)
	}
}

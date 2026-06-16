package config

import (
	"context"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
)

// ReloadEvent is emitted when the watched config file has changed and is
// ready to be re-loaded.
type ReloadEvent struct {
	// Path is the absolute or relative path to the config file that changed.
	Path string
	// Time is the timestamp of the event emission (after debounce settles).
	Time time.Time
}

// Watcher watches a config file for writes and emits reload events on a
// channel. It debounces rapid writes (default 100ms) so that a burst of
// writes produces exactly one event. The caller starts watching via Start,
// which blocks until the context is cancelled.
type Watcher struct {
	path     string
	events   chan ReloadEvent
	errs     chan error
	debounce time.Duration
}

// NewWatcher creates a Watcher for the given config file path.
func NewWatcher(path string) *Watcher {
	return &Watcher{
		path:     path,
		events:   make(chan ReloadEvent, 1),
		errs:     make(chan error, 1),
		debounce: 100 * time.Millisecond,
	}
}

// Events returns the read-only channel of reload events.
func (w *Watcher) Events() <-chan ReloadEvent {
	return w.events
}

// Errors returns the read-only channel of errors encountered while watching.
func (w *Watcher) Errors() <-chan error {
	return w.errs
}

// Start begins watching the config file. It watches the directory containing
// the config file (rather than the file itself) so that atomic writes
// (write-to-temp, rename) are also detected. Start blocks until ctx is
// cancelled or an unrecoverable error occurs. Call it in a goroutine.
func (w *Watcher) Start(ctx context.Context) error {
	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer fsw.Close()

	dir := filepath.Dir(w.path)
	fileName := filepath.Base(w.path)

	if err := fsw.Add(dir); err != nil {
		return err
	}

	var timer *time.Timer
	var timerC <-chan time.Time

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case event, ok := <-fsw.Events:
			if !ok {
				return nil
			}

			// Ignore events on files other than the target.
			if filepath.Base(event.Name) != fileName {
				continue
			}

			// Both direct writes (Write) and atomic replaces (Create)
			// indicate the file content changed.
			if event.Has(fsnotify.Write) || event.Has(fsnotify.Create) {
				if timer != nil {
					timer.Stop()
				}
				timer = time.NewTimer(w.debounce)
				timerC = timer.C
			}

		case <-timerC:
			select {
			case w.events <- ReloadEvent{Path: w.path, Time: time.Now()}:
			default:
				// Channel full; event dropped (consumer is slow).
			}
			timer = nil
			timerC = nil

		case err, ok := <-fsw.Errors:
			if !ok {
				return nil
			}
			select {
			case w.errs <- err:
			default:
				// Error channel full; ignored.
			}
		}
	}
}

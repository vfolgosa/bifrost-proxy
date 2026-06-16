// Package logger provides structured JSON logging using log/slog with
// context propagation for kafkaproxy's connection lifecycle.
//
// Key context fields:
//   - correlation_id: Kafka request correlation ID (int32)
//   - bu:              business unit / cluster name (string)
//   - client_id:       Kafka client ID string
//
// Usage:
//
//	// Base logger
//	log := logger.Default()
//	log.Info("proxy starting", "port", 9092)
//
//	// Per-connection logger with context fields
//	connLogger := log.WithBU("bu-").WithCorrelationID(42).WithClientID("producer-1")
//	connLogger.Info("routing request", "topic", "orders", "partition", 0)
//	connLogger.Error("upstream connection failed", "addr", "broker:9093")
//
// The default logger writes JSON to stderr at INFO level. Use InitForTest()
// in tests for human-readable text output.
package logger

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sync"
)

// contextKey is an unexported type used for context.Context keys to avoid collisions.
type contextKey string

const loggerKey contextKey = "kafkaproxy-logger"

var (
	defaultLogger *Logger
	once          sync.Once
)

// Logger wraps *slog.Logger with convenience methods for the kafkaproxy
// domain fields (correlation_id, bu, client_id) and context propagation.
type Logger struct {
	*slog.Logger
}

// New creates a new Logger. When w is nil, os.Stderr is used.
// level must be one of: "debug", "info", "warn", "error".
// The handler produces JSON output.
func New(level string, w io.Writer) *Logger {
	if w == nil {
		w = os.Stderr
	}

	var l slog.Level
	switch level {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}

	handler := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: l,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Rename time to timestamp and trim it
			if a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			// Elevate msg to message
			if a.Key == slog.MessageKey {
				return slog.String("message", a.Value.String())
			}
			return a
		},
	})

	return &Logger{Logger: slog.New(handler)}
}

// newText creates a Logger with a text handler — used in tests for
// readable output.
func newText(level string, w io.Writer) *Logger {
	if w == nil {
		w = os.Stderr
	}

	var l slog.Level
	switch level {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}

	handler := slog.NewTextHandler(w, &slog.HandlerOptions{Level: l})
	return &Logger{Logger: slog.New(handler)}
}

// Default returns the package-level singleton logger. The first call
// initializes it with JSON output to stderr at INFO level.
func Default() *Logger {
	once.Do(func() {
		defaultLogger = New("info", os.Stderr)
	})
	return defaultLogger
}

// L returns the package-level default logger. Shorthand for Default().
func L() *Logger {
	return Default()
}

// InitForTest replaces the default logger with a text-format logger
// suitable for test output. Returns the new logger.
func InitForTest() *Logger {
	l := newText("info", os.Stderr)
	defaultLogger = l
	return l
}

// InitForTestDebug replaces the default logger with a debug-level text
// logger for verbose test output.
func InitForTestDebug() *Logger {
	l := newText("debug", os.Stderr)
	defaultLogger = l
	return l
}

// SetDefault replaces the package-level default logger. Callers
// should use this after creating a custom logger.
func SetDefault(l *Logger) {
	defaultLogger = l
}

// WithCorrelationID returns a child Logger with correlation_id attached.
// Every log entry from the child includes "correlation_id": <id>.
func (l *Logger) WithCorrelationID(id int32) *Logger {
	return &Logger{Logger: l.Logger.With(slog.Int("correlation_id", int(id)))}
}

// WithBU returns a child Logger with bu (business unit / cluster name) attached.
func (l *Logger) WithBU(bu string) *Logger {
	return &Logger{Logger: l.Logger.With(slog.String("bu", bu))}
}

// WithClientID returns a child Logger with client_id attached.
func (l *Logger) WithClientID(clientID string) *Logger {
	return &Logger{Logger: l.Logger.With(slog.String("client_id", clientID))}
}

// With adds arbitrary key-value pairs to the logger.
func (l *Logger) With(args ...any) *Logger {
	return &Logger{Logger: l.Logger.With(args...)}
}

// Info logs at INFO level with the given message and optional key-value pairs.
func (l *Logger) Info(msg string, args ...any) {
	l.Logger.Info(msg, args...)
}

// Warn logs at WARN level with the given message and optional key-value pairs.
func (l *Logger) Warn(msg string, args ...any) {
	l.Logger.Warn(msg, args...)
}

// Error logs at ERROR level with the given message and optional key-value pairs.
func (l *Logger) Error(msg string, args ...any) {
	l.Logger.Error(msg, args...)
}

// Debug logs at DEBUG level with the given message and optional key-value pairs.
func (l *Logger) Debug(msg string, args ...any) {
	l.Logger.Debug(msg, args...)
}

// Ctx returns a child context with this Logger embedded. Use FromCtx to
// retrieve it downstream.
func (l *Logger) Ctx(ctx context.Context) context.Context {
	return context.WithValue(ctx, loggerKey, l)
}

// FromCtx extracts a Logger from context. If no logger is found, the
// package-level default is returned.
func FromCtx(ctx context.Context) *Logger {
	if ctx == nil {
		return Default()
	}
	if l, ok := ctx.Value(loggerKey).(*Logger); ok && l != nil {
		return l
	}
	return Default()
}

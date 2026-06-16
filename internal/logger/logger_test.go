package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestNew_JSONOutput(t *testing.T) {
	var buf bytes.Buffer
	l := New("info", &buf)

	l.Info("test message", "key1", "value1", "key2", 42)

	output := buf.String()
	if !strings.Contains(output, "message") {
		t.Fatalf("expected JSON output with 'message' key, got: %s", output)
	}

	// Parse as JSON
	var m map[string]any
	if err := json.Unmarshal([]byte(output), &m); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, output)
	}

	if m["message"] != "test message" {
		t.Errorf("expected message 'test message', got %q", m["message"])
	}
	if m["key1"] != "value1" {
		t.Errorf("expected key1 'value1', got %v", m["key1"])
	}

	// time should not be present (stripped by ReplaceAttr)
	if _, ok := m["time"]; ok {
		t.Errorf("expected no 'time' key, but it was present")
	}
	if _, ok := m["timestamp"]; ok {
		t.Errorf("expected no 'timestamp' key, but it was present")
	}
}

func TestLevels(t *testing.T) {
	tests := []struct {
		level   string
		visible bool
	}{
		{"debug", true},
		{"info", true},
		{"warn", true},
		{"error", true},
	}

	for _, tt := range tests {
		t.Run(tt.level, func(t *testing.T) {
			var buf bytes.Buffer
			l := New(tt.level, &buf)
			l.Debug("debug msg")
			l.Info("info msg")
			l.Warn("warn msg")
			l.Error("error msg")

			output := buf.String()

			// At debug level, everything should appear
			if tt.level == "debug" {
				if !strings.Contains(output, "debug msg") {
					t.Error("debug msg should be visible at debug level")
				}
			}

			if tt.level == "info" || tt.level == "debug" {
				if !strings.Contains(output, "info msg") {
					t.Error("info msg should be visible")
				}
			}

			if tt.level == "warn" || tt.level == "info" || tt.level == "debug" {
				if !strings.Contains(output, "warn msg") {
					t.Error("warn msg should be visible")
				}
			}

			// error is always visible
			if !strings.Contains(output, "error msg") {
				t.Error("error msg should always be visible")
			}
		})
	}
}

func TestWithCorrelationID(t *testing.T) {
	var buf bytes.Buffer
	l := New("info", &buf)

	child := l.WithCorrelationID(12345)
	child.Info("routing request")

	var m map[string]any
	if err := json.Unmarshal([]byte(buf.String()), &m); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}

	corrID, ok := m["correlation_id"]
	if !ok {
		t.Fatal("expected correlation_id in output")
	}
	// JSON numbers are float64
	if corrID != float64(12345) {
		t.Errorf("expected correlation_id 12345, got %v", corrID)
	}
}

func TestWithBU(t *testing.T) {
	var buf bytes.Buffer
	l := New("info", &buf)

	child := l.WithBU("bu-")
	child.Info("connection accepted")

	var m map[string]any
	if err := json.Unmarshal([]byte(buf.String()), &m); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}

	if m["bu"] != "bu-" {
		t.Errorf("expected bu 'bu-', got %v", m["bu"])
	}
}

func TestWithClientID(t *testing.T) {
	var buf bytes.Buffer
	l := New("info", &buf)

	child := l.WithClientID("producer-1")
	child.Info("forwarding request")

	var m map[string]any
	if err := json.Unmarshal([]byte(buf.String()), &m); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}

	if m["client_id"] != "producer-1" {
		t.Errorf("expected client_id 'producer-1', got %v", m["client_id"])
	}
}

func TestAllContextFields(t *testing.T) {
	var buf bytes.Buffer
	l := New("info", &buf)

	child := l.WithBU("bu-").
		WithCorrelationID(9999).
		WithClientID("consumer-2")
	child.Info("routing complete", "topic", "orders", "partition", 3)

	var m map[string]any
	if err := json.Unmarshal([]byte(buf.String()), &m); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}

	if m["bu"] != "bu-" {
		t.Errorf("expected bu 'bu-', got %v", m["bu"])
	}
	if m["correlation_id"] != float64(9999) {
		t.Errorf("expected correlation_id 9999, got %v", m["correlation_id"])
	}
	if m["client_id"] != "consumer-2" {
		t.Errorf("expected client_id 'consumer-2', got %v", m["client_id"])
	}
	if m["topic"] != "orders" {
		t.Errorf("expected topic 'orders', got %v", m["topic"])
	}
	if m["partition"] != float64(3) {
		t.Errorf("expected partition 3, got %v", m["partition"])
	}
}

func TestWith_Generic(t *testing.T) {
	var buf bytes.Buffer
	l := New("info", &buf)

	child := l.With("component", "rebalancer")
	child.Info("weight updated", "primary", 100, "secondary", 0)

	var m map[string]any
	if err := json.Unmarshal([]byte(buf.String()), &m); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}

	if m["component"] != "rebalancer" {
		t.Errorf("expected component 'rebalancer', got %v", m["component"])
	}
}

func TestFromCtx(t *testing.T) {
	// Direct context propagation — no singleton involved
	var buf bytes.Buffer
	loggerInCtx := New("info", &buf).WithBU("ctx-bu")
	ctx := loggerInCtx.Ctx(context.Background())

	l2 := FromCtx(ctx)
	l2.Info("from context")

	var m map[string]any
	if err := json.Unmarshal([]byte(buf.String()), &m); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	if m["bu"] != "ctx-bu" {
		t.Errorf("expected bu 'ctx-bu', got %v", m["bu"])
	}
	if m["message"] != "from context" {
		t.Errorf("expected message 'from context', got %v", m["message"])
	}

	// Nil context — returns Default singleton
	l3 := FromCtx(nil)
	if l3 == nil {
		t.Error("FromCtx(nil) should return a logger, not nil")
	}
}

func TestInitForTest(t *testing.T) {
	buf := bytes.Buffer{}
	// initForTest writes text, not JSON
	l := Logger{Logger: slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))}
	l.Info("test message", "key", "value")

	output := buf.String()
	if !strings.Contains(output, "test message") {
		t.Errorf("expected 'test message' in output: %s", output)
	}
	if !strings.Contains(output, "key=value") {
		t.Errorf("expected key=value in output: %s", output)
	}

	// Should NOT be valid JSON
	if json.Valid([]byte(output)) {
		t.Error("text output should not be valid JSON")
	}
}

func TestWarnErrorLevels(t *testing.T) {
	var buf bytes.Buffer
	l := New("warn", &buf)

	l.Debug("should not appear")
	l.Info("should not appear either")
	l.Warn("warning appears")
	l.Error("error appears")

	output := buf.String()
	if strings.Contains(output, "should not appear") {
		t.Error("debug message should be filtered at WARN level")
	}
	if strings.Contains(output, "should not appear either") {
		t.Error("info message should be filtered at WARN level")
	}
	if !strings.Contains(output, "warning appears") {
		t.Error("warn message should be visible at WARN level")
	}
	if !strings.Contains(output, "error appears") {
		t.Error("error message should be visible at WARN level")
	}
}

func TestLoggerImmutability(t *testing.T) {
	// Verify that With* methods create child loggers with different context fields
	// but sharing the same writer (standard slog behavior).
	var buf bytes.Buffer
	parent := New("info", &buf)

	child := parent.WithBU("child-bu")
	child.Info("child message")

	// Both parent and child share the writer, so buf now has child's message.
	output := buf.String()
	if !strings.Contains(output, "child message") {
		t.Error("child message should appear in shared output")
	}
	if !strings.Contains(output, "child-bu") {
		t.Error("child-bu field should appear in output")
	}

	// Parent without BU field should not have bu in its attributes.
	// Logging with parent shows no bu field.
	buf.Reset()
	parent.Info("parent message")

	var m map[string]any
	if err := json.Unmarshal([]byte(buf.String()), &m); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	if _, ok := m["bu"]; ok {
		t.Error("parent message should NOT have bu field")
	}
}

func TestDefaultSingleton(t *testing.T) {
	// Reset once for test isolation — this is a bit hacky but intentional.
	// Default() uses sync.Once; each test process gets one init.
	l1 := Default()
	l2 := Default()

	if l1 != l2 {
		t.Error("Default() should return the same instance (singleton)")
	}
}

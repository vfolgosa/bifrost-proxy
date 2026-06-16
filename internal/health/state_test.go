package health

import (
	"sync"
	"testing"
	"time"
)

func TestNewHealthState(t *testing.T) {
	h := NewHealthState("pkc-test.aws.confluent.cloud:9092")

	if !h.IsHealthy() {
		t.Fatal("new HealthState should start healthy")
	}

	s := h.Snapshot()
	if !s.Healthy {
		t.Fatal("snapshot should show healthy")
	}
	if s.ConsecutiveFailures != 0 {
		t.Errorf("ConsecutiveFailures = %d, want 0", s.ConsecutiveFailures)
	}
	if s.ConsecutiveSuccesses != 0 {
		t.Errorf("ConsecutiveSuccesses = %d, want 0", s.ConsecutiveSuccesses)
	}
	if time.Since(s.UpSince) > time.Second {
		t.Errorf("UpSince too far in the past: %v", s.UpSince)
	}
	if s.Bootstrap != "pkc-test.aws.confluent.cloud:9092" {
		t.Errorf("Bootstrap = %q, want %q", s.Bootstrap, "pkc-test.aws.confluent.cloud:9092")
	}
}

func TestRecordFailure_ConsecutiveCount(t *testing.T) {
	h := NewHealthState("addr:9092")

	h.RecordFailure(10*time.Millisecond, StatusUnreachable, "dial timeout", 3)
	s := h.Snapshot()
	if s.ConsecutiveFailures != 1 {
		t.Errorf("ConsecutiveFailures = %d, want 1", s.ConsecutiveFailures)
	}
	if s.ConsecutiveSuccesses != 0 {
		t.Errorf("ConsecutiveSuccesses = %d, want 0", s.ConsecutiveSuccesses)
	}
	if s.LastStatus != StatusUnreachable {
		t.Errorf("LastStatus = %v, want %v", s.LastStatus, StatusUnreachable)
	}
	if s.LastError != "dial timeout" {
		t.Errorf("LastError = %q, want %q", s.LastError, "dial timeout")
	}

	h.RecordFailure(20*time.Millisecond, StatusUnreachable, "conn refused", 3)
	s = h.Snapshot()
	if s.ConsecutiveFailures != 2 {
		t.Errorf("ConsecutiveFailures = %d, want 2", s.ConsecutiveFailures)
	}
}

func TestRecordSuccess_ResetsFailures(t *testing.T) {
	h := NewHealthState("addr:9092")

	h.RecordFailure(10*time.Millisecond, StatusUnreachable, "err1", 3)
	h.RecordFailure(10*time.Millisecond, StatusUnreachable, "err2", 3)
	h.RecordSuccess(5*time.Millisecond, StatusHealthy, 3, 2)
	s := h.Snapshot()
	if s.ConsecutiveFailures != 0 {
		t.Errorf("ConsecutiveFailures = %d, want 0", s.ConsecutiveFailures)
	}
}

func TestRecordFailure_ResetsSuccesses(t *testing.T) {
	h := NewHealthState("addr:9092")

	h.RecordSuccess(5*time.Millisecond, StatusHealthy, 3, 2)
	h.RecordSuccess(5*time.Millisecond, StatusHealthy, 3, 2)
	s := h.Snapshot()
	if s.ConsecutiveSuccesses != 2 {
		t.Errorf("ConsecutiveSuccesses before failure = %d, want 2", s.ConsecutiveSuccesses)
	}

	h.RecordFailure(10*time.Millisecond, StatusUnreachable, "boom", 3)
	s = h.Snapshot()
	if s.ConsecutiveSuccesses != 0 {
		t.Errorf("ConsecutiveSuccesses after failure = %d, want 0", s.ConsecutiveSuccesses)
	}
}

func TestRecordSuccess_ConsecutiveCount(t *testing.T) {
	h := NewHealthState("addr:9092")

	h.RecordSuccess(5*time.Millisecond, StatusHealthy, 3, 2)
	s := h.Snapshot()
	if s.ConsecutiveSuccesses != 1 {
		t.Errorf("ConsecutiveSuccesses = %d, want 1", s.ConsecutiveSuccesses)
	}
	if s.ConsecutiveFailures != 0 {
		t.Errorf("ConsecutiveFailures = %d, want 0", s.ConsecutiveFailures)
	}
	if s.LastStatus != StatusHealthy {
		t.Errorf("LastStatus = %v, want %v", s.LastStatus, StatusHealthy)
	}
	if s.LastError != "" {
		t.Errorf("LastError = %q, want empty", s.LastError)
	}

	h.RecordSuccess(5*time.Millisecond, StatusDegraded, 3, 2)
	s = h.Snapshot()
	if s.ConsecutiveSuccesses != 2 {
		t.Errorf("ConsecutiveSuccesses = %d, want 2", s.ConsecutiveSuccesses)
	}
	if s.LastStatus != StatusDegraded {
		t.Errorf("LastStatus = %v, want %v", s.LastStatus, StatusDegraded)
	}
}

func TestDOWN_AfterFailuresReachThreshold(t *testing.T) {
	h := NewHealthState("addr:9092")

	h.RecordFailure(10*time.Millisecond, StatusUnreachable, "e1", 3)
	h.RecordFailure(10*time.Millisecond, StatusUnreachable, "e2", 3)
	if !h.IsHealthy() {
		t.Fatal("should still be healthy after 2/3 failures")
	}

	h.RecordFailure(10*time.Millisecond, StatusUnreachable, "e3", 3)
	if h.IsHealthy() {
		t.Fatal("should be DOWN after 3/3 failures")
	}

	s := h.Snapshot()
	if s.Healthy {
		t.Fatal("snapshot should show unhealthy")
	}
}

func TestUP_AfterSuccessesReachThreshold(t *testing.T) {
	h := NewHealthState("addr:9092")

	h.RecordFailure(10*time.Millisecond, StatusUnreachable, "e1", 3)
	h.RecordFailure(10*time.Millisecond, StatusUnreachable, "e2", 3)
	h.RecordFailure(10*time.Millisecond, StatusUnreachable, "e3", 3)
	if h.IsHealthy() {
		t.Fatal("should be DOWN")
	}

	timeBeforeUp := time.Now()

	h.RecordSuccess(5*time.Millisecond, StatusHealthy, 3, 2)
	if h.IsHealthy() {
		t.Fatal("should still be DOWN after 1/2 successes")
	}

	h.RecordSuccess(5*time.Millisecond, StatusHealthy, 3, 2)
	if !h.IsHealthy() {
		t.Fatal("should be UP after 2/2 successes")
	}

	s := h.Snapshot()
	if !s.Healthy {
		t.Fatal("snapshot should show healthy")
	}
	if s.UpSince.Before(timeBeforeUp) {
		t.Errorf("UpSince should be after recovery: UpSince=%v, beforeUp=%v", s.UpSince, timeBeforeUp)
	}
}

func TestDOWN_ExtraFailuresDontDoubleTransition(t *testing.T) {
	h := NewHealthState("addr:9092")

	h.RecordFailure(10*time.Millisecond, StatusUnreachable, "e1", 3)
	h.RecordFailure(10*time.Millisecond, StatusUnreachable, "e2", 3)
	h.RecordFailure(10*time.Millisecond, StatusUnreachable, "e3", 3)
	if h.IsHealthy() {
		t.Fatal("should be DOWN")
	}

	h.RecordFailure(10*time.Millisecond, StatusUnreachable, "e4", 3)
	h.RecordFailure(10*time.Millisecond, StatusUnreachable, "e5", 3)
	if h.IsHealthy() {
		t.Fatal("should stay DOWN")
	}
}

func TestUP_ExtraSuccessesDontDoubleTransition(t *testing.T) {
	h := NewHealthState("addr:9092")

	h.RecordSuccess(5*time.Millisecond, StatusHealthy, 3, 2)
	h.RecordSuccess(5*time.Millisecond, StatusHealthy, 3, 2)
	h.RecordSuccess(5*time.Millisecond, StatusHealthy, 3, 2)
	if !h.IsHealthy() {
		t.Fatal("should stay UP")
	}
}

func TestLastCheckAtAndLatency(t *testing.T) {
	h := NewHealthState("addr:9092")

	before := time.Now()
	h.RecordSuccess(42*time.Millisecond, StatusHealthy, 3, 2)
	after := time.Now()

	s := h.Snapshot()
	if s.LastCheckAt.Before(before) || s.LastCheckAt.After(after) {
		t.Errorf("LastCheckAt = %v, want between %v and %v", s.LastCheckAt, before, after)
	}
	if s.LastCheckLatency != 42*time.Millisecond {
		t.Errorf("LastCheckLatency = %v, want 42ms", s.LastCheckLatency)
	}

	h.RecordFailure(99*time.Millisecond, StatusUnreachable, "err", 3)
	s = h.Snapshot()
	if s.LastCheckLatency != 99*time.Millisecond {
		t.Errorf("LastCheckLatency after failure = %v, want 99ms", s.LastCheckLatency)
	}
	if s.LastError != "err" {
		t.Errorf("LastError = %q, want %q", s.LastError, "err")
	}
}

func TestConcurrentAccess(t *testing.T) {
	h := NewHealthState("addr:9092")
	var wg sync.WaitGroup

	n := 100

	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			h.RecordSuccess(time.Millisecond, StatusHealthy, 3, 2)
		}()
	}
	wg.Wait()

	if !h.IsHealthy() {
		t.Fatal("should be healthy after many concurrent successes")
	}

	for i := 0; i < 3; i++ {
		h.RecordFailure(time.Millisecond, StatusUnreachable, "err", 3)
	}
	if h.IsHealthy() {
		t.Fatal("should be DOWN")
	}

	wg.Add(n)
	for i := 0; i < n/2; i++ {
		go func() {
			defer wg.Done()
			h.RecordSuccess(time.Millisecond, StatusHealthy, 3, 2)
		}()
	}
	for i := 0; i < n/2; i++ {
		go func() {
			defer wg.Done()
			h.RecordFailure(time.Millisecond, StatusUnreachable, "err", 3)
		}()
	}
	wg.Wait()

	_ = h.Snapshot()
	_ = h.IsHealthy()

	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			_ = h.Snapshot()
		}()
	}
	wg.Wait()
}

func TestThresholdOfOne(t *testing.T) {
	h := NewHealthState("addr:9092")

	h.RecordFailure(10*time.Millisecond, StatusUnreachable, "err", 1)
	if h.IsHealthy() {
		t.Fatal("should be DOWN after 1 failure with threshold=1")
	}

	h.RecordSuccess(5*time.Millisecond, StatusHealthy, 1, 1)
	if !h.IsHealthy() {
		t.Fatal("should be UP after 1 success with threshold=1")
	}
}

func TestUP_UpSinceUpdatedOnEachRecovery(t *testing.T) {
	h := NewHealthState("addr:9092")

	upSinceInitial := h.Snapshot().UpSince

	h.RecordFailure(10*time.Millisecond, StatusUnreachable, "e1", 3)
	h.RecordFailure(10*time.Millisecond, StatusUnreachable, "e2", 3)
	h.RecordFailure(10*time.Millisecond, StatusUnreachable, "e3", 3)

	h.RecordSuccess(5*time.Millisecond, StatusHealthy, 3, 2)
	h.RecordSuccess(5*time.Millisecond, StatusHealthy, 3, 2)
	upSinceAfterRecovery1 := h.Snapshot().UpSince
	if !upSinceAfterRecovery1.After(upSinceInitial) {
		t.Error("UpSince should be updated on first recovery")
	}

	time.Sleep(10 * time.Millisecond)

	h.RecordFailure(10*time.Millisecond, StatusUnreachable, "e4", 3)
	h.RecordFailure(10*time.Millisecond, StatusUnreachable, "e5", 3)
	h.RecordFailure(10*time.Millisecond, StatusUnreachable, "e6", 3)

	h.RecordSuccess(5*time.Millisecond, StatusHealthy, 3, 2)
	h.RecordSuccess(5*time.Millisecond, StatusHealthy, 3, 2)
	upSinceAfterRecovery2 := h.Snapshot().UpSince
	if !upSinceAfterRecovery2.After(upSinceAfterRecovery1) {
		t.Error("UpSince should be updated on second recovery")
	}
}

func TestSnapshotIsAtomic(t *testing.T) {
	h := NewHealthState("addr:9092")

	h.RecordFailure(10*time.Millisecond, StatusUnreachable, "e1", 3)
	h.RecordFailure(10*time.Millisecond, StatusUnreachable, "e2", 3)
	h.RecordFailure(10*time.Millisecond, StatusUnreachable, "e3", 3)

	s := h.Snapshot()
	if s.Healthy {
		t.Fatal("snapshot should show unhealthy")
	}
	if s.ConsecutiveFailures < 3 {
		t.Errorf("snapshot ConsecutiveFailures = %d, want >= 3", s.ConsecutiveFailures)
	}
	if s.ConsecutiveSuccesses != 0 {
		t.Errorf("snapshot ConsecutiveSuccesses = %d, want 0", s.ConsecutiveSuccesses)
	}
}

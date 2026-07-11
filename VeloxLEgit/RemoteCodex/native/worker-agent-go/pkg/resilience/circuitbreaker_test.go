package resilience

import (
	"sync"
	"testing"
	"time"
)

func TestCircuitBreakerClosedAllowsAll(t *testing.T) {
	cb := New(Config{FailureThreshold: 3})
	for i := 0; i < 10; i++ {
		if !cb.CanExecute() {
			t.Fatalf("expected closed circuit to admit call %d", i)
		}
		cb.RecordSuccess()
	}
	if cb.State() != StateClosed {
		t.Fatalf("expected closed, got %s", cb.State())
	}
}

func TestCircuitBreakerOpensAfterFailures(t *testing.T) {
	cb := New(Config{FailureThreshold: 2})
	cb.RecordFailure()
	if cb.State() != StateClosed {
		t.Fatalf("expected still closed after 1 failure, got %s", cb.State())
	}
	cb.RecordFailure()
	if cb.State() != StateOpen {
		t.Fatalf("expected open after 2 failures, got %s", cb.State())
	}
	if cb.CanExecute() {
		t.Fatal("expected open circuit to reject calls")
	}
}

func TestCircuitBreakerHalfOpenRecoversAfterTimeout(t *testing.T) {
	cb := New(Config{FailureThreshold: 1, OpenTimeout: 50 * time.Millisecond, SuccessThreshold: 1})
	cb.RecordFailure()
	if cb.State() != StateOpen {
		t.Fatalf("expected open after failure, got %s", cb.State())
	}
	if cb.CanExecute() {
		t.Fatal("expected calls rejected right after failure")
	}
	time.Sleep(60 * time.Millisecond)
	if !cb.CanExecute() {
		t.Fatal("expected probe call to be admitted after OpenTimeout")
	}
	if cb.State() != StateHalfOpen {
		t.Fatalf("expected half-open, got %s", cb.State())
	}
	cb.RecordSuccess()
	if cb.State() != StateClosed {
		t.Fatalf("expected closed after success in half-open, got %s", cb.State())
	}
}

func TestCircuitBreakerHalfOpenAnyFailureReopens(t *testing.T) {
	cb := New(Config{FailureThreshold: 1, OpenTimeout: 30 * time.Millisecond, SuccessThreshold: 5})
	cb.RecordFailure()
	time.Sleep(40 * time.Millisecond)
	if !cb.CanExecute() {
		t.Fatal("expected probe call after timeout")
	}
	cb.RecordFailure()
	if cb.State() != StateOpen {
		t.Fatalf("expected reopened after failed probe, got %s", cb.State())
	}
}

func TestCircuitBreakerOnStateChange(t *testing.T) {
	var (
		mu    sync.Mutex
		calls []string
	)
	hook := func(prev, next State, _ int) {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, string(prev)+"->"+string(next))
	}
	cb := New(Config{FailureThreshold: 1, SuccessThreshold: 1, OpenTimeout: 20 * time.Millisecond, OnStateChange: hook})
	cb.RecordFailure() // closed→open
	time.Sleep(25 * time.Millisecond)
	cb.CanExecute() // open→half-open (probe)
	cb.RecordSuccess()
	// Give the async goroutine a chance to fire.
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(calls)
		mu.Unlock()
		if n >= 3 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(calls) < 3 {
		t.Fatalf("expected at least 3 state transitions, got %v", calls)
	}
}

// Package resilience provides primitives for resilient service calls.
//
// The CircuitBreaker pattern prevents cascading failures by short-circuiting
// calls to a failing dependency after a threshold of consecutive failures.
//
// The package is domain-agnostic and can be used for HTTP clients, subprocess
// launches, subprocess pool managers, or any "call a remote thing N times"
// pattern. It does not depend on the Velox HTTP API, master/worker topology,
// or logging conventions — callers wire in their own logger via the optional
// OnStateChange hook.
package resilience

import (
	"sync"
	"time"
)

// State represents the current circuit breaker state.
type State string

const (
	StateClosed   State = "closed"
	StateOpen     State = "open"
	StateHalfOpen State = "half-open"
)

// Config configures a CircuitBreaker.
type Config struct {
	// FailureThreshold is the number of consecutive failures required to
	// transition from closed → open.
	FailureThreshold int
	// SuccessThreshold is the number of consecutive successes required to
	// transition from half-open → closed.
	SuccessThreshold int
	// OpenTimeout is the duration to stay in the "open" state before allowing
	// a single probe request via half-open.
	OpenTimeout time.Duration
	// HalfOpenMaxCalls is the maximum number of concurrent calls permitted
	// while in half-open. Defaults to 1 if zero.
	HalfOpenMaxCalls int
	// OnStateChange, if non-nil, is invoked whenever the state changes.
	// Useful for wiring in structured logging without coupling this package
	// to a particular logger.
	OnStateChange func(prev, next State, failures int)
}

// CircuitBreaker implements the circuit breaker pattern.
//
// State transitions:
//
//	closed  --[N failures]-->                  open
//	open    --[OpenTimeout elapsed]------->    half-open
//	half-open --[M successes]----------->     closed
//	half-open --[any failure]----------->      open
type CircuitBreaker struct {
	mu               sync.RWMutex
	state            State
	failureCount     int
	successCount     int
	lastFailureTime  time.Time
	halfOpenInFlight int

	failureThreshold int
	successThreshold int
	openTimeout      time.Duration
	halfOpenMax      int
	hook             func(prev, next State, failures int)
}

// New creates a new CircuitBreaker. Passing zero values leaves defaults in
// place (FailureThreshold=5, SuccessThreshold=3, OpenTimeout=60s,
// HalfOpenMaxCalls=1).
func New(cfg Config) *CircuitBreaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.SuccessThreshold <= 0 {
		cfg.SuccessThreshold = 3
	}
	if cfg.OpenTimeout <= 0 {
		cfg.OpenTimeout = 60 * time.Second
	}
	if cfg.HalfOpenMaxCalls <= 0 {
		cfg.HalfOpenMaxCalls = 1
	}
	return &CircuitBreaker{
		state:            StateClosed,
		failureThreshold: cfg.FailureThreshold,
		successThreshold: cfg.SuccessThreshold,
		openTimeout:      cfg.OpenTimeout,
		halfOpenMax:      cfg.HalfOpenMaxCalls,
		hook:             cfg.OnStateChange,
	}
}

// CanExecute returns true if a request can proceed. In half-open state it
// admits at most halfOpenMax concurrent probes.
func (cb *CircuitBreaker) CanExecute() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		return true
	case StateOpen:
		if time.Since(cb.lastFailureTime) > cb.openTimeout {
			cb.transitionLocked(StateHalfOpen)
			cb.halfOpenInFlight = 1
			return true
		}
		return false
	case StateHalfOpen:
		if cb.halfOpenInFlight >= cb.halfOpenMax {
			return false
		}
		cb.halfOpenInFlight++
		return true
	default:
		return true
	}
}

// RecordSuccess registers a successful operation. The caller must have
// previously returned true from CanExecute.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		cb.failureCount = 0
	case StateHalfOpen:
		if cb.halfOpenInFlight > 0 {
			cb.halfOpenInFlight--
		}
		cb.successCount++
		if cb.successCount >= cb.successThreshold {
			cb.transitionLocked(StateClosed)
			cb.resetLocked()
		}
	}
}

// RecordFailure registers a failed operation.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.lastFailureTime = time.Now()

	switch cb.state {
	case StateClosed:
		cb.failureCount++
		if cb.failureCount >= cb.failureThreshold {
			cb.transitionLocked(StateOpen)
		}
	case StateHalfOpen:
		if cb.halfOpenInFlight > 0 {
			cb.halfOpenInFlight--
		}
		cb.transitionLocked(StateOpen)
		cb.successCount = 0
	}
}

// State returns the current state, useful for diagnostics and metrics.
func (cb *CircuitBreaker) State() State {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// GetState returns the current state as a string. Back-compat alias for the
// legacy pkg/api.CircuitBreaker.GetState signature.
func (cb *CircuitBreaker) GetState() string {
	return string(cb.State())
}

// FailureCount returns the current consecutive-failure counter. Useful for
// metrics emission.
func (cb *CircuitBreaker) FailureCount() int {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.failureCount
}

func (cb *CircuitBreaker) transitionLocked(next State) {
	prev := cb.state
	if prev == next {
		return
	}
	cb.state = next
	if cb.hook != nil {
		// Invoke outside the main critical section to avoid reentrant locking
		// if the hook tries to call back into the breaker.
		hook := cb.hook
		failures := cb.failureCount
		go hook(prev, next, failures)
	}
}

func (cb *CircuitBreaker) resetLocked() {
	cb.failureCount = 0
	cb.successCount = 0
	cb.halfOpenInFlight = 0
}

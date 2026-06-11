// Package api provides HTTP client for communicating with the Velox Master server.
package api

import (
	"sync"
	"time"

	"velox-worker-agent/pkg/logger"
)

// Circuit breaker states
const (
	CircuitClosed   = "closed"
	CircuitOpen     = "open"
	CircuitHalfOpen = "half-open"
)

// CircuitBreaker implements the circuit breaker pattern to prevent cascading failures.
type CircuitBreaker struct {
	mu               sync.RWMutex
	state            string
	failureCount     int
	successCount     int
	lastFailureTime  time.Time
	failureThreshold int
	successThreshold int
	timeout          time.Duration
	halfOpenMax      int
}

// NewCircuitBreaker creates a new circuit breaker.
func NewCircuitBreaker(failureThreshold, successThreshold int, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:            CircuitClosed,
		failureThreshold: failureThreshold,
		successThreshold: successThreshold,
		timeout:          timeout,
		halfOpenMax:      3,
	}
}

// CanExecute checks if a request can be executed.
func (cb *CircuitBreaker) CanExecute() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	switch cb.state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		if time.Since(cb.lastFailureTime) > cb.timeout {
			return true
		}
		return false
	case CircuitHalfOpen:
		return true
	default:
		return true
	}
}

// RecordSuccess records a successful request.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		cb.failureCount = 0
	case CircuitHalfOpen:
		cb.successCount++
		if cb.successCount >= cb.successThreshold {
			cb.state = CircuitClosed
			cb.failureCount = 0
			cb.successCount = 0
			logger.Info("[CIRCUIT_BREAKER] Circuit closed - service recovered")
		}
	}
}

// RecordFailure records a failed request.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.lastFailureTime = time.Now()

	switch cb.state {
	case CircuitClosed:
		cb.failureCount++
		if cb.failureCount >= cb.failureThreshold {
			cb.state = CircuitOpen
			logger.Warn("[CIRCUIT_BREAKER] Circuit opened - too many failures")
		}
	case CircuitHalfOpen:
		cb.state = CircuitOpen
		cb.successCount = 0
		logger.Warn("[CIRCUIT_BREAKER] Circuit reopened - test request failed")
	}
}

// GetState returns the current circuit breaker state.
func (cb *CircuitBreaker) GetState() string {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

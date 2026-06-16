// Package api: this file is a thin backwards-compatibility shim around the
// reusable pkg/resilience.CircuitBreaker. The CRUD lives in pkg/resilience;
// the legacy constants/types are preserved here as aliases so existing
// imports of velox-worker-agent/pkg/api continue to compile during the
// deprecation window. New code should import pkg/resilience directly.
package api

import (
	"time"

	"velox-worker-agent/pkg/resilience"
)

// Backwards-compat constants for callers that referenced the old
// CircuitClosed/CircuitOpen/CircuitHalfOpen string constants.
const (
	CircuitClosed   = resilience.StateClosed
	CircuitOpen     = resilience.StateOpen
	CircuitHalfOpen = resilience.StateHalfOpen
)

// CircuitBreaker is an alias for resilience.CircuitBreaker so existing
// references (e.g. Client.circuitBreaker) keep working.
type CircuitBreaker = resilience.CircuitBreaker

// NewCircuitBreaker creates a new circuit breaker with the legacy
// (failureThreshold, successThreshold, timeout) signature. Internally it
// builds a resilience.Config and delegates to resilience.New.
func NewCircuitBreaker(failureThreshold, successThreshold int, timeout time.Duration) *CircuitBreaker {
	return resilience.New(resilience.Config{
		FailureThreshold: failureThreshold,
		SuccessThreshold: successThreshold,
		OpenTimeout:      timeout,
		HalfOpenMaxCalls: 3, // preserve the original hard-coded halfOpenMax behaviour
	})
}

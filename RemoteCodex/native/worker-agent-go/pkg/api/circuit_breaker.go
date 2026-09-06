// Package api: this file is a thin alias shim around the reusable
// pkg/resilience.CircuitBreaker. The state machine lives in pkg/resilience;
// this file keeps the call-site-shaped constructor + type alias used by
// client.go. New code should import pkg/resilience directly.
//
// A1-3 audit triage: the three state-string constants (CircuitClosed /
// CircuitOpen / CircuitHalfOpen) had zero references and were removed.
// The type alias and NewCircuitBreaker have live callers and stay.
package api

import (
	"time"

	"velox-worker-agent/pkg/resilience"
)

// CircuitBreaker is an alias for resilience.CircuitBreaker so existing
// references (e.g. Client.circuitBreaker) keep working.
type CircuitBreaker = resilience.CircuitBreaker

// NewCircuitBreaker creates a circuit breaker with the
// (failureThreshold, successThreshold, timeout) signature used by the
// client options. Internally it builds a resilience.Config and delegates
// to resilience.New.
func NewCircuitBreaker(failureThreshold, successThreshold int, timeout time.Duration) *CircuitBreaker {
	return resilience.New(resilience.Config{
		FailureThreshold: failureThreshold,
		SuccessThreshold: successThreshold,
		OpenTimeout:      timeout,
		HalfOpenMaxCalls: 3, // preserve the original hard-coded halfOpenMax behaviour
	})
}

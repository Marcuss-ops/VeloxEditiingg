// Package logger: rate-limiting helpers around the reusable obs.RateLimiter.
//
// The struct, milestones, and Reset logic live in pkg/obs so other
// Velox components can share the same primitives.
package logger

import "velox-worker-agent/pkg/obs"

// RateLimiter is an alias for obs.RateLimiter.
type RateLimiter = obs.RateLimiter

// globalRateLimiter is the package-wide rate limiter shared by all worker
// log helpers. It is intentionally a single instance to prevent the
// milestone counters from fragmenting across handlers.
var globalRateLimiter = obs.GlobalRateLimiter()

// GlobalRateLimiter exposes the shared rate limiter. New callers wanting
// isolated counters should construct a dedicated limiter via obs.NewRateLimiter.
func GlobalRateLimiter() *RateLimiter {
	return obs.GlobalRateLimiter()
}

// Package obs provides observability primitives reusable across Velox
// components (master server, worker agent, integrations, CLI tools).
//
// The package is structured to be domain-agnostic and is intentionally
// hosted in velox-shared so consumers do not need to depend on each
// other's internal packages. It exposes:
//
//   - Event / EventCode: typed structured logging events
//   - RateLimiter: a milestone-based rate limiter for noisy repeated events
//
// Code that wants to emit events can either import this package directly,
// or wrap these primitives into its own helper functions.
package obs

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// EventCode is a typed identifier for a logical event ("JOB_STARTED",
// "API_RATE_LIMITED", etc). Callers are expected to define their own
// const values — see examples in the worker agent's logger package.
type EventCode string

// Cross-component EventCode constants. These describe transport-agnostic
// events emitted by the HTTP / API layer and are intentionally defined here
// (rather than in a worker- or master-specific package) so that any Velox
// component — master, worker, integration, CLI tool — can reference them
// without taking on a dependency on a different component's event package.
const (
	// EventAPIRetry is emitted when an HTTP call is retried after a
	// transient failure, with the attempt and backoff in the payload.
	EventAPIRetry EventCode = "API_RETRY"
	// EventAPISuccess is emitted when an HTTP call ultimately succeeds,
	// optionally including the number of retries it took.
	EventAPISuccess EventCode = "API_SUCCESS"
	// EventAPIError is emitted when an HTTP call fails after exhausting
	// retries (or when a non-retryable error is encountered).
	EventAPIError EventCode = "API_ERROR"
)

// Event represents a single structured event entry. JSON shape is stable and
// akin to logfmt:
//
//	{
//	  "event":     "JOB_STARTED",
//	  "timestamp": "2026-06-14T12:34:56Z",
//	  "fields":    { "job_id": "...", "worker_id": "..." }
//	}
type Event struct {
	Name      EventCode              `json:"event"`
	Timestamp string                 `json:"timestamp"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
}

// NewEvent creates an event with the supplied code, timestamped in UTC RFC3339.
func NewEvent(code EventCode) *Event {
	return &Event{
		Name:      code,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Fields:    make(map[string]interface{}),
	}
}

// WithField attaches a key-value pair to the event and returns the event for
// chained construction.
func (e *Event) WithField(key string, value interface{}) *Event {
	if e.Fields == nil {
		e.Fields = make(map[string]interface{})
	}
	e.Fields[key] = value
	return e
}

// WithError attaches an error to the "error" field (if non-nil).
func (e *Event) WithError(err error) *Event {
	if err == nil {
		return e
	}
	return e.WithField("error", err.Error())
}

// WithDuration attaches an elapsed time as "duration_ms".
func (e *Event) WithDuration(d time.Duration) *Event {
	return e.WithField("duration_ms", d.Milliseconds())
}

// String returns the JSON serialization. Falls back to a static envelope on
// marshal failure so logging never panics.
func (e *Event) String() string {
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Sprintf(`{"event":"%s","error":"marshal failed"}`, e.Name)
	}
	return string(data)
}

// RateLimiter implements a milestone-based rate limiter suitable for noisy,
// repetitive events like repeated heartbeat failures, retries, or churn.
//
// Milestones (configurable via constructor): the limiter emits the event on
// hit #1, #5, #10, #50, #100, #500, #1000, then every 1000th. Between
// milestones the event is dropped.
//
// The limiter is safe for concurrent use.
type RateLimiter struct {
	mu        sync.Mutex
	counters  map[string]int
	milestone []int
}

// NewRateLimiter returns a fresh RateLimiter. If no milestones are supplied,
// the default Velox-friendly set is used: 1, 5, 10, 50, 100, 500, 1000,
// then every 1000.
func NewRateLimiter(milestones ...int) *RateLimiter {
	if len(milestones) == 0 {
		milestones = []int{1, 5, 10, 50, 100, 500, 1000, 1000}
	}
	return &RateLimiter{
		counters:  make(map[string]int),
		milestone: milestones,
	}
}

// globalRateLimiter is a process-wide default. Shared between every Velox
// component importing this package; components that need isolated counters
// should construct their own RateLimiter via NewRateLimiter.
var globalRateLimiter = NewRateLimiter()

// GlobalRateLimiter returns the process-wide default RateLimiter.
func GlobalRateLimiter() *RateLimiter {
	return globalRateLimiter
}

// ShouldLog increments the counter for eventKey and returns whether the event
// passes the milestone filter, along with the current count (useful for
// embedding in the event payload).
func (r *RateLimiter) ShouldLog(eventKey string) (bool, int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.counters[eventKey]++
	count := r.counters[eventKey]

	for _, step := range r.milestone {
		if count == step {
			return true, count
		}
	}
	// Final milestone is an "every-N" step.
	if n := r.milestone[len(r.milestone)-1]; n > 0 && count > n && count%n == 0 {
		return true, count
	}
	return false, count
}

// Reset zeroes the counters for the supplied key (or all keys if empty).
func (r *RateLimiter) Reset(key ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(key) == 0 {
		r.counters = make(map[string]int)
		return
	}
	for _, k := range key {
		delete(r.counters, k)
	}
}

// Package obs: backward-compatible re-export of velox-shared/obs for the
// worker agent. The canonical implementation lives in velox-shared so any
// Velox component (master, worker, integrations, CLI) can import it
// without depending on this module. Callers inside the worker agent may
// keep importing velox-worker-agent/pkg/obs for now — every symbol here is
// a type alias or a one-line wrapper.
//
// New code outside the worker agent should import velox-shared/obs
// directly.
package obs

import sharedobs "velox-shared/obs"

// Type aliases preserve identity, so methods defined on
// *sharedobs.Event / *sharedobs.RateLimiter remain callable through these
// aliases.
type (
	EventCode   = sharedobs.EventCode
	Event       = sharedobs.Event
	RateLimiter = sharedobs.RateLimiter
)

// Constants cross-component with the Velox API transport layer.
const (
	EventAPIRetry   = sharedobs.EventAPIRetry
	EventAPISuccess = sharedobs.EventAPISuccess
	EventAPIError   = sharedobs.EventAPIError
)

// Constructor / accessor wrappers.
func NewEvent(code EventCode) *Event {
	return sharedobs.NewEvent(code)
}

func NewRateLimiter(milestones ...int) *RateLimiter {
	return sharedobs.NewRateLimiter(milestones...)
}

func GlobalRateLimiter() *RateLimiter {
	return sharedobs.GlobalRateLimiter()
}

// Package logger: structured event emission for the Velox worker agent.
//
// The generic event and rate-limiter primitives live in pkg/obs (a
// reusable, domain-agnostic package). This file only declares the
// Worker-domain EventCode constants. Code that wants to emit events calls
// obs.NewEvent(...).WithField(...) directly.
package logger

import "velox-worker-agent/pkg/obs"

// EventCode is the worker-domain typed event identifier. The alias keeps a
// stable reference for callers that previously used logger.EventCode.
type EventCode = obs.EventCode

// Event is an alias for obs.Event so helper code can keep using
// logger.NewEvent(...).
type Event = obs.Event

// NewEvent delegates to obs.NewEvent, stamping the event in UTC RFC3339.
//
// Worker-domain callers should chain calls to obs.Event.WithField to attach
// payloads (worker_id, job_id, etc.).
func NewEvent(code EventCode) *Event {
	return obs.NewEvent(code)
}

// EventCode constants. These describe Velox worker-agent specific events.
// Generic cross-component codes (e.g. master/worker agnostic transports)
// belong in pkg/obs or in the calling component.
const (
	EventStartup         EventCode = "STARTUP"
	EventConfigLoaded    EventCode = "CONFIG_LOADED"
	EventConfigInvalid   EventCode = "CONFIG_INVALID"
	EventRegisterSuccess EventCode = "REGISTER_SUCCESS"
	EventRegisterFailed  EventCode = "REGISTER_FAILED"
	EventUnregister      EventCode = "UNREGISTER"

	EventHeartbeatSuccess EventCode = "HEARTBEAT_SUCCESS"
	EventHeartbeatFailed  EventCode = "HEARTBEAT_FAILED"

	EventJobClaimed   EventCode = "JOB_CLAIMED"
	EventJobStarted   EventCode = "JOB_STARTED"
	EventJobCompleted EventCode = "JOB_COMPLETED"
	EventJobFailed    EventCode = "JOB_FAILED"
	EventJobTimeout   EventCode = "JOB_TIMEOUT"

	EventMasterReachable   EventCode = "MASTER_REACHABLE"
	EventMasterUnreachable EventCode = "MASTER_URL_UNREACHABLE"

	// Note: cross-component HTTP transport codes (EventAPIRetry/EventAPISuccess/
	// EventAPIError) live in pkg/obs/event.go so any Velox component can
	// reuse them without depending on pkg/logger.
)

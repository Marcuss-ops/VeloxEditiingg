// Package logger provides structured event logging for the Velox Worker Agent.
package logger

import (
	"encoding/json"
	"fmt"
	"time"
)

// EventCode represents a structured event code.
type EventCode string

const (
	// Lifecycle events
	EventStartup       EventCode = "STARTUP"
	EventConfigLoaded  EventCode = "CONFIG_LOADED"
	EventConfigInvalid EventCode = "CONFIG_INVALID"

	// Registration events
	EventRegisterSuccess EventCode = "REGISTER_SUCCESS"
	EventRegisterFailed  EventCode = "REGISTER_FAILED"
	EventUnregister      EventCode = "UNREGISTER"

	// Heartbeat events (rate-limited)
	EventHeartbeatSuccess EventCode = "HEARTBEAT_SUCCESS"
	EventHeartbeatFailed  EventCode = "HEARTBEAT_FAILED"

	// Job events
	EventJobClaimed   EventCode = "JOB_CLAIMED"
	EventJobStarted   EventCode = "JOB_STARTED"
	EventJobCompleted EventCode = "JOB_COMPLETED"
	EventJobFailed    EventCode = "JOB_FAILED"
	EventJobTimeout   EventCode = "JOB_TIMEOUT"

	// Master communication events (rate-limited)
	EventMasterReachable   EventCode = "MASTER_REACHABLE"
	EventMasterUnreachable EventCode = "MASTER_URL_UNREACHABLE"
)

// Event represents a structured log event.
type Event struct {
	Name      EventCode              `json:"event"`
	Timestamp string                 `json:"timestamp"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
}

// NewEvent creates a new event with the given code.
func NewEvent(code EventCode) *Event {
	return &Event{
		Name:      code,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Fields:    make(map[string]interface{}),
	}
}

// Builder methods

func (e *Event) WorkerID(id string) *Event           { e.Fields["worker_id"] = id; return e }
func (e *Event) Version(v string) *Event              { e.Fields["version"] = v; return e }
func (e *Event) MasterURL(url string) *Event          { e.Fields["master_url"] = url; return e }
func (e *Event) JobID(id string) *Event               { e.Fields["job_id"] = id; return e }
func (e *Event) JobType(t string) *Event              { e.Fields["job_type"] = t; return e }
func (e *Event) Duration(d time.Duration) *Event      { e.Fields["duration_ms"] = d.Milliseconds(); return e }
func (e *Event) Attempt(a int) *Event                 { e.Fields["attempt"] = a; return e }
func (e *Event) MaxAttempts(m int) *Event             { e.Fields["max_attempts"] = m; return e }
func (e *Event) Priority(p int) *Event                { e.Fields["priority"] = p; return e }
func (e *Event) Status(s string) *Event               { e.Fields["status"] = s; return e }
func (e *Event) Reason(reason string) *Event          { e.Fields["reason"] = reason; return e }
func (e *Event) Count(c int) *Event                   { e.Fields["count"] = c; return e }

func (e *Event) Error(err error) *Event {
	if err != nil {
		e.Fields["error"] = err.Error()
	}
	return e
}

func (e *Event) WithField(key string, value interface{}) *Event {
	e.Fields[key] = value
	return e
}

// String returns the JSON representation of the event.
func (e *Event) String() string {
	jsonBytes, err := json.Marshal(e)
	if err != nil {
		return fmt.Sprintf(`{"event":"%s","error":"marshal failed"}`, e.Name)
	}
	return string(jsonBytes)
}

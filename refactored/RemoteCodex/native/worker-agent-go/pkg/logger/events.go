// Package logger provides structured event logging for the Velox Worker Agent.
// This file implements the event codes and structured logging from 11_LOGGING_OPERATIVO_SENZA_RUMORE.md
package logger

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// ============================================================================
// Event Codes (from 11_LOGGING_OPERATIVO_SENZA_RUMORE.md)
// ============================================================================

// EventCode represents a structured event code
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

// Event represents a structured log event
type Event struct {
	Name      EventCode              `json:"event"`
	Timestamp string                 `json:"timestamp"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
}

// NewEvent creates a new event with the given code
func NewEvent(code EventCode) *Event {
	return &Event{
		Name:      code,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Fields:    make(map[string]interface{}),
	}
}

// WorkerID sets the worker_id field
func (e *Event) WorkerID(id string) *Event {
	e.Fields["worker_id"] = id
	return e
}

// Version sets the version field
func (e *Event) Version(v string) *Event {
	e.Fields["version"] = v
	return e
}

// MasterURL sets the master_url field
func (e *Event) MasterURL(url string) *Event {
	e.Fields["master_url"] = url
	return e
}

// JobID sets the job_id field
func (e *Event) JobID(id string) *Event {
	e.Fields["job_id"] = id
	return e
}

// JobType sets the job_type field
func (e *Event) JobType(t string) *Event {
	e.Fields["job_type"] = t
	return e
}

// Duration sets the duration_ms field
func (e *Event) Duration(d time.Duration) *Event {
	e.Fields["duration_ms"] = d.Milliseconds()
	return e
}

// Attempt sets the attempt field
func (e *Event) Attempt(a int) *Event {
	e.Fields["attempt"] = a
	return e
}

// MaxAttempts sets the max_attempts field
func (e *Event) MaxAttempts(m int) *Event {
	e.Fields["max_attempts"] = m
	return e
}

// Priority sets the priority field
func (e *Event) Priority(p int) *Event {
	e.Fields["priority"] = p
	return e
}

// Status sets the status field
func (e *Event) Status(s string) *Event {
	e.Fields["status"] = s
	return e
}

// Error sets the error field
func (e *Event) Error(err error) *Event {
	if err != nil {
		e.Fields["error"] = err.Error()
	}
	return e
}

// Count sets the count field (for rate-limited events)
func (e *Event) Count(c int) *Event {
	e.Fields["count"] = c
	return e
}

// String returns the JSON representation of the event
func (e *Event) String() string {
	jsonBytes, err := json.Marshal(e)
	if err != nil {
		return fmt.Sprintf(`{"event":"%s","error":"marshal failed"}`, e.Name)
	}
	return string(jsonBytes)
}

// ============================================================================
// Rate Limiter (from 11_LOGGING_OPERATIVO_SENZA_RUMORE.md)
// ============================================================================

// RateLimiter implements rate limiting for log events
type RateLimiter struct {
	mu       sync.Mutex
	counters map[string]int
}

var globalRateLimiter = &RateLimiter{
	counters: make(map[string]int),
}

// GlobalRateLimiter returns the global rate limiter
func GlobalRateLimiter() *RateLimiter {
	return globalRateLimiter
}

// Reset resets all counters
func (r *RateLimiter) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counters = make(map[string]int)
}

// ShouldLog returns true if the event should be logged based on rate limiting rules.
// Returns (shouldLog, currentCount)
// Milestones: 1, 5, 10, 50, 100, 500, 1000, then every 1000
func (r *RateLimiter) ShouldLog(eventKey string) (bool, int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.counters[eventKey]++
	count := r.counters[eventKey]

	// Milestone logging: 1, 5, 10, 50, 100, 500, 1000, 2000, 3000, ...
	switch {
	case count == 1:
		return true, count
	case count == 5:
		return true, count
	case count == 10:
		return true, count
	case count == 50:
		return true, count
	case count == 100:
		return true, count
	case count == 500:
		return true, count
	case count >= 1000 && count%1000 == 0:
		return true, count
	default:
		return false, count
	}
}

// ============================================================================
// Convenience Functions for Common Events
// ============================================================================

// LogStartup logs a STARTUP event
func LogStartup(workerID, version, masterURL string) {
	event := NewEvent(EventStartup).
		WorkerID(workerID).
		Version(version).
		MasterURL(masterURL)
	Info("[%s] %s", event.Name, event.String())
}

// LogRegisterSuccess logs a REGISTER_SUCCESS event
func LogRegisterSuccess(workerID, masterURL string) {
	event := NewEvent(EventRegisterSuccess).
		WorkerID(workerID).
		MasterURL(masterURL)
	Info("[%s] %s", event.Name, event.String())
}

// LogRegisterFailed logs a REGISTER_FAILED event (rate-limited)
func LogRegisterFailed(workerID, masterURL string, err error) {
	eventKey := fmt.Sprintf("register_failed:%s", workerID)
	shouldLog, count := globalRateLimiter.ShouldLog(eventKey)

	if shouldLog {
		event := NewEvent(EventRegisterFailed).
			WorkerID(workerID).
			MasterURL(masterURL).
			Error(err).
			Count(count)
		Error("[%s] %s", event.Name, event.String())
	}
}

// LogHeartbeatSuccess logs a HEARTBEAT_SUCCESS event (DEBUG level)
func LogHeartbeatSuccess(workerID, status string) {
	event := NewEvent(EventHeartbeatSuccess).
		WorkerID(workerID).
		Status(status)
	Debug("[%s] %s", event.Name, event.String())
}

// LogHeartbeatFailed logs a HEARTBEAT_FAILED event (rate-limited)
func LogHeartbeatFailed(workerID string, err error, attempt, maxAttempts int) {
	eventKey := fmt.Sprintf("heartbeat_failed:%s", workerID)
	shouldLog, count := globalRateLimiter.ShouldLog(eventKey)

	if shouldLog {
		event := NewEvent(EventHeartbeatFailed).
			WorkerID(workerID).
			Error(err).
			Attempt(attempt).
			MaxAttempts(maxAttempts).
			Count(count)
		Warn("[%s] %s", event.Name, event.String())
	}
}

// LogJobClaimed logs a JOB_CLAIMED event
func LogJobClaimed(workerID, jobID, jobType string, priority int) {
	event := NewEvent(EventJobClaimed).
		WorkerID(workerID).
		JobID(jobID).
		JobType(jobType).
		Priority(priority)
	Info("[%s] %s", event.Name, event.String())
}

// LogJobStarted logs a JOB_STARTED event
func LogJobStarted(workerID, jobID, jobType string) {
	event := NewEvent(EventJobStarted).
		WorkerID(workerID).
		JobID(jobID).
		JobType(jobType)
	Info("[%s] %s", event.Name, event.String())
}

// LogJobCompleted logs a JOB_COMPLETED event
func LogJobCompleted(workerID, jobID string, duration time.Duration) {
	event := NewEvent(EventJobCompleted).
		WorkerID(workerID).
		JobID(jobID).
		Duration(duration)
	Info("[%s] %s", event.Name, event.String())
}

// LogJobFailed logs a JOB_FAILED event
func LogJobFailed(workerID, jobID string, err error, duration time.Duration) {
	event := NewEvent(EventJobFailed).
		WorkerID(workerID).
		JobID(jobID).
		Error(err).
		Duration(duration)
	Error("[%s] %s", event.Name, event.String())
}

// LogMasterReachable logs a MASTER_REACHABLE event
func LogMasterReachable(workerID, masterURL string) {
	event := NewEvent(EventMasterReachable).
		WorkerID(workerID).
		MasterURL(masterURL)
	Debug("[%s] %s", event.Name, event.String())
}

// LogMasterUnreachableRateLimited logs a MASTER_URL_UNREACHABLE event (rate-limited)
func LogMasterUnreachableRateLimited(workerID, masterURL string, err error) {
	eventKey := fmt.Sprintf("master_unreachable:%s:%s", workerID, masterURL)
	shouldLog, count := globalRateLimiter.ShouldLog(eventKey)

	if shouldLog {
		event := NewEvent(EventMasterUnreachable).
			WorkerID(workerID).
			MasterURL(masterURL).
			Error(err).
			Count(count)
		Warn("[%s] %s", event.Name, event.String())
	}
}

// Compatibility wrappers for older call sites.
func LogConfigCreated(configPath, workerID string) {
	Info("[CONFIG_CREATED] worker_id=%s path=%s", workerID, configPath)
}

func LogConfigLoaded(configPath, workerID string) {
	Info("[CONFIG_LOADED] worker_id=%s path=%s", workerID, configPath)
}

func LogConfigError(err error) {
	if err == nil {
		Error("[CONFIG_ERROR] unknown")
		return
	}
	Error("[CONFIG_ERROR] %v", err)
}

func LogSignalReceived(workerID, signalName string) {
	Info("[SIGNAL] worker_id=%s signal=%s", workerID, signalName)
}

func LogHeartbeatRecover(workerID string, _ int) {
	LogMasterReachable(workerID, "")
}

// Compatibility wrapper: older code calls LogJobStart.
func LogJobStart(workerID, jobID, jobType string, _ int) {
	LogJobStarted(workerID, jobID, jobType)
}

// Compatibility wrapper: older code calls LogJobSuccess.
func LogJobSuccess(workerID, jobID, _ string, duration time.Duration) {
	LogJobCompleted(workerID, jobID, duration)
}

// Compatibility wrapper: older code calls LogJobFailedWithType.
func LogJobFailedWithType(workerID, jobID, _ string, err error, duration time.Duration) {
	LogJobFailed(workerID, jobID, err, duration)
}

// Reason sets the reason field on an Event
func (e *Event) Reason(reason string) *Event {
	e.Fields["reason"] = reason
	return e
}

// WithField sets an arbitrary field on an Event
func (e *Event) WithField(key string, value interface{}) *Event {
	e.Fields[key] = value
	return e
}



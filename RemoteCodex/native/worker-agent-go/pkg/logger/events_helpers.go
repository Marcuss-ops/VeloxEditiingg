// Package logger: structured event-emission helpers for the worker agent.
//
// Each helper constructs an obs.Event, attaches the relevant fields, and
// forwards it to the package's leveled log functions. The rate-limited
// helpers record the current count via obs.RateLimiter so very noisy
// failures (continuous heartbeat retries, repeated register failures) do
// not flood the logs.
package logger

import (
	"fmt"
	"time"

	obs "velox-shared/obs"
)

// withEvent constructs an event with the supplied code and attaches every
// supplied key/value pair via obs.Event.WithField. Returning the *Event
// (rather than just the JSON string) lets callers tweak it further if
// needed before emission.
func withEvent(code EventCode, kvs ...interface{}) *obs.Event {
	e := obs.NewEvent(code)
	for i := 0; i+1 < len(kvs); i += 2 {
		key, _ := kvs[i].(string)
		if key == "" {
			continue
		}
		e.WithField(key, kvs[i+1])
	}
	return e
}

// LogStartup logs a STARTUP event.
func LogStartup(workerID, version, masterURL string) {
	e := obs.NewEvent(EventStartup).
		WithField("worker_id", workerID).
		WithField("version", version).
		WithField("master_url", masterURL)
	Info("[%s] %s", e.Name, e.String())
}

// LogRegisterSuccess logs a REGISTER_SUCCESS event.
func LogRegisterSuccess(workerID, masterURL string) {
	e := obs.NewEvent(EventRegisterSuccess).
		WithField("worker_id", workerID).
		WithField("master_url", masterURL)
	Info("[%s] %s", e.Name, e.String())
}

// LogRegisterFailed logs a REGISTER_FAILED event (rate-limited).
func LogRegisterFailed(workerID, masterURL string, err error) {
	eventKey := fmt.Sprintf("register_failed:%s", workerID)
	if emit, count := globalRateLimiter.ShouldLog(eventKey); emit {
		e := obs.NewEvent(EventRegisterFailed).
			WithField("worker_id", workerID).
			WithField("master_url", masterURL).
			WithError(err).
			WithField("count", count)
		Error("[%s] %s", e.Name, e.String())
	}
}

// LogHeartbeatSuccess logs a HEARTBEAT_SUCCESS event (DEBUG level).
func LogHeartbeatSuccess(workerID, status string) {
	e := obs.NewEvent(EventHeartbeatSuccess).
		WithField("worker_id", workerID).
		WithField("status", status)
	Debug("[%s] %s", e.Name, e.String())
}

// LogHeartbeatFailed logs a HEARTBEAT_FAILED event (rate-limited).
func LogHeartbeatFailed(workerID string, err error, attempt, maxAttempts int) {
	eventKey := fmt.Sprintf("heartbeat_failed:%s", workerID)
	if emit, count := globalRateLimiter.ShouldLog(eventKey); emit {
		e := obs.NewEvent(EventHeartbeatFailed).
			WithField("worker_id", workerID).
			WithError(err).
			WithField("attempt", attempt).
			WithField("max_attempts", maxAttempts).
			WithField("count", count)
		Warn("[%s] %s", e.Name, e.String())
	}
}

// LogJobClaimed logs a JOB_CLAIMED event.
func LogJobClaimed(workerID, jobID, jobType string, priority int) {
	e := obs.NewEvent(EventJobClaimed).
		WithField("worker_id", workerID).
		WithField("job_id", jobID).
		WithField("job_type", jobType).
		WithField("priority", priority)
	Info("[%s] %s", e.Name, e.String())
}

// LogJobStarted logs a JOB_STARTED event.
func LogJobStarted(workerID, jobID, jobType string) {
	e := obs.NewEvent(EventJobStarted).
		WithField("worker_id", workerID).
		WithField("job_id", jobID).
		WithField("job_type", jobType)
	Info("[%s] %s", e.Name, e.String())
}

// LogJobCompleted logs a JOB_COMPLETED event.
func LogJobCompleted(workerID, jobID string, duration time.Duration) {
	e := obs.NewEvent(EventJobCompleted).
		WithField("worker_id", workerID).
		WithField("job_id", jobID).
		WithDuration(duration)
	Info("[%s] %s", e.Name, e.String())
}

// LogJobCancelled logs an operator/request cancellation separately from a
// worker or renderer failure.
func LogJobCancelled(workerID, jobID string, duration time.Duration) {
	e := obs.NewEvent(EventJobCancelled).
		WithField("worker_id", workerID).
		WithField("job_id", jobID).
		WithDuration(duration)
	Info("[%s] %s", e.Name, e.String())
}

// LogJobFailed logs a JOB_FAILED event.
func LogJobFailed(workerID, jobID string, err error, duration time.Duration) {
	e := obs.NewEvent(EventJobFailed).
		WithField("worker_id", workerID).
		WithField("job_id", jobID).
		WithError(err).
		WithDuration(duration)
	Error("[%s] %s", e.Name, e.String())
}

// LogMasterReachable logs a MASTER_REACHABLE event.
func LogMasterReachable(workerID, masterURL string) {
	e := obs.NewEvent(EventMasterReachable).
		WithField("worker_id", workerID).
		WithField("master_url", masterURL)
	Debug("[%s] %s", e.Name, e.String())
}

// LogMasterUnreachableRateLimited logs a MASTER_URL_UNREACHABLE event (rate-limited).
func LogMasterUnreachableRateLimited(workerID, masterURL string, err error) {
	eventKey := fmt.Sprintf("master_unreachable:%s:%s", workerID, masterURL)
	if emit, count := globalRateLimiter.ShouldLog(eventKey); emit {
		e := obs.NewEvent(EventMasterUnreachable).
			WithField("worker_id", workerID).
			WithField("master_url", masterURL).
			WithError(err).
			WithField("count", count)
		Warn("[%s] %s", e.Name, e.String())
	}
}

// Compatibility wrappers for older call sites. They are kept as small,
// forward-only facades so legacy code paths still compile.
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

func LogJobStart(workerID, jobID, jobType string, _ int) {
	LogJobStarted(workerID, jobID, jobType)
}

func LogJobSuccess(workerID, jobID, _ string, duration time.Duration) {
	LogJobCompleted(workerID, jobID, duration)
}

func LogJobFailedWithType(workerID, jobID, _ string, err error, duration time.Duration) {
	LogJobFailed(workerID, jobID, err, duration)
}

// Package logger provides structured event logging for the Velox Worker Agent.
package logger

import (
	"fmt"
	"time"
)

// LogStartup logs a STARTUP event.
func LogStartup(workerID, version, masterURL string) {
	event := NewEvent(EventStartup).WorkerID(workerID).Version(version).MasterURL(masterURL)
	Info("[%s] %s", event.Name, event.String())
}

// LogRegisterSuccess logs a REGISTER_SUCCESS event.
func LogRegisterSuccess(workerID, masterURL string) {
	event := NewEvent(EventRegisterSuccess).WorkerID(workerID).MasterURL(masterURL)
	Info("[%s] %s", event.Name, event.String())
}

// LogRegisterFailed logs a REGISTER_FAILED event (rate-limited).
func LogRegisterFailed(workerID, masterURL string, err error) {
	eventKey := fmt.Sprintf("register_failed:%s", workerID)
	shouldLog, count := globalRateLimiter.ShouldLog(eventKey)
	if shouldLog {
		event := NewEvent(EventRegisterFailed).WorkerID(workerID).MasterURL(masterURL).Error(err).Count(count)
		Error("[%s] %s", event.Name, event.String())
	}
}

// LogHeartbeatSuccess logs a HEARTBEAT_SUCCESS event (DEBUG level).
func LogHeartbeatSuccess(workerID, status string) {
	event := NewEvent(EventHeartbeatSuccess).WorkerID(workerID).Status(status)
	Debug("[%s] %s", event.Name, event.String())
}

// LogHeartbeatFailed logs a HEARTBEAT_FAILED event (rate-limited).
func LogHeartbeatFailed(workerID string, err error, attempt, maxAttempts int) {
	eventKey := fmt.Sprintf("heartbeat_failed:%s", workerID)
	shouldLog, count := globalRateLimiter.ShouldLog(eventKey)
	if shouldLog {
		event := NewEvent(EventHeartbeatFailed).WorkerID(workerID).Error(err).Attempt(attempt).MaxAttempts(maxAttempts).Count(count)
		Warn("[%s] %s", event.Name, event.String())
	}
}

// LogJobClaimed logs a JOB_CLAIMED event.
func LogJobClaimed(workerID, jobID, jobType string, priority int) {
	event := NewEvent(EventJobClaimed).WorkerID(workerID).JobID(jobID).JobType(jobType).Priority(priority)
	Info("[%s] %s", event.Name, event.String())
}

// LogJobStarted logs a JOB_STARTED event.
func LogJobStarted(workerID, jobID, jobType string) {
	event := NewEvent(EventJobStarted).WorkerID(workerID).JobID(jobID).JobType(jobType)
	Info("[%s] %s", event.Name, event.String())
}

// LogJobCompleted logs a JOB_COMPLETED event.
func LogJobCompleted(workerID, jobID string, duration time.Duration) {
	event := NewEvent(EventJobCompleted).WorkerID(workerID).JobID(jobID).Duration(duration)
	Info("[%s] %s", event.Name, event.String())
}

// LogJobFailed logs a JOB_FAILED event.
func LogJobFailed(workerID, jobID string, err error, duration time.Duration) {
	event := NewEvent(EventJobFailed).WorkerID(workerID).JobID(jobID).Error(err).Duration(duration)
	Error("[%s] %s", event.Name, event.String())
}

// LogMasterReachable logs a MASTER_REACHABLE event.
func LogMasterReachable(workerID, masterURL string) {
	event := NewEvent(EventMasterReachable).WorkerID(workerID).MasterURL(masterURL)
	Debug("[%s] %s", event.Name, event.String())
}

// LogMasterUnreachableRateLimited logs a MASTER_URL_UNREACHABLE event (rate-limited).
func LogMasterUnreachableRateLimited(workerID, masterURL string, err error) {
	eventKey := fmt.Sprintf("master_unreachable:%s:%s", workerID, masterURL)
	shouldLog, count := globalRateLimiter.ShouldLog(eventKey)
	if shouldLog {
		event := NewEvent(EventMasterUnreachable).WorkerID(workerID).MasterURL(masterURL).Error(err).Count(count)
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

func LogJobStart(workerID, jobID, jobType string, _ int) {
	LogJobStarted(workerID, jobID, jobType)
}

func LogJobSuccess(workerID, jobID, _ string, duration time.Duration) {
	LogJobCompleted(workerID, jobID, duration)
}

func LogJobFailedWithType(workerID, jobID, _ string, err error, duration time.Duration) {
	LogJobFailed(workerID, jobID, err, duration)
}

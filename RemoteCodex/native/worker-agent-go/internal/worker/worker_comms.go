// Package worker provides communication and heartbeat logic for the worker agent.
// All control-plane messages are sent via ControlTransport.Send() instead of
// calling the HTTP API client directly.
package worker

import (
	"context"
	"math"
	"os"
	"runtime"
	"time"

	"velox-shared/controltransport"
	"velox-worker-agent/pkg/logger"
)

// Heartbeat intervals based on worker status
const (
	heartbeatIntervalIdle      = 60 * time.Second // Idle: less frequent
	heartbeatIntervalBusy      = 15 * time.Second // Busy: more frequent for progress updates
	heartbeatIntervalError     = 10 * time.Second // Error: rapid recovery attempts
	heartbeatMaxBackoff        = 5 * time.Minute  // Maximum backoff interval
	heartbeatBackoffMultiplier = 2.0              // Backoff multiplier
)

// detectMaxParallelJobs calculates the optimal concurrency based on hardware.
// Formula: clamp(NumCPU / 2, min=1, max=8)
//
// Used at worker init time to size the concurrency limiter; runtime
// capacity is read from w.concurrencyLimiter.MaxActiveJobs() everywhere
// else (PR-3.5 invariant: single source of truth for max_parallel_jobs).
func detectMaxParallelJobs() int {
	cpuCount := runtime.NumCPU()
	if cpuCount <= 0 {
		cpuCount = 2
	}
	parallel := int(math.Max(1, math.Min(8, float64(cpuCount/2))))
	return parallel
}

// getHeartbeatInterval returns the appropriate heartbeat interval based on worker status.
func (w *Worker) getHeartbeatInterval() time.Duration {
	w.mu.RLock()
	defer w.mu.RUnlock()

	switch w.status {
	case StatusBusy:
		return heartbeatIntervalBusy
	case StatusError:
		return heartbeatIntervalError
	default:
		return heartbeatIntervalIdle
	}
}

// heartbeatLoop sends periodic heartbeats to the master via transport.Send().
func (w *Worker) heartbeatLoop(ctx context.Context) {
	defer w.wg.Done()

	consecutiveErrors := 0
	maxConsecutiveErrors := 5
	currentInterval := w.getHeartbeatInterval()

	ticker := time.NewTicker(currentInterval)
	defer ticker.Stop()

	if err := w.sendHeartbeat(ctx); err != nil {
		logger.LogHeartbeatFailed(w.config.WorkerID, err, 1, maxConsecutiveErrors)
	} else {
		logger.LogHeartbeatSuccess(w.config.WorkerID, string(StatusIdle))
	}

	lastStatus := w.getStatus()

	for {
		select {
		case <-ctx.Done():
			w.logger.Debug("Heartbeat loop exiting (context done)")
			return
		case <-w.stopChan:
			w.logger.Debug("Heartbeat loop exiting (stop signal)")
			return
		case <-ticker.C:
			currentStatus := w.getStatus()
			if currentStatus != lastStatus {
				newInterval := w.getHeartbeatInterval()
				if newInterval != currentInterval {
					w.logger.Debug("[HEARTBEAT] Status changed %s->%s, adjusting interval %v->%v",
						lastStatus, currentStatus, currentInterval, newInterval)
					currentInterval = newInterval
					ticker.Reset(currentInterval)
				}
				lastStatus = currentStatus
			}

			err := w.sendHeartbeat(ctx)
			if err != nil {
				consecutiveErrors++
				logger.LogHeartbeatFailed(w.config.WorkerID, err, consecutiveErrors, maxConsecutiveErrors)

				if consecutiveErrors >= maxConsecutiveErrors {
					currentInterval = time.Duration(float64(currentInterval) * heartbeatBackoffMultiplier)
					if currentInterval > heartbeatMaxBackoff {
						currentInterval = heartbeatMaxBackoff
					}
					w.logger.Warn("[HEARTBEAT_BACKOFF] Applying backoff, next heartbeat in %v",
						currentInterval)
					ticker.Reset(currentInterval)
				}
			} else {
				if consecutiveErrors > 0 {
					logger.LogHeartbeatRecover(w.config.WorkerID, consecutiveErrors)
				}
				consecutiveErrors = 0

				newInterval := w.getHeartbeatInterval()
				if newInterval != currentInterval {
					currentInterval = newInterval
					ticker.Reset(currentInterval)
				}
			}
		}
	}
}

// getStatus returns the current worker status, derived from activeJobs and error state.
func (w *Worker) getStatus() Status {
	return w.Status()
}

// sendHeartbeat sends a single heartbeat to the master via transport.Send().
// PR-3.5: capabilities are derived from Worker.capabilitiesMap() —
// the same single helper buildHello uses. Any wire-shape change
// touches ONE function; hello and heartbeat stay in lock-step.
func (w *Worker) sendHeartbeat(ctx context.Context) error {
	status := w.Status()

	recentLogs, recentErrors := w.recentLogs.Snapshot(300, 100)
	payload := map[string]interface{}{}
	if len(recentLogs) > 0 {
		payload["recent_logs"] = recentLogs
		payload["recent_logs_count"] = len(recentLogs)
	}
	if len(recentErrors) > 0 {
		payload["recent_errors"] = recentErrors
		payload["recent_errors_count"] = len(recentErrors)
	}
	payload["worker_status"] = string(status)
	payload["worker_id"] = w.config.WorkerID
	payload["worker_name"] = w.config.WorkerName
	payload["status"] = string(status)
	payload["code_version"] = w.version
	payload["bundle_version"] = w.config.BundleVersion
	payload["bundle_hash"] = w.config.BundleHash
	payload["protocol_version"] = w.config.ProtocolVersion
	payload["engine_version"] = w.config.EngineVersion

	// PR-3.6 / F4: attach typed WorkerResourceCounters on every
	// heartbeat. The sampler publishes a refreshed snapshot to its
	// emit slot every 3 ticks (≈ 15s on the master F2 LastSeenResources
	// delta cadence); on a busy worker the bus cadence is 15s and the
	// snapshot is therefore fresh on every beat. On the idle 60s cadence
	// the same snapshot is replayed up to 4×, which is fine because the
	// sampler generates no work between emits. Nil-safe: skip the field
	// entirely if the sampler hasn't yet sampled (early session).
	if w.sampler != nil {
		if snap := w.sampler.Latest(); snap != nil {
			if m := snap.ToWireMap(); m != nil {
				payload["resources"] = m
			}
		}
	}

	hostname := ""
	if h, err := os.Hostname(); err == nil {
		hostname = h
	}
	payload["capabilities"] = w.capabilitiesMap(hostname)

	payload["jobs_completed"] = w.jobsCompleted.Load()
	payload["jobs_failed"] = w.jobsFailed.Load()

	w.activeJobsMu.RLock()
	activeJobList := make([]map[string]interface{}, 0, len(w.activeJobs))
	var primaryJobID string
	for _, aj := range w.activeJobs {
		if primaryJobID == "" {
			primaryJobID = aj.Job.JobID
		}
		jobInfo := map[string]interface{}{
			"job_id":     aj.Job.JobID,
			"job_run_id": aj.Job.JobRunID,
			"job_type":   aj.Job.JobType,
			"priority":   aj.Job.Priority,
			"lease_id":   aj.LeaseID,
			"attempt":    resolveJobAttempt(aj.Job),
		}
		if aj.Progress.Percent > 0 {
			jobInfo["progress_percent"] = aj.Progress.Percent
			jobInfo["progress_scene"] = aj.Progress.Scene
			jobInfo["progress_total"] = aj.Progress.TotalScenes
			if aj.Progress.Stage != "" {
				jobInfo["progress_stage"] = aj.Progress.Stage
			}
		}
		activeJobList = append(activeJobList, jobInfo)
	}
	w.activeJobsMu.RUnlock()

	if len(activeJobList) > 0 {
		payload["active_jobs"] = activeJobList
		payload["active_jobs_count"] = len(activeJobList)
	}
	payload["current_job"] = primaryJobID

	msg := controltransport.NewMessageWithPayload(
		controltransport.MsgHeartbeat,
		w.config.WorkerID,
		w.config.ProtocolVersion,
		payload,
	)

	if err := w.transport.Send(ctx, msg); err != nil {
		return err
	}
	return nil
}

// leaseRenewLoop sends periodic lease renewals for all active jobs AND
// task-native leases via transport.Send().
//
// PR-2 (canonical-attempt-identity): the task-native path fires
// MsgTaskLeaseRenewal alongside the existing MsgLeaseRenewal so a worker
// carrying BOTH legacy-job and task-native entries (during the PR #5
// cutover) keeps both lease types current. Each MsgTaskLeaseRenewal
// carries (task_id, attempt_id, lease_id, requested_expiry) so the
// master's RenewLease CAS predicate matches the canonical TaskAttempt
// row. Iteration source for task-native is the activeTaskLeases map
// populated by the MsgTaskLeaseGranted handler (added in this PR;
// pendingTasks → executeJob wiring is the caller-side population
// surface).
func (w *Worker) leaseRenewLoop(ctx context.Context) {
	defer w.wg.Done()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.Debug("Lease renew loop exiting (context done)")
			return
		case <-w.stopChan:
			w.logger.Debug("Lease renew loop exiting (stop signal)")
			return
		case <-ticker.C:
			w.activeJobsMu.RLock()
			jobs := make([]*ActiveJob, 0, len(w.activeJobs))
			for _, aj := range w.activeJobs {
				jobs = append(jobs, aj)
			}
			w.activeJobsMu.RUnlock()

			for _, aj := range jobs {
				if aj == nil || aj.Job == nil {
					continue
				}

				leaseID := aj.LeaseID
				if leaseID == "" {
					continue
				}

				leaseExpiry := time.Now().UTC().Add(30 * time.Minute).Format(time.RFC3339)
				attempt := resolveJobAttempt(aj.Job)

				payload := map[string]interface{}{
					"job_id":           aj.Job.JobID,
					"lease_id":         leaseID,
					"attempt":          attempt,
					"lease_expires_at": leaseExpiry,
				}

				msg := controltransport.NewMessageWithPayload(
					controltransport.MsgLeaseRenewal,
					w.config.WorkerID,
					w.config.ProtocolVersion,
					payload,
				)

				if err := w.transport.Send(ctx, msg); err != nil {
					w.logger.Warn("[LEASE] Failed to renew lease for job %s: %v", aj.Job.JobID, err)
				} else {
					w.logger.Debug("[LEASE] Renewed lease for job %s (lease_id=%s)", aj.Job.JobID, leaseID)
				}
			}

			// PR-2: task-native renewals — dispatched ALONGSIDE the
			// legacy job-loop. Snapshot under RLock, iterate outside the
			// lock (transport.Send is network I/O; must NOT block the
			// write-side Stop()/Remove callers).
			taskLeases := w.SnapshotActiveTaskLeases()
			for _, tl := range taskLeases {
				if tl == nil || tl.TaskID == "" || tl.LeaseID == "" {
					continue
				}

				taskExpiry := time.Now().UTC().Add(30 * time.Minute).Format(time.RFC3339)
				payload := map[string]interface{}{
					"task_id":          tl.TaskID,
					"attempt_id":       tl.AttemptID,
					"lease_id":         tl.LeaseID,
					"requested_expiry": taskExpiry,
				}

				msg := controltransport.NewMessageWithPayload(
					controltransport.MsgTaskLeaseRenewal,
					w.config.WorkerID,
					w.config.ProtocolVersion,
					payload,
				)

				if err := w.transport.Send(ctx, msg); err != nil {
					w.logger.Warn("[TASK_LEASE] Failed to renew lease for task %s: %v", tl.TaskID, err)
				} else {
					w.logger.Debug("[TASK_LEASE] Renewed lease for task %s (attempt=%s lease_id=%s)",
						tl.TaskID, tl.AttemptID, tl.LeaseID)
				}
			}
		}
	}
}

// AddActiveTaskLease registers a task-native lease so leaseRenewLoop will
// dispatch MsgTaskLeaseRenewal for it. Called by the MsgTaskLeaseGranted
// handler (PR #5 / canonical-attempt-identity) right after it pops the
// pending task from pendingTasks. Safe for concurrent callers; nil/empty
// taskID or leaseID is a no-op (caller must drop via canonical-error path).
//
// The map is unconditionally initialized in worker_init.go New(); this
// helper never performs lazy-init — if the map is nil here, the worker
// was constructed defectively and a panic is the correct loud-failure
// surface (cover-up via lazy init masks operator bugs).
func (w *Worker) AddActiveTaskLease(taskID, attemptID, leaseID string) {
	if taskID == "" || leaseID == "" {
		return
	}
	w.activeTaskLeasesMu.Lock()
	defer w.activeTaskLeasesMu.Unlock()
	w.activeTaskLeases[taskID] = &ActiveTaskLease{
		TaskID:    taskID,
		AttemptID: attemptID,
		LeaseID:   leaseID,
	}
}

// RemoveActiveTaskLease deregisters a task-native lease so leaseRenewLoop
// stops dispatching MsgTaskLeaseRenewal for it. Called on MsgLeaseRevoked
// / canonical terminal-state transition (executeJob returns SUCCEEDED /
// FAILED / CANCELLED / TIMED_OUT). Empty taskID is a no-op.
func (w *Worker) RemoveActiveTaskLease(taskID string) {
	if taskID == "" {
		return
	}
	w.activeTaskLeasesMu.Lock()
	defer w.activeTaskLeasesMu.Unlock()
	delete(w.activeTaskLeases, taskID)
}

// SnapshotActiveTaskLeases returns a defensive copy of the current
// task-native lease set. Iteration over the snapshot must occur WITHOUT
// holding activeTaskLeasesMu — transport.Send is network I/O and would
// otherwise block Remove-side writers (Stop, cancelJob, Revoked handlers).
func (w *Worker) SnapshotActiveTaskLeases() []*ActiveTaskLease {
	w.activeTaskLeasesMu.RLock()
	defer w.activeTaskLeasesMu.RUnlock()
	if len(w.activeTaskLeases) == 0 {
		return nil
	}
	out := make([]*ActiveTaskLease, 0, len(w.activeTaskLeases))
	for _, tl := range w.activeTaskLeases {
		out = append(out, tl)
	}
	return out
}

// calculateBackoff returns the next backoff interval capped at heartbeatMaxBackoff.
func (w *Worker) calculateBackoff(current time.Duration) time.Duration {
	next := time.Duration(float64(current) * heartbeatBackoffMultiplier)
	if next > heartbeatMaxBackoff {
		return heartbeatMaxBackoff
	}
	return next
}

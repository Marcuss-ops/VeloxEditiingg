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
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/pkg/logger"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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
// Formula: clamp(NumCPU / 2, min=1, max=8).
//
// ⚠️ This is a FALLBACK only: if cfg.MaxActiveJobs > 0 (which includes the
// default value 1 from DefaultConfig), worker_init.go uses the configured
// value instead. Operators who want hardware-detected concurrency must
// explicitly set max_active_jobs=0 in their config.
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

	// Build typed Heartbeat proto directly instead of map payload.
	hb := &pb.Heartbeat{
		WorkerName:      w.config.WorkerName,
		WorkerStatus:    string(status),
		Status:          string(status),
		CodeVersion:     w.version,
		BundleVersion:   w.config.BundleVersion,
		BundleHash:      w.config.BundleHash,
		ProtocolVersion: w.config.ProtocolVersion,
		EngineVersion:   w.config.EngineVersion,
		JobsCompleted:   w.tasksCompleted.Load(),
		JobsFailed:      w.tasksFailed.Load(),
	}

	// Collect dynamic extra fields (recent_logs, capabilities, active_jobs,
	// resources, current_job) into Heartbeat.Extra as structpb.Struct.
	extraMap := make(map[string]interface{})

	recentLogs, recentErrors := w.recentLogs.Snapshot(300, 100)
	if len(recentLogs) > 0 {
		extraMap["recent_logs"] = recentLogs
		extraMap["recent_logs_count"] = len(recentLogs)
	}
	if len(recentErrors) > 0 {
		extraMap["recent_errors"] = recentErrors
		extraMap["recent_errors_count"] = len(recentErrors)
	}

	// PR-3.6 / F4: attach typed WorkerResourceCounters.
	if w.sampler != nil {
		if snap := w.sampler.Latest(); snap != nil {
			if m := snap.ToWireMap(); m != nil {
				extraMap["resources"] = m
			}
		}
	}

	hostname := ""
	if h, err := os.Hostname(); err == nil {
		hostname = h
	}
	extraMap["capabilities"] = w.capabilitiesMap(hostname)
	extraMap["worker_id"] = w.config.WorkerID

	w.activeTasksMu.RLock()
	activeJobList := make([]map[string]interface{}, 0, len(w.activeTasks))
	var primaryJobID string
	for _, at := range w.activeTasks {
		if primaryJobID == "" {
			primaryJobID = at.JobID
		}
		jobInfo := map[string]interface{}{
			"job_id":     at.JobID,
			"job_run_id": "",
			"job_type":   at.Task.ExecutorID,
			"priority":   0,
			"lease_id":   at.LeaseID,
			"attempt":    at.Task.AttemptNumber,
		}
		if at.Progress.Percent > 0 {
			jobInfo["progress_percent"] = at.Progress.Percent
			jobInfo["progress_scene"] = at.Progress.Scene
			jobInfo["progress_total"] = at.Progress.TotalScenes
			if at.Progress.Stage != "" {
				jobInfo["progress_stage"] = at.Progress.Stage
			}
		}
		activeJobList = append(activeJobList, jobInfo)
	}
	w.activeTasksMu.RUnlock()

	if len(activeJobList) > 0 {
		extraMap["active_jobs"] = activeJobList
	}
	hb.CurrentJob = primaryJobID
	hb.ActiveJobsCount = int32(len(activeJobList))

	// Serialize extra map to structpb.Struct.
	if len(extraMap) > 0 {
		if extra, err := structpb.NewStruct(extraMap); err == nil {
			hb.Extra = extra
		}
	}

	msg := controltransport.NewTypedMessage(
		controltransport.MsgHeartbeat,
		w.config.WorkerID,
		w.config.ProtocolVersion,
		hb,
	)

	if err := w.transport.Send(ctx, msg); err != nil {
		return err
	}
	return nil
}

// leaseRenewLoop sends periodic lease renewals for all active jobs AND
// task-native leases via transport.Send().
//
// PR-2 (canonical-attempt-identity): fires MsgTaskLeaseRenewal for
// every activeTaskLeases entry. Legacy job lease renewal removed.
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
			// PR-2: task-native lease renewals — dispatched via activeTaskLeases.
			// Builds typed pb.TaskLeaseRenewal directly instead of map payload.
			taskLeases := w.SnapshotActiveTaskLeases()
			for _, tl := range taskLeases {
				if tl == nil || tl.TaskID == "" || tl.JobID == "" || tl.AttemptID == "" || tl.LeaseID == "" || tl.AttemptNumber <= 0 {
					continue
				}

				taskExpiry := time.Now().UTC().Add(30 * time.Minute)
				renewal := &pb.TaskLeaseRenewal{
					TaskId:          tl.TaskID,
					JobId:           tl.JobID,
					AttemptId:       tl.AttemptID,
					LeaseId:         tl.LeaseID,
					AttemptNumber:   int32(tl.AttemptNumber),
					Revision:        int32(tl.Revision),
					RequestedExpiry: timestamppb.New(taskExpiry),
				}

				msg := controltransport.NewTypedMessage(
					controltransport.MsgTaskLeaseRenewal,
					w.config.WorkerID,
					w.config.ProtocolVersion,
					renewal,
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
func (w *Worker) AddActiveTaskLease(taskID, jobID, attemptID, leaseID string, attemptNumber, revision int) {
	if taskID == "" || jobID == "" || attemptID == "" || leaseID == "" || attemptNumber <= 0 {
		return
	}
	w.activeTaskLeasesMu.Lock()
	defer w.activeTaskLeasesMu.Unlock()
	w.activeTaskLeases[taskID] = &ActiveTaskLease{
		TaskID:        taskID,
		JobID:         jobID,
		AttemptID:     attemptID,
		LeaseID:       leaseID,
		AttemptNumber: attemptNumber,
		Revision:      revision,
	}
}

// RemoveActiveTaskLease deregisters a task-native lease so leaseRenewLoop
// stops dispatching MsgTaskLeaseRenewal for it. Called on MsgLeaseRevoked
// / canonical terminal-state transition (executeTask returns SUCCEEDED /
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

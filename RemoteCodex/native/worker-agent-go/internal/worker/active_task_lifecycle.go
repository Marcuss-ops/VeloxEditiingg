// Package worker — active-task lifecycle helpers.
//
// active_task_lifecycle.go owns the helpers extracted from the
// monolithic executeTask in the original job_executor.go. The
// helpers preserve byte-identical behavior — the same lock
// acquisition order, the same telemetry call ordering, the same
// 3-branch outcome handling — and are intentionally small so a
// future regression can be bisected to the helper boundary.
//
// Helper surface:
//
//	registerActiveTask        — builds *ActiveTaskExecution, inserts
//	                            it into the activeTasks + taskIDsByJob
//	                            maps under activeTasksMu. Returns the
//	                            pointer so the caller can assign
//	                            activeTask.Cancel = jobCancel AFTER
//	                            wakeHeartbeat (preserving the
//	                            original ordering).
//	unregisterActiveTask      — deferred cleanup that mirrors the
//	                            original closure: deletes from the
//	                            maps, removes empty jobID entries,
//	                            wakes the heartbeat.
//	withJobProgressCallback   — wraps the parent context with the
//	                            progress callback that updates
//	                            activeTask.Progress under the
//	                            activeTasksMu lock.
//	recordTaskStart           — telemetry seeds (SetWorkerStatus=2,
//	                            SetWorkerActiveJobs, LogJobStart).
//	recordTaskOutcome         — the 3-branch outcome telemetry
//	                            (cancelled / failed / succeeded)
//	                            matching the original nested if/else.
//	                            RecordJobRuntime is called in every
//	                            branch.
//	recordTaskFinish          — restores idle-side telemetry
//	                            (SetWorkerStatus=1, SetWorkerActiveJobs).
package worker

import (
	"context"
	"errors"
	"time"

	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/logger"
	"velox-worker-agent/pkg/video/pipeline"
)

// registerActiveTask builds the ActiveTaskExecution entry, inserts
// it under activeTasksMu, and returns the pointer. The caller MUST
// call wakeHeartbeat immediately after, then assign
// activeTask.Cancel = jobCancel — preserving the original ordering
// where the heartbeat goroutine sees the new entry BEFORE the cancel
// function is wired up.
func (w *Worker) registerActiveTask(taskID, attemptID string, pte *PendingTaskExecution) *ActiveTaskExecution {
	activeTask := &ActiveTaskExecution{
		TaskID:    taskID,
		AttemptID: attemptID,
		JobID:     pte.JobID,
		Task:      pte,
		LeaseID:   pte.LeaseID,
		StartedAt: time.Now(),
	}
	w.activeTasksMu.Lock()
	w.activeTasks[taskID] = activeTask
	w.taskIDsByJob[pte.JobID] = append(w.taskIDsByJob[pte.JobID], taskID)
	w.activeTasksMu.Unlock()
	return activeTask
}

// unregisterActiveTask is the deferred cleanup that mirrors the
// original closure: deletes the active task from both maps, removes
// the jobID entry when its task list drains to zero, then wakes the
// heartbeat so the next tick reports the updated state.
func (w *Worker) unregisterActiveTask(taskID string, pte *PendingTaskExecution) {
	w.activeTasksMu.Lock()
	delete(w.activeTasks, taskID)
	taskIDs := w.taskIDsByJob[pte.JobID]
	for i, tid := range taskIDs {
		if tid == taskID {
			w.taskIDsByJob[pte.JobID] = append(taskIDs[:i], taskIDs[i+1:]...)
			break
		}
	}
	if len(w.taskIDsByJob[pte.JobID]) == 0 {
		delete(w.taskIDsByJob, pte.JobID)
	}
	w.activeTasksMu.Unlock()
	w.wakeHeartbeat()
}

// withJobProgressCallback returns a child context carrying the
// canonical progress callback that updates activeTask.Progress under
// the activeTasksMu lock. The callback uses taskID to dynamically
// look up the current entry — never the captured pointer — so a
// later replace (which the original code does NOT do) would still
// route to the fresh entry.
func (w *Worker) withJobProgressCallback(parent context.Context, taskID string) context.Context {
	return pipeline.WithProgressCallback(parent, func(percent, scene, total int, stage string) {
		w.activeTasksMu.Lock()
		if current := w.activeTasks[taskID]; current != nil {
			current.Progress = JobProgress{Percent: int32(percent), Scene: int32(scene), TotalScenes: int32(total), Stage: stage}
		}
		w.activeTasksMu.Unlock()
	})
}

// recordTaskStart seeds the start-side telemetry counters: worker
// status is bumped to busy (2) and the active-jobs gauge tracks the
// concurrency limiter snapshot. LogJobStart fires once per task.
func (w *Worker) recordTaskStart(pte *PendingTaskExecution) {
	telemetry.GetPrometheusMetrics().SetWorkerStatus(w.config.WorkerID, 2)
	telemetry.GetPrometheusMetrics().SetWorkerActiveJobs(w.config.WorkerID, float64(w.concurrencyLimiter.ActiveJobCount()))
	logger.LogJobStart(w.config.WorkerID, pte.JobID, pte.ExecutorID, 0)
}

// recordTaskOutcome captures the 3-branch outcome telemetry
// matching the original nested if/else in executeTask:
//
//  1. execErr is context.Canceled → LogJobCancelled only.
//  2. execErr is any other error   → LogJobFailedWithType +
//     setStatus(StatusError) + tasksFailed.Add + RecordJobFailure.
//  3. execErr is nil               → LogJobSuccess +
//     tasksCompleted.Add + RecordJobSuccess.
//
// RecordJobRuntime fires in every branch (job-runtime is observed
// regardless of outcome). The "upload task outputs" wrap error
// from executeTask routes here as a non-cancelled error and lands
// in branch 2, which is the canonical "failed" surface for upload
// rejection.
func (w *Worker) recordTaskOutcome(pte *PendingTaskExecution, execErr error, duration time.Duration) {
	if execErr != nil {
		if errors.Is(execErr, context.Canceled) {
			logger.LogJobCancelled(w.config.WorkerID, pte.JobID, duration)
		} else {
			logger.LogJobFailedWithType(w.config.WorkerID, pte.JobID, pte.ExecutorID, execErr, duration)
			w.setStatus(StatusError)
			w.tasksFailed.Add(1)
			telemetry.RecordJobFailure(duration.Milliseconds())
		}
		telemetry.GetPrometheusMetrics().RecordJobRuntime(pte.ExecutorID, float64(duration.Milliseconds()))
	} else {
		logger.LogJobSuccess(w.config.WorkerID, pte.JobID, pte.ExecutorID, duration)
		w.tasksCompleted.Add(1)
		telemetry.RecordJobSuccess(duration.Milliseconds())
		telemetry.GetPrometheusMetrics().RecordJobRuntime(pte.ExecutorID, float64(duration.Milliseconds()))
	}
}

// recordTaskFinish restores idle-side telemetry AFTER the outcome
// has been recorded and BEFORE submitTaskResult runs, matching the
// original ordering in executeTask (SetWorkerStatus=1 + SetWorker-
// ActiveJobs fire after RecordJobRuntime and before submitTaskResult).
func (w *Worker) recordTaskFinish() {
	telemetry.GetPrometheusMetrics().SetWorkerStatus(w.config.WorkerID, 1)
	telemetry.GetPrometheusMetrics().SetWorkerActiveJobs(w.config.WorkerID, float64(w.concurrencyLimiter.ActiveJobCount()))
}

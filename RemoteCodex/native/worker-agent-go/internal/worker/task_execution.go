// Package worker provides task processing logic for the worker agent.
// Every execution flows through the task-native dispatch:
// TaskOffer → executeTask → TaskResult, using PendingTaskExecution
// as the canonical typed carrier for all identity and spec data.
//
// File layout in this package (job_executor split):
//
//	task_execution.go       — this file: executeTask orchestrator.
//	                          Owns the canonical package doc above.
//	task_dispatch.go        — runJobTask + dispatchTaskRunner (the
//	                          dispatch path that resolves assets and
//	                          invokes TaskRunner.Run).
//	task_result_builder.go  — submitTaskResult: builds and sends
//	                          the typed pb.TaskResult via the
//	                          transport (wire-format canonical).
//	active_task_lifecycle.go — active-task lifecycle helpers:
//	                          registerActiveTask, unregisterActiveTask,
//	                          withJobProgressCallback, recordTaskStart,
//	                          recordTaskOutcome, recordTaskFinish.
//	job_executor.go         — redistributive shell (kept for git
//	                          history; content moved to the four
//	                          files above).
//	output_upload.go        — uploadTaskOutputs + selectUploadableOutput
//	                          (kept separate; not part of the split).
package worker

import (
	"context"
	"fmt"
	"time"

	"velox-worker-agent/internal/telemetry"
)

// executeTask executes a task and reports the result via typed TaskResult.
//
// Lifecycle order is preserved byte-identical to the original
// job_executor.go executeTask:
//
//  1. Concurrency limiter acquire (with deferred release).
//  2. State-transition check (StatusIdle → StatusBusy).
//  3. Active task registration (helper builds *ActiveTaskExecution,
//     inserts into maps under activeTasksMu, returns the pointer).
//  4. wakeHeartbeat (publishes the new task state immediately).
//  5. Defer unregisterActiveTask (mirrors the original closure).
//  6. jobCtx + jobCancel = context.WithCancel(ctx).
//  7. activeTask.Cancel = jobCancel (assigned AFTER the wake so the
//     heartbeat goroutine observes the same ordering as the original).
//  8. Defer jobCancel (paired with the cancel context).
//  9. jobCtx = withJobProgressCallback (lock-and-update Progress).
//  10. recordTaskStart (telemetry + LogJobStart).
//  11. runJobTask → dispatchTaskRunner.
//  12. Upload outputs (only when runJobTask returned nil execErr; a
//     non-nil uploadErr wraps execErr via fmt.Errorf("upload task
//     outputs: %w", uploadErr) so submitTaskResult + recordTaskOutcome
//     see the canonical "failed" branch).
//  13. recordTaskOutcome (3-branch outcome telemetry).
//  14. recordTaskFinish (idle-side telemetry restoration).
//  15. Submit context + ack timing + submitTaskResult +
//     RecordJobCompleteAck (ack latency captured after Send returns).
//  16. Error backoff: 2-second sleep + StatusIdle transition (only on
//     non-nil execErr).
func (w *Worker) executeTask(ctx context.Context, pte *PendingTaskExecution, taskID, attemptID string) {
	if err := w.concurrencyLimiter.Acquire(ctx, pte.JobID, 0); err != nil {
		w.logger.Warn("[CONCURRENCY] Failed to acquire slot for job %s: %v", pte.JobID, err)
		return
	}
	defer w.concurrencyLimiter.Release()

	if !w.canTransitionTo(StatusBusy) {
		w.logger.Warn("Cannot accept task: invalid state transition from %s to busy", w.Status())
		return
	}

	activeTask := w.registerActiveTask(taskID, attemptID, pte)
	w.wakeHeartbeat()
	defer w.unregisterActiveTask(taskID, pte)

	jobCtx, jobCancel := context.WithCancel(ctx)
	activeTask.Cancel = jobCancel
	defer jobCancel()
	jobCtx = w.withJobProgressCallback(jobCtx, taskID)

	w.recordTaskStart(pte)

	startTime := time.Now()

	w.logger.Info("[TASK] Executing task %s (job=%s attempt=%s)", taskID, pte.JobID, attemptID)

	report, execErr := w.runJobTask(jobCtx, pte)

	duration := time.Since(startTime)

	if execErr == nil {
		if uploadErr := w.uploadTaskOutputs(jobCtx, pte, report); uploadErr != nil {
			execErr = fmt.Errorf("upload task outputs: %w", uploadErr)
		}
	}

	w.recordTaskOutcome(pte, execErr, duration)
	w.recordTaskFinish()

	submitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ackStartTime := time.Now()

	w.submitTaskResult(submitCtx, pte, taskID, attemptID, report, execErr)

	telemetry.GetPrometheusMetrics().RecordJobCompleteAck(pte.ExecutorID, float64(time.Since(ackStartTime).Milliseconds()))

	if execErr != nil {
		time.Sleep(2 * time.Second)
		if w.canTransitionTo(StatusIdle) {
			w.setStatus(StatusIdle)
		}
	}
}

// Package worker provides task processing logic for the worker agent.
// Every execution flows through the task-native dispatch:
// TaskOffer → executeTask → TaskResult, using PendingTaskExecution
// as the canonical typed carrier for all identity and spec data.
//
// File layout in this package:
//
//	task_execution.go       — this file: executeTask orchestrator.
//	                          Owns the canonical package doc above.
//	task_dispatch.go        — dispatch path (runJobTask +
//	                          dispatchTaskRunner) AND active-task
//	                          lifecycle helpers (registerActiveTask +
//	                          unregisterActiveTask +
//	                          withJobProgressCallback).
//	task_result_builder.go  — submitTaskResult: builds and sends
//	                          the typed pb.TaskResult via the
//	                          transport (wire-format canonical).
//	active_task_lifecycle.go — metriche + upload helpers:
//	                          recordTaskStart, recordTaskOutcome
//	                          (3-branch outcome telemetry),
//	                          recordTaskFinish, uploadTaskOutputs
//	                          (OTel "upload" span + master API
//	                          upload), selectUploadableOutput.
package worker

import (
	"context"
	"fmt"
	"time"

	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/internal/telemetry"
)

func reportRecorder(report *taskrunner.TaskExecutionReport) *telemetry.EventRecorder {
	if report == nil {
		return nil
	}
	return report.AttemptRecorder
}

// executeTask executes a task and reports the result via typed TaskResult.
//
// Lifecycle order:
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
//  16. Error backoff: cancellable 2-second delay + StatusIdle transition (only on
//     non-nil execErr).
func (w *Worker) executeTask(ctx context.Context, pte *PendingTaskExecution, taskID, attemptID string) {
	acquired := false
	if err := w.concurrencyLimiter.Acquire(ctx, pte.JobID, 0); err != nil {
		w.logger.Warn("[CONCURRENCY] Failed to acquire slot for job %s: %v", pte.JobID, err)
		return
	}
	acquired = true
	defer func() {
		if acquired {
			w.concurrencyLimiter.Release()
		}
	}()

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
	attemptTelemetry := telemetry.NewAttemptTelemetrySession(w.sampler)
	// Start/Stop is the single telemetry entry point: the session drives
	// the collector+sink pipeline (collectors gather the RAW facts at
	// Stop, sinks project them). Producers never call the pipeline.
	attemptTelemetry.BindPipeline(w.newAttemptPipeline(pte, attemptID, attemptTelemetry))
	attemptTelemetry.Start(jobCtx)
	jobCtx = telemetry.WithAttemptTelemetry(jobCtx, attemptTelemetry)

	w.logger.Info("[TASK] Executing task %s (job=%s attempt=%s)", taskID, pte.JobID, attemptID)

	report, execErr := w.runJobTask(jobCtx, pte)
	recordExecutionDownloadMetrics(report)
	if execErr == nil {
		pipelineStatus := pte.ExecutorID
		if report != nil && report.ExecutorKey != "" {
			pipelineStatus = report.ExecutorKey
		}
		w.logArtifactProtocol("RENDER_COMPLETED", pte, startTime, "", "", "", map[string]interface{}{
			"executor_id":     pte.ExecutorID,
			"output_count":    artifactReportOutputCount(report),
			"pipeline_status": pipelineStatus,
		})
	}

	duration := time.Since(startTime)
	if report != nil {
		// The execute phase marker is the canonical lifecycle timing. Do not
		// read pipeline.render_ms from the deprecated report map: that map is
		// only a compatibility projection for unmigrated consumers.
		if renderDuration, ok := report.RenderDuration(); ok {
			duration = renderDuration
		}
	}
	// Prometheus render timing is projected from the canonical AttemptSnapshot
	// by PrometheusSink at attempt Stop. This lifecycle path only keeps the
	// duration for task outcome accounting.

	if execErr == nil {
		if acquired {
			w.concurrencyLimiter.Release()
			acquired = false
		}
		if uploadErr := w.uploadTaskOutputs(jobCtx, pte, report); uploadErr != nil {
			execErr = fmt.Errorf("upload task outputs: %w", uploadErr)
		}
	}

	if report != nil && report.AttemptEvents != nil {
		status := telemetry.StatusOK
		if execErr != nil {
			status = telemetry.StatusFailed
		}
		report.AttemptEvents.AttemptCompleted(status)
	}

	// Upload/commit runs outside TaskRunner.Run. Snapshot those late events
	// only after the complete attempt lifecycle, preserving the canonical
	// append-only journal for every later projection.
	taskrunner.AppendDetailedPhases(report, reportRecorder(report))
	if report != nil {
		// Hand the executor-owned RAW envelope to the canonical attempt
		// pipeline before Stop. The session collector will merge only its
		// resource-owned fields, preserving engine/resolver/publisher facts.
		attemptTelemetry.SetExecutorRawMetrics(report.RawMetrics)
	}
	result := attemptTelemetry.Stop(context.Background())
	if report != nil {
		// Raw typed metrics are the canonical accumulator. Merge only the
		// fields owned by AttemptTelemetrySession so executor/media/cache
		// facts already present on the report are not overwritten.
		var raw telemetry.RawExecutionMetrics
		if report.RawMetrics != nil {
			raw = *report.RawMetrics
		} else if report.TypedMetrics != nil {
			raw = *report.TypedMetrics
		}
		telemetry.MergeAttemptResourceFactsInto(&raw, result.Metrics)
		report.RawMetrics = &raw
		report.TypedMetrics = report.RawMetrics
	}

	w.recordTaskOutcome(pte, execErr, duration)
	w.recordTaskFinish()

	submitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ackStartTime := time.Now()

	w.reporter.Submit(submitCtx, pte, taskID, attemptID, report, execErr)

	telemetry.GetPrometheusMetrics().RecordJobCompleteAck(pte.ExecutorID, float64(time.Since(ackStartTime).Milliseconds()))

	if execErr != nil {
		timer := time.NewTimer(2 * time.Second)
		select {
		case <-jobCtx.Done():
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
		}
		if w.canTransitionTo(StatusIdle) {
			w.setStatus(StatusIdle)
		}
	}
}

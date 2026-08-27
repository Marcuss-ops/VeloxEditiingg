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

	sharedtelemetry "velox-shared/telemetry"
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
	jobCtx = withProgressTaskID(w.withJobProgressCallback(jobCtx, taskID), taskID)

	w.recordTaskStart(pte)

	startTime := time.Now()
	renderStartedAt := startTime
	milestones := telemetry.NewAttemptMilestoneRecorderAt(startTime)
	milestones.Mark(sharedtelemetry.MilestoneAttemptAccepted)
	milestones.Mark(sharedtelemetry.MilestoneExecutionStarted)
	activeTask.Milestones = milestones
	waterfall := taskrunner.NewWaterfallRecorder(startTime)
	waterfall.Transition("wait_before_assets", startTime)
	attemptTelemetry := telemetry.NewAttemptTelemetrySession(w.sampler)
	attemptTelemetry.BindPipeline(w.newAttemptPipeline(pte, attemptID, attemptTelemetry))
	attemptTelemetry.Start(jobCtx)
	jobCtx = telemetry.WithAttemptTelemetry(jobCtx, attemptTelemetry)
	jobCtx = telemetry.WithMilestoneRecorder(jobCtx, milestones)
	jobCtx = taskrunner.WithWaterfallRecorder(jobCtx, waterfall)

	w.logger.Info("[TASK] Executing task %s (job=%s attempt=%s)", taskID, pte.JobID, attemptID)

	report, execErr := w.runJobTask(jobCtx, pte)
	if m := telemetry.MilestoneRecorderFromContext(jobCtx); m != nil {
		if execErr == nil {
			m.Mark(sharedtelemetry.MilestoneAllAssetsReady)
		}
		m.Mark(sharedtelemetry.MilestoneRenderCompleted)
		m.Mark(sharedtelemetry.MilestoneFinalizeStarted)
	}
	waterfall.Transition("finalize", time.Now().UTC())
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
		renderEndedAt := time.Now()
		if acquired {
			w.concurrencyLimiter.Release()
			acquired = false
		}
		if m := telemetry.MilestoneRecorderFromContext(jobCtx); m != nil {
			m.Mark(sharedtelemetry.MilestoneFinalizeCompleted)
			// The typed publisher (modern path) marks output.durable, publish.queued
			// and publish.started at their real boundaries inside publishArtifactsV1
			// (spool registered, declared, transfer begun). Only the legacy apiClient
			// upload path needs those coarse marks here; firing them earlier would
			// otherwise clobber the precise times, since Mark is first-write-wins.
			if w.transport == nil || w.publisherRegistry == nil {
				m.Mark(sharedtelemetry.MilestoneOutputDurable)
				m.Mark(sharedtelemetry.MilestonePublishQueued)
				m.Mark(sharedtelemetry.MilestonePublishStarted)
			}
		}
		w.UpdateOperationalPhase(taskID, PhasePublishing)
		waterfall.Transition("upload", time.Now().UTC())
		uploadStartedAt := time.Now()
		telemetry.GetPrometheusMetrics().RecordRenderUploadOverlap(renderEndedAt.Sub(renderStartedAt))
		_ = uploadStartedAt
		if uploadErr := w.uploadTaskOutputs(jobCtx, pte, report); uploadErr != nil {
			execErr = fmt.Errorf("upload task outputs: %w", uploadErr)
		}
		if m := telemetry.MilestoneRecorderFromContext(jobCtx); m != nil {
			m.Mark(sharedtelemetry.MilestonePublishCompleted)
		}
	}

	if report != nil && report.AttemptEvents != nil {
		status := telemetry.StatusOK
		if execErr != nil {
			status = telemetry.StatusFailed
		}
		report.AttemptEvents.AttemptCompleted(status)
	}
	if m := telemetry.MilestoneRecorderFromContext(jobCtx); m != nil {
		m.Mark(sharedtelemetry.MilestoneResultSending)
	}
	waterfall.Transition("commit_wait", time.Now().UTC())

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

	// Operational lifecycle: committing task result.
	w.UpdateOperationalPhase(taskID, PhaseCommitWait)

	submitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ackStartTime := time.Now()

	waterfall.Transition("result_report", time.Now().UTC())
	waterfall.Finish(time.Now().UTC(), waterfallStatus(execErr))
	if report != nil {
		report.Waterfall = waterfall.Snapshot()
		if m := telemetry.MilestoneRecorderFromContext(jobCtx); m != nil {
			// The durable report can only carry milestones up to result.sending:
			// it must be fully built before Submit serialises and hashes it.
			// result.sent / attempt.completed are stamped post-Submit below, at
			// the real transport boundary, and reach consumers through the live
			// heartbeat projection.
			report.Milestones = m.Snapshot()
		}
	}
	w.reporter.Submit(submitCtx, pte, taskID, attemptID, report, execErr)

	if m := telemetry.MilestoneRecorderFromContext(jobCtx); m != nil {
		// Submit is synchronous and returns once the transport accepted the
		// TaskResult (the spool-backed outbox path also waits out the
		// TaskResultAck window; Send errors are swallowed and retried inside
		// the reporter), so this is the true result.sent boundary. Stamping it
		// pre-Submit collapsed sending/sent/completed onto one elapsed_ms and
		// made reporting lag invisible to the waterfall.
		m.Mark(sharedtelemetry.MilestoneResultSent)
		m.Mark(sharedtelemetry.MilestoneAttemptCompleted)
	}

	// Operational lifecycle: task completed.
	w.UpdateOperationalPhase(taskID, PhaseDone)

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

func waterfallStatus(err error) string {
	if err != nil {
		return "failed"
	}
	return "ok"
}

// Package worker — active-task metriche + upload.
//
// active_task_lifecycle.go owns the helpers extracted from the
// monolithic executeTask in the original job_executor.go that are
// about the task's metriche (telemetry) and upload (output bytes)
// side-effects. Behaviour-preserving structural split:
//
//	recordTaskStart           — telemetry seeds (SetWorkerStatus=2,
//	                            SetWorkerActiveJobs, LogJobStart).
//	recordTaskOutcome         — the 3-branch outcome telemetry
//	                            (cancelled / failed / succeeded)
//	                            matching the original nested if/else.
//	                            RecordJobRuntime is called in every
//	                            branch.
//	recordTaskFinish          — restores idle-side telemetry
//	                            (SetWorkerStatus=1, SetWorkerActiveJobs).
//	uploadTaskOutputs         — uploads the first uploadable output
//	                            artifact to the master API and wraps
//	                            the canonical "upload" OTel span.
//	                            Returns nil when there are no outputs;
//	                            wraps with "upload task outputs: %w"
//	                            at the caller (executeTask) when the
//	                            master rejects the upload.
//	selectUploadableOutput    — picks the canonical render.output
//	                            artifact first, falling back to any
//	                            artifact with a non-empty URI.
//
// The active-task registration / cleanup / progress-tracking helpers
// (registerActiveTask, unregisterActiveTask, withJobProgressCallback)
// live in task_dispatch.go alongside the dispatch path because they
// are part of the same resource-management surface.
package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/oteltrace"
	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/logger"
)

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

// uploadTaskOutputs uploads the canonical render.output (or the
// first artifact with a non-empty URI) to the master API. Started
// here so it integrates with the active-task lifecycle surface
// (telemetry counters, status transitions, task-result wrapping).
//
// Behaviour:
//   - Returns nil when there are no outputs to upload.
//   - Returns "worker output upload: api client is not configured"
//     when apiClient is nil.
//   - Returns "worker output upload: no uploadable output with a
//     local file path" when no artifact carries a URI.
//   - Returns "worker output upload: output file <path> is not
//     readable: <err>" when the local file is missing.
//   - Returns the api client's error verbatim on transport failure.
//   - Returns "worker output upload: master rejected upload for
//     job <id>: <err>" when the master responds with !OK.
//
// Scorecard v2 / Step 15: starts an "upload" span for distributed
// tracing. The span is closed on function return via defer span.End().
func (w *Worker) uploadTaskOutputs(ctx context.Context, pte *PendingTaskExecution, report *taskrunner.TaskExecutionReport) error {
	ctx, span := oteltrace.StartSpan(ctx, "upload",
		oteltrace.AttrJobID(pte.JobID),
		oteltrace.AttrTaskID(pte.TaskID),
	)
	defer span.End()

	if report == nil || len(report.Outputs) == 0 {
		return nil
	}
	if w.apiClient == nil {
		return fmt.Errorf("worker output upload: api client is not configured")
	}

	ref, ok := selectUploadableOutput(report.Outputs)
	if !ok {
		return fmt.Errorf("worker output upload: no uploadable output with a local file path")
	}
	if _, err := os.Stat(ref.URI); err != nil {
		return fmt.Errorf("worker output upload: output file %q is not readable: %w", ref.URI, err)
	}

	resp, err := w.apiClient.UploadCompletedVideo(ctx, api.UploadCompletedVideoRequest{
		JobID:         pte.JobID,
		AttemptID:     pte.AttemptID,
		WorkerID:      w.config.WorkerID,
		LeaseID:       pte.LeaseID,
		AttemptNumber: pte.AttemptNumber,
		Revision:      pte.JobRevision,
		FilePath:      ref.URI,
	})
	if err != nil {
		return err
	}
	if !resp.OK {
		return fmt.Errorf("worker output upload: master rejected upload for job %s: %s", pte.JobID, resp.Error)
	}

	w.logger.Info("[TASK] Output uploaded for task %s (job=%s artifact=%s upload=%s)",
		pte.TaskID, pte.JobID, resp.ArtifactID, resp.UploadID)
	return nil
}

// selectUploadableOutput picks the canonical upload candidate from
// the executor's outputs:
//
//  1. The first artifact whose Type == "render.output" AND URI != "".
//  2. Otherwise, the first artifact with a non-empty URI.
//  3. If neither matches, returns (zero, false) and the caller
//     surfaces a "no uploadable output with a local file path" error.
func selectUploadableOutput(outputs []executor.ArtifactRef) (executor.ArtifactRef, bool) {
	for _, ref := range outputs {
		if ref.Type == "render.output" && ref.URI != "" {
			return ref, true
		}
	}
	for _, ref := range outputs {
		if ref.URI != "" {
			return ref, true
		}
	}
	return executor.ArtifactRef{}, false
}

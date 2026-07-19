// Package worker — task dispatch to TaskRunner.
//
// task_dispatch.go owns the dispatch path invoked by executeTask:
//   - runJobTask wraps dispatchTaskRunner with a 30-minute per-job
//     timeout context (the canonical worker-side budget).
//   - dispatchTaskRunner resolves the pre-compiled TaskSpec's asset
//     payload via the worker asset bridge, then invokes
//     TaskRunner.Run. Surface area:
//   - wraps taskRunner.Run errors with "taskrunner.Run: %w"
//   - maps a non-success report to a wrapped error that preserves
//     the canonical (executor_key, code, detail) tuple on the wire
//   - preserves context.Canceled identity when report.ErrorCode ==
//     taskrunner.CodeCanceled (operator aborts must NOT be flattened
//     to a generic FAILED attempt on the master)
//   - enforces "every successful output has a non-empty hash" so the
//     executor cannot declare success with empty content hashes
package worker

import (
	"context"
	"fmt"
	"time"

	"velox-worker-agent/internal/taskrunner"
)

// runJobTask executes the actual task via the TaskRunner.
//
// Job timeout is 30 minutes; this matches the worker-side budget
// defined for the canonical task-native dispatch path. The deadline
// cancels the dispatch context but does NOT short-circuit the
// telemetry/result reporting on the caller side (executeTask records
// outcome via recordTaskOutcome regardless).
func (w *Worker) runJobTask(ctx context.Context, pte *PendingTaskExecution) (*taskrunner.TaskExecutionReport, error) {
	w.logger.Info("[JOB] Starting execution: id=%s executor=%s", pte.JobID, pte.ExecutorID)

	jobTimeout := 30 * time.Minute
	jobCtx, cancel := context.WithTimeout(ctx, jobTimeout)
	defer cancel()

	w.logger.Info("[JOB] Phase: registry dispatch for executor=%s", pte.ExecutorID)
	report, err := w.dispatchTaskRunner(jobCtx, pte)
	if err != nil {
		return report, err
	}
	return report, nil
}

// dispatchTaskRunner runs the TaskRunner with the pre-compiled TaskSpec
// from PendingTaskExecution.
//
// The dispatch path:
//  1. If the spec carries a payload, resolve it via the worker asset
//     bridge (resolveTaskAssets). A failure here aborts before the
//     task runner is invoked, so the executor never sees a partially
//     resolved payload.
//  2. Invoke taskRunner.Run with the (possibly resolved) spec.
//  3. On taskRunner.Run error: wrap with "taskrunner.Run: %w" and
//     surface the (possibly partial) report alongside.
//  4. On non-success report: if ErrorCode == taskrunner.CodeCanceled
//     preserve context.Canceled identity on the wire; otherwise wrap
//     with "executor <key> failed: code=%q detail=%q".
//  5. fix/artifact-metadata: validate every output artifact has a
//     non-empty Hash before declaring the task succeeded.
func (w *Worker) dispatchTaskRunner(ctx context.Context, pte *PendingTaskExecution) (*taskrunner.TaskExecutionReport, error) {
	if w.taskRunner == nil {
		return nil, fmt.Errorf("worker has no taskRunner configured; call worker.New with options to install one")
	}

	spec := pte.Spec
	if spec.Payload != nil {
		resolvedPayload, err := w.resolveTaskAssets(ctx, spec.Payload)
		if err != nil {
			return nil, err
		}
		spec.Payload = resolvedPayload
	}

	report, runErr := w.taskRunner.Run(ctx, spec)
	if runErr != nil {
		return &report, fmt.Errorf("taskrunner.Run: %w", runErr)
	}
	if report.Status != "succeeded" {
		// Preserve cancellation identity for the wire result. Wrapping every
		// non-success report as a generic error would turn operator aborts
		// into FAILED attempts on the master.
		if report.ErrorCode == taskrunner.CodeCanceled {
			return &report, context.Canceled
		}
		return &report, fmt.Errorf("executor %s failed: code=%q detail=%q",
			report.ExecutorKey, report.ErrorCode, report.ErrorDetail)
	}
	// fix/artifact-metadata: validate every output artifact has a non-empty
	// Hash before declaring the task succeeded.
	for i, ref := range report.Outputs {
		if ref.Hash == "" {
			return &report, fmt.Errorf("executor %s succeeded but output artifact %d has empty hash (type=%q uri=%q) — executor must provide a content hash for every produced artifact",
				report.ExecutorKey, i, ref.Type, ref.URI)
		}
	}
	return &report, nil
}

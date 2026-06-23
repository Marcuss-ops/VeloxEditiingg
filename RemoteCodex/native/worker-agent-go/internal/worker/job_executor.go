// Package worker provides job processing logic for the worker agent.
//
// PR-3.9 notes:
//
//   - The legacy render / process_video / process_audio helpers
//     (executeWorkflowJob, runRenderJob, runVideoJob, runAudioJob,
//     newVideoWorkflow, videoWorkflow interface) are GONE. They were
//     duplicate routing: every job type now resolves through
//     executor.Registry → TaskRunner, with worker.executeJob keeping
//     only the concurrency / active-jobs / cancel / transport surface.
//   - All legacy job types (render / process_video / process_audio)
//     resolve to the scene.composite.v1 executor that the production
//     composition root (cmd/velox-worker-agent/main.go) registers
//     against the canonical pipeline.Runner.
//   - worker.executeJob no longer imports pkg/video; the
//     pipeline dependency lives in main.go where the SceneComposite
//     adapter is wired.
//
// PR #5 (refactor/task-native-reporting):
//   - health_check removed from runJobTask — health is handled by
//     heartbeat RPC only. No more special-case carve-out.
//   - dispatchTaskRunner receives a pre-compiled executor.TaskSpec
//     (from TaskOffer.task_spec), never reconstructs from job.JobType
//   - job.Parameters.
//   - Returns a typed taskrunner.TaskExecutionReport instead of
//     legacy map[string]interface{}.
//   - executeJob sends typed pb.TaskResult for task-native jobs
//     instead of the legacy map-based JobResult message.
package worker

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"velox-shared/controltransport"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/logger"
)

// executeJob executes a job and reports the result.
// PR #5: for task-native jobs (those carrying _task_id in Parameters), the
// result is reported as a typed TaskResult instead of the legacy JobResult.
func (w *Worker) executeJob(ctx context.Context, job *api.Job) {
	if err := w.concurrencyLimiter.Acquire(ctx, job.JobID, job.Priority); err != nil {
		w.logger.Warn("[CONCURRENCY] Failed to acquire slot for job %s: %v", job.JobID, err)
		return
	}
	defer w.concurrencyLimiter.Release()

	if !w.canTransitionTo(StatusBusy) {
		w.logger.Warn("Cannot accept job: invalid state transition from %s to busy", w.Status())
		return
	}

	activeJob := &ActiveJob{
		Job:       job,
		LeaseID:   resolveLeaseID(job),
		StartedAt: time.Now(),
	}
	w.activeJobsMu.Lock()
	w.activeJobs[job.JobID] = activeJob
	w.activeJobsMu.Unlock()
	defer func() {
		w.activeJobsMu.Lock()
		delete(w.activeJobs, job.JobID)
		w.activeJobsMu.Unlock()
	}()

	jobCtx, jobCancel := context.WithCancel(ctx)
	w.registerJobCancel(job.JobID, jobCancel)
	defer w.unregisterJobCancel(job.JobID)

	telemetry.GetPrometheusMetrics().SetWorkerStatus(w.config.WorkerID, 2)
	telemetry.GetPrometheusMetrics().SetWorkerActiveJobs(w.config.WorkerID, float64(w.concurrencyLimiter.ActiveJobCount()))

	logger.LogJobStart(w.config.WorkerID, job.JobID, job.JobType, job.Priority)

	startTime := time.Now()

	// PR #5: detect task-native dispatch (TaskOffer path) vs legacy JobOffer path.
	taskID, _ := job.Parameters["_task_id"].(string)
	attemptID, _ := job.Parameters["_attempt_id"].(string)
	isTaskNative := taskID != ""

	w.logger.Info("[JOB] Executing job %s via runJobTask (taskNative=%v)", job.JobID, isTaskNative)

	// PR #5: runJobTask returns typed TaskExecutionReport.
	report, execErr := w.runJobTask(jobCtx, job)

	// Upload completed video (legacy path: map-based output).
	// Task-native path skips this — artifacts travel via TaskResult outputs.
	var output map[string]interface{}
	if !isTaskNative && execErr == nil {
		output = w.extractLegacyOutput(report)
		if shouldUploadCompletedVideo(job, output) {
			updatedOutput, upErr := w.uploadCompletedVideo(ctx, job, output)
			if upErr != nil {
				execErr = fmt.Errorf("upload completed video failed: %w", upErr)
			} else {
				output = updatedOutput
				if localVideoPath := extractOutputVideoPath(output); localVideoPath != "" {
					if err := os.Remove(localVideoPath); err != nil {
						w.logger.Warn("[CLEANUP] Failed to remove local video file %s: %v", localVideoPath, err)
					} else {
						w.logger.Info("[CLEANUP] Removed local video file %s after successful upload", localVideoPath)
					}
				}
			}
		}
	}

	duration := time.Since(startTime)

	if execErr != nil {
		logger.LogJobFailedWithType(w.config.WorkerID, job.JobID, job.JobType, execErr, duration)
		w.setStatus(StatusError)
		w.jobsFailed.Add(1)
		telemetry.RecordJobFailure(duration.Milliseconds())
		telemetry.GetPrometheusMetrics().RecordJobRuntime(job.JobType, float64(duration.Milliseconds()))
	} else {
		logger.LogJobSuccess(w.config.WorkerID, job.JobID, job.JobType, duration)
		w.jobsCompleted.Add(1)
		telemetry.RecordJobSuccess(duration.Milliseconds())
		telemetry.GetPrometheusMetrics().RecordJobRuntime(job.JobType, float64(duration.Milliseconds()))
	}

	telemetry.GetPrometheusMetrics().SetWorkerStatus(w.config.WorkerID, 1)
	telemetry.GetPrometheusMetrics().SetWorkerActiveJobs(w.config.WorkerID, float64(w.concurrencyLimiter.ActiveJobCount()))

	submitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ackStartTime := time.Now()

	if isTaskNative {
		// PR #5: send typed TaskResult instead of legacy JobResult.
		w.submitTaskResult(submitCtx, job, taskID, attemptID, report, execErr)
	} else {
		// Legacy path: map-based JobResult.
		w.submitLegacyJobResult(submitCtx, job, output, execErr, startTime)
	}

	telemetry.GetPrometheusMetrics().RecordJobCompleteAck(job.JobType, float64(time.Since(ackStartTime).Milliseconds()))

	if execErr != nil {
		time.Sleep(2 * time.Second)
		if w.canTransitionTo(StatusIdle) {
			w.setStatus(StatusIdle)
		}
	}
}

// submitTaskResult sends a typed pb.TaskResult via the transport (PR #5).
// Includes output_artifacts from the TaskExecutionReport and the lease_id
// from the TaskOffer.
func (w *Worker) submitTaskResult(ctx context.Context, job *api.Job, taskID, attemptID string, report *taskrunner.TaskExecutionReport, execErr error) {
	status := "succeeded"
	var errorCode, errorDetail string
	if execErr != nil {
		status = "failed"
		errorDetail = execErr.Error()
		if report != nil {
			errorCode = report.ErrorCode
		}
	}

	resultPayload := map[string]interface{}{
		"task_id":      taskID,
		"job_id":       job.JobID,
		"attempt_id":   attemptID,
		"status":       status,
		"error_code":   errorCode,
		"error_detail": errorDetail,
		"executor_id":  job.JobType,
		"lease_id":     resolveLeaseID(job),
	}
	if report != nil {
		resultPayload["executor_key"] = report.ExecutorKey
		// Collect output artifacts from the report.
		if len(report.Outputs) > 0 {
			artifacts := make([]map[string]interface{}, 0, len(report.Outputs))
			for _, ref := range report.Outputs {
				artifacts = append(artifacts, map[string]interface{}{
					"uri":  ref.URI,
					"hash": ref.Hash,
					"type": ref.Type,
				})
			}
			resultPayload["output_artifacts"] = artifacts
		}
	}

	resultMsg := controltransport.NewMessageWithPayload(
		controltransport.MsgTaskResult,
		w.config.WorkerID,
		w.config.ProtocolVersion,
		resultPayload,
	)

	if submitErr := w.transport.Send(ctx, resultMsg); submitErr != nil {
		w.logger.Error("[TASK] Failed to submit TaskResult for %s: %v", taskID, submitErr)
	} else {
		artifactCount := 0
		if report != nil {
			artifactCount = len(report.Outputs)
		}
		w.logger.Info("[TASK] TaskResult submitted for %s (status: %s, artifacts: %d)", taskID, status, artifactCount)
	}
}

// submitLegacyJobResult sends the legacy map-based JobResult.
func (w *Worker) submitLegacyJobResult(ctx context.Context, job *api.Job, output map[string]interface{}, execErr error, startTime time.Time) {
	result := &api.JobResult{
		JobID:           job.JobID,
		JobRunID:        resolveJobRunID(job),
		WorkerID:        w.config.WorkerID,
		StartTime:       startTime.Format(time.RFC3339),
		Output:          make(map[string]interface{}),
		ContractVersion: api.ContractVersionV2,
		LeaseID:         resolveLeaseID(job),
		Attempt:         resolveJobAttempt(job),
	}
	if execErr != nil {
		result.Status = "failed"
		result.Error = execErr.Error()
	} else {
		result.Status = "success"
		result.Output = output
	}
	result.EndTime = time.Now().Format(time.RFC3339)

	recentLogs, recentErrors := w.recentLogs.Snapshot(300, 100)
	if result.Output == nil {
		result.Output = make(map[string]interface{})
	}
	result.Output["contract_version"] = api.ContractVersionV2
	result.Output["worker_id"] = w.config.WorkerID
	result.Output["worker_name"] = w.config.WorkerName
	result.Output["worker_status"] = string(w.Status())
	result.Output["worker_recent_logs"] = recentLogs
	result.Output["worker_recent_errors"] = recentErrors
	result.Output["worker_recent_logs_count"] = len(recentLogs)
	result.Output["worker_recent_errors_count"] = len(recentErrors)
	if job != nil {
		result.Output["job_type"] = job.JobType
		result.Output["job_priority"] = job.Priority
		result.Output["job_run_id"] = resolveJobRunID(job)
		result.Output["lease_id"] = resolveLeaseID(job)
		result.Output["attempt"] = resolveJobAttempt(job)
		result.LeaseID = resolveLeaseID(job)
	}

	resultPayload := map[string]interface{}{
		"job_id":           result.JobID,
		"job_run_id":       result.JobRunID,
		"status":           result.Status,
		"error":            result.Error,
		"start_time":       result.StartTime,
		"end_time":         result.EndTime,
		"lease_id":         result.LeaseID,
		"attempt":          result.Attempt,
		"output":           result.Output,
		"contract_version": result.ContractVersion,
	}

	resultMsg := controltransport.NewMessageWithPayload(
		controltransport.MsgJobResult,
		w.config.WorkerID,
		w.config.ProtocolVersion,
		resultPayload,
	)

	if submitErr := w.transport.Send(ctx, resultMsg); submitErr != nil {
		w.logger.Error("Failed to submit job result for %s: %v", job.JobID, submitErr)
	} else {
		w.logger.Info("[JOB] Result submitted and completed for %s (status: %s)", job.JobID, result.Status)
	}
}

// extractLegacyOutput converts a TaskExecutionReport into the legacy
// map[string]interface{} shape expected by the upload pipeline.
func (w *Worker) extractLegacyOutput(report *taskrunner.TaskExecutionReport) map[string]interface{} {
	if report == nil {
		return nil
	}
	out := map[string]interface{}{
		"status":       "completed",
		"job_id":       report.JobID,
		"executor_id":  report.ExecutorID,
		"executor_key": report.ExecutorKey,
		"phase_count":  len(report.PhaseMarkers),
	}
	if len(report.Outputs) > 0 {
		out["output_path"] = report.Outputs[0].URI
		out["output_hash"] = report.Outputs[0].Hash
	}
	return out
}

func shouldUploadCompletedVideo(job *api.Job, output map[string]interface{}) bool {
	if job == nil {
		return false
	}
	return strings.TrimSpace(extractOutputVideoPath(output)) != ""
}

// runJobTask executes the actual job task.
//
// PR #5: health_check removed — health is handled exclusively by the
// heartbeat RPC. Every job type now resolves through executor.Registry →
// TaskRunner. The single dispatch surface has no carve-outs.
//
// PR #5: returns a typed *taskrunner.TaskExecutionReport instead of
// legacy map[string]interface{}. The caller (executeJob) decides
// whether to send a typed TaskResult or a legacy JobResult based on
// whether the job was dispatched via TaskOffer.
func (w *Worker) runJobTask(ctx context.Context, job *api.Job) (*taskrunner.TaskExecutionReport, error) {
	w.logger.Info("[JOB] Starting execution: id=%s type=%s", job.JobID, job.JobType)

	jobTimeout := 30 * time.Minute
	if job.TimeoutSecs > 0 {
		jobTimeout = time.Duration(job.TimeoutSecs) * time.Second
	}
	jobCtx, cancel := context.WithTimeout(ctx, jobTimeout)
	defer cancel()

	w.logger.Info("[JOB] Phase: registry dispatch for type=%s", job.JobType)
	report, err := w.dispatchTaskRunner(jobCtx, job)
	if err != nil {
		return report, err
	}
	return report, nil
}

// dispatchTaskRunner runs the TaskRunner with a pre-compiled or
// job-derived TaskSpec.
//
// PR #5 changes:
//   - Task-native path: the TaskSpec arrives pre-compiled in
//     job.Parameters["task_spec"] (set by the MsgTaskOffer handler).
//     The executor_id comes from TaskOffer.executor_id directly.
//   - Legacy path: the TaskSpec is derived from job.JobType +
//     job.Parameters (verbatim), with version always 1.
//
// Returns the typed *taskrunner.TaskExecutionReport instead of
// legacy map[string]interface{}.
func (w *Worker) dispatchTaskRunner(ctx context.Context, job *api.Job) (*taskrunner.TaskExecutionReport, error) {
	if w.taskRunner == nil {
		return nil, fmt.Errorf("worker has no taskRunner configured; call worker.New with options to install one")
	}

	// PR #5: check for pre-compiled TaskSpec from TaskOffer.
	var spec executor.TaskSpec
	if specPayload, ok := job.Parameters["task_spec"].(map[string]interface{}); ok && len(specPayload) > 0 {
		// Task-native path: spec arrives pre-compiled from master.
		spec = executor.TaskSpec{
			Version:    1,
			JobID:      job.JobID,
			ExecutorID: strings.TrimSpace(job.JobType),
			Payload:    specPayload,
		}
	} else {
		// Legacy path: derive spec from job fields.
		spec = executor.TaskSpec{
			Version:    1,
			JobID:      job.JobID,
			ExecutorID: strings.TrimSpace(job.JobType),
			Payload:    job.Parameters,
		}
	}

	report, runErr := w.taskRunner.Run(ctx, spec)
	if runErr != nil {
		return &report, fmt.Errorf("taskrunner.Run: %w", runErr)
	}
	if report.Status != "succeeded" {
		return &report, fmt.Errorf("executor %s failed: code=%q detail=%q",
			report.ExecutorKey, report.ErrorCode, report.ErrorDetail)
	}
	return &report, nil
}

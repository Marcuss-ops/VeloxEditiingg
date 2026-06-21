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
package worker

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"velox-shared/controltransport"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/logger"
)

// executeJob executes a job and reports the result.
// executeJob esegue un job dall'acquisizione del concurrency slot fino alla
// notifica del risultato al master. Gestisce:
//   - Acquisizione concurrency slot
//   - Transizione di stato (idle → busy → idle/error)
//   - Upload del video completato
//   - Submit del risultato (success/failure) al master
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

	// Register in activeJobs map for multi-job tracking (status derived from map length)
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

	// Create cancellable context and register it for cancel_job command
	jobCtx, jobCancel := context.WithCancel(ctx)
	w.registerJobCancel(job.JobID, jobCancel)
	defer w.unregisterJobCancel(job.JobID)

	telemetry.GetPrometheusMetrics().SetWorkerStatus(w.config.WorkerID, 2)
	telemetry.GetPrometheusMetrics().SetWorkerActiveJobs(w.config.WorkerID, float64(w.concurrencyLimiter.ActiveJobCount()))

	logger.LogJobStart(w.config.WorkerID, job.JobID, job.JobType, job.Priority)

	startTime := time.Now()
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

	var output map[string]interface{}
	var execErr error

	w.logger.Info("[JOB] Executing job %s via runJobTask", job.JobID)
	output, execErr = w.runJobTask(jobCtx, job)

	if execErr == nil && shouldUploadCompletedVideo(job, output) {
		updatedOutput, upErr := w.uploadCompletedVideo(ctx, job, output)
		if upErr != nil {
			execErr = fmt.Errorf("upload completed video failed: %w", upErr)
		} else {
			output = updatedOutput
			// Clean up local completed video file to conserve disk space
			if localVideoPath := extractOutputVideoPath(output); localVideoPath != "" {
				if err := os.Remove(localVideoPath); err != nil {
					w.logger.Warn("[CLEANUP] Failed to remove local video file %s: %v", localVideoPath, err)
				} else {
					w.logger.Info("[CLEANUP] Removed local video file %s after successful upload", localVideoPath)
				}
			}
		}
	}

	duration := time.Since(startTime)

	if execErr != nil {
		logger.LogJobFailedWithType(w.config.WorkerID, job.JobID, job.JobType, execErr, duration)
		result.Status = "failed"
		result.Error = execErr.Error()
		w.setStatus(StatusError)
		w.jobsFailed.Add(1)
		telemetry.RecordJobFailure(duration.Milliseconds())
		telemetry.GetPrometheusMetrics().RecordJobRuntime(job.JobType, float64(duration.Milliseconds()))
	} else {
		logger.LogJobSuccess(w.config.WorkerID, job.JobID, job.JobType, duration)
		result.Status = "success"
		result.Output = output
		w.jobsCompleted.Add(1)
		telemetry.RecordJobSuccess(duration.Milliseconds())
		telemetry.GetPrometheusMetrics().RecordJobRuntime(job.JobType, float64(duration.Milliseconds()))
	}

	telemetry.GetPrometheusMetrics().SetWorkerStatus(w.config.WorkerID, 1)
	telemetry.GetPrometheusMetrics().SetWorkerActiveJobs(w.config.WorkerID, float64(w.concurrencyLimiter.ActiveJobCount()))

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

	submitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ackStartTime := time.Now()

	// Send job result via transport (handles both submit and complete)
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

	if submitErr := w.transport.Send(submitCtx, resultMsg); submitErr != nil {
		w.logger.Error("Failed to submit job result for %s: %v", job.JobID, submitErr)
	} else {
		w.logger.Info("[JOB] Result submitted and completed for %s (status: %s)", job.JobID, result.Status)
		telemetry.GetPrometheusMetrics().RecordJobCompleteAck(job.JobType, float64(time.Since(ackStartTime).Milliseconds()))
	}

	if execErr != nil {
		time.Sleep(2 * time.Second)
		if w.canTransitionTo(StatusIdle) {
			w.setStatus(StatusIdle)
		}
	}
}

func shouldUploadCompletedVideo(job *api.Job, output map[string]interface{}) bool {
	if job == nil {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(job.JobType), "health_check") {
		return false
	}
	return strings.TrimSpace(extractOutputVideoPath(output)) != ""
}

// runJobTask executes the actual job task.
//
// PR-3.9 invariant: there is exactly ONE dispatch surface. Every job
// type resolves through executor.Registry → TaskRunner, except for
// health_check which is intentionally OUT of the executor catalog
// (master polls the worker's health at registration time and the
// response payload is intentionally minimal — putting it in the
// catalog would conflict with master-side health semantics).
//
// Hard rule: do NOT reintroduce per-job-type switch arms here. New
// job types register an Executor; the registry does the routing.
// scripts/ci/check-architecture.sh enforces this.
func (w *Worker) runJobTask(ctx context.Context, job *api.Job) (map[string]interface{}, error) {
	w.logger.Info("[JOB] Starting execution: id=%s type=%s", job.JobID, job.JobType)

	jobTimeout := 30 * time.Minute
	if job.TimeoutSecs > 0 {
		jobTimeout = time.Duration(job.TimeoutSecs) * time.Second
	}
	jobCtx, cancel := context.WithTimeout(ctx, jobTimeout)
	defer cancel()

	switch job.JobType {
	case "health_check":
		w.logger.Info("[JOB] Phase: health_check")
		return map[string]interface{}{"status": "healthy", "worker_id": w.config.WorkerID}, nil
	default:
		w.logger.Info("[JOB] Phase: registry dispatch for type=%s", job.JobType)
		return w.dispatchTaskRunner(jobCtx, job)
	}
}

// dispatchTaskRunner resolves job.JobType as the executor ID with
// version 1 and runs the corresponding Executor through the worker's
// TaskRunner (PR-3.8). It is the single dispatch entry point after
// the health_check carve-out and the canonical surface for every
// future executor registration.
//
// Mapping rules:
//
//   - job.JobType      → executor.TaskSpec.ExecutorID (trimmed).
//   - Version          → always 1 today; promoted to a per-job value
//     once the master starts announcing ExecutorIDs
//     with embedded versions (PR-1 contracts).
//   - job.Parameters   → executor.TaskSpec.Payload (verbatim).
//
// Output shape: a single map[string]interface{} matching the legacy
// executeWorkflowJob contract so the downstream upload pipeline
// (shouldUploadCompletedVideo → uploadCompletedVideo) continues to
// operate without modification. PR-3.9 promotes multi-output tasks
// to publish every ArtifactRef via the master-side transport.
//
// Errors:
//   - Nil taskRunner — defensive-only; New() always builds one, this
//     surfaces worker-bootstrap bugs instead of silently routing
//     through the registry miss path.
//   - TaskRunner.Run returning (report, err) at the program-fault
//     level — wrapped with %w.
//   - report.Status != "succeeded" — formatted with code+detail so
//     executeJob's execErr path surfaces a sane error message.
func (w *Worker) dispatchTaskRunner(ctx context.Context, job *api.Job) (map[string]interface{}, error) {
	if w.taskRunner == nil {
		return nil, fmt.Errorf("worker has no taskRunner configured; call worker.New with options to install one")
	}
	spec := executor.TaskSpec{
		Version:    1,
		JobID:      job.JobID,
		ExecutorID: strings.TrimSpace(job.JobType),
		Payload:    job.Parameters,
	}
	report, runErr := w.taskRunner.Run(ctx, spec)
	if runErr != nil {
		return nil, fmt.Errorf("taskrunner.Run: %w", runErr)
	}
	if report.Status != "succeeded" {
		// Registry miss maps to CodeUnsupportedExecutor via the runner;
		// executor-side failures land here with their canonical code.
		return nil, fmt.Errorf("executor %s failed: code=%q detail=%q",
			report.ExecutorKey, report.ErrorCode, report.ErrorDetail)
	}
	out := map[string]interface{}{
		"status":       "completed",
		"job_id":       job.JobID,
		"executor_id":  job.JobType,
		"executor_key": report.ExecutorKey,
		"phase_count":  len(report.PhaseMarkers),
	}
	if len(report.Outputs) > 0 {
		// Preserve legacy "output_path" semantics so the downstream
		// upload pipeline in executeJob (shouldUploadCompletedVideo
		// → uploadCompletedVideo) continues to operate without
		// modification. The richer ArtifactRefs are already inside
		// report.Outputs and travel back via the canonical
		// TaskExecutionReport on the master transport (PR-4).
		out["output_path"] = report.Outputs[0].URI
		out["output_hash"] = report.Outputs[0].Hash
	}
	return out, nil
}

// Package worker provides job processing logic for the worker agent.
package worker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"velox-shared/controltransport"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
	"velox-worker-agent/pkg/video"
)

type videoWorkflow interface {
	SetProgressCallback(func(percent, scene, total int, stage string))
	ProcessSingleVideo(ctx context.Context, input renderJobParams, statusCallback func(string, bool)) (string, error)
}

var newVideoWorkflow = func(cfg *config.WorkerConfig, log *logger.Logger) videoWorkflow {
	return video.NewVideoGenerationWorkflow(cfg, log)
}

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
// PR-3.8 dispatch surface: the legacy render / process_video /
// process_audio switch was REPLACED with a single TaskRunner
// dispatch that resolves the executor by job.JobType through the
// worker's executor.Registry. Health-check stays as an explicit,
// registry-free branch (out of PR-3.5 / PR-3.8 scope: master polls
// the worker's health at registration time and the response payload
// is intentionally minimal — adding it to the executor catalog would
// conflict with master-side health semantics).
//
// "render" / "process_video" / "process_audio" still resolve via the
// registry; existing jobs keep working once the corresponding
// executors are registered (PR-3.9 lands the scene.composite.v1
// real implementation).
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
// (shouldUploadCompletedVideo → uploadCompletedVideo) works without
// modification. PR-3.9 promotes multi-output tasks to publish every
// ArtifactRef via the master-side transport.
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
		// modification. The richer ArtifactRefs are surfaced to the
		// master via the TaskExecutionReport (PR-3.9 wires this).
		out["output_path"] = report.Outputs[0].URI
		out["output_hash"] = report.Outputs[0].Hash
	}
	return out, nil
}

// executeWorkflowJob is a shared implementation for render/video/audio jobs.
func (w *Worker) executeWorkflowJob(ctx context.Context, job *api.Job, jobLabel string, defaultExt string) (map[string]interface{}, error) {
	p := extractRenderJobParams(job.Parameters)
	resolvedAudioPath, err := w.resolveVoiceoverAudioPath(ctx, p.AudioPath, job.Parameters)
	if err != nil {
		return nil, err
	}
	p.AudioPath = resolvedAudioPath

	// Inject asset cache dir from worker config if not set in job params
	assetCacheDir := strings.TrimSpace(p.AssetCacheDir)
	if assetCacheDir == "" {
		assetCacheDir = strings.TrimSpace(w.config.AssetCacheDir)
	}
	p.AssetCacheDir = assetCacheDir
	if assetCacheDir != "" {
		if err := os.MkdirAll(assetCacheDir, 0755); err == nil {
			w.logger.Info("[CACHE] Asset cache enabled: %s", assetCacheDir)
		} else {
			w.logger.Warn("[CACHE] Cannot create asset cache dir %s: %v, caching disabled", assetCacheDir, err)
			p.AssetCacheDir = ""
		}
	}

	wfLogger := logger.New(logger.DebugLevel, os.Stdout)
	wfLogger.SetPrefix("[WORKFLOW]")

	workflow := newVideoWorkflow(&config.WorkerConfig{
		WorkerID:   w.config.WorkerID,
		WorkerName: w.config.WorkerName,
		MasterURL:  w.config.MasterURL,
		LogLevel:   w.config.LogLevel,
	}, wfLogger)

	// Capture jobID for per-job progress tracking via activeJobs map
	jobID := job.JobID
	workflow.SetProgressCallback(func(percent, scene, total int, stage string) {
		w.activeJobsMu.Lock()
		if aj, ok := w.activeJobs[jobID]; ok {
			aj.Progress.Percent = int32(percent)
			aj.Progress.Scene = int32(scene)
			aj.Progress.TotalScenes = int32(total)
			aj.Progress.Stage = stage
		}
		w.activeJobsMu.Unlock()
	})

	outputPath := p.OutputPath
	if outputPath == "" {
		outputPath = fmt.Sprintf("/tmp/velox/output/%s.%s", job.JobID, defaultExt)
	}
	p.OutputPath = outputPath
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return nil, fmt.Errorf("create output directory %s: %w", filepath.Dir(outputPath), err)
	}

	statusCallback := func(msg string, isError bool) {
		if isError {
			w.logger.Error("%s", msg)
		} else {
			w.logger.Info("%s", msg)
		}
	}

	resultPath, err := workflow.ProcessSingleVideo(ctx,
		p,
		statusCallback)

	if err != nil {
		return nil, fmt.Errorf("%s job failed: %w", jobLabel, err)
	}
	return map[string]interface{}{
		"status": "completed", "job_id": job.JobID, "output_path": resultPath,
	}, nil
}

func (w *Worker) runRenderJob(ctx context.Context, job *api.Job) (map[string]interface{}, error) {
	return w.executeWorkflowJob(ctx, job, "render", "mp4")
}

func (w *Worker) runVideoJob(ctx context.Context, job *api.Job) (map[string]interface{}, error) {
	return w.executeWorkflowJob(ctx, job, "video", "mp4")
}

func (w *Worker) runAudioJob(ctx context.Context, job *api.Job) (map[string]interface{}, error) {
	return w.executeWorkflowJob(ctx, job, "audio", "mp3")
}

// Package worker provides job processing logic for the worker agent.
package worker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
	"velox-worker-agent/pkg/video"
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

	w.mu.Lock()
	w.currentJob = job
	w.status = StatusBusy
	w.mu.Unlock()

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

	w.mu.Lock()
	w.currentJob = nil
	w.progressPercent.Store(0)
	w.progressScene.Store(0)
	w.progressTotal.Store(0)
	w.progressStage.Store("idle")
	duration := time.Since(startTime)

	if execErr != nil {
		logger.LogJobFailedWithType(w.config.WorkerID, job.JobID, job.JobType, execErr, duration)
		result.Status = "failed"
		result.Error = execErr.Error()
		w.status = StatusError
		w.jobsFailed.Add(1)
		telemetry.RecordJobFailure(duration.Milliseconds())
		telemetry.GetPrometheusMetrics().RecordJobRuntime(job.JobType, float64(duration.Milliseconds()))
	} else {
		logger.LogJobSuccess(w.config.WorkerID, job.JobID, job.JobType, duration)
		result.Status = "success"
		result.Output = output
		w.status = StatusIdle
		w.jobsCompleted.Add(1)
		telemetry.RecordJobSuccess(duration.Milliseconds())
		telemetry.GetPrometheusMetrics().RecordJobRuntime(job.JobType, float64(duration.Milliseconds()))
	}
	w.mu.Unlock()

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
	}

	submitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ackStartTime := time.Now()
	var submitErr error
	if w.config.UseV2Endpoints != nil && *w.config.UseV2Endpoints {
		submitErr = w.apiClient.SubmitJobResultV2(submitCtx, job.JobID, result)
	} else {
		submitErr = w.apiClient.SubmitJobResult(submitCtx, result)
	}
	if submitErr != nil {
		w.logger.Error("Failed to submit job result for %s: %v", job.JobID, submitErr)
	} else {
		w.logger.Debug("Job result submitted: %s (status: %s)", job.JobID, result.Status)
		telemetry.GetPrometheusMetrics().RecordJobCompleteAck(job.JobType, float64(time.Since(ackStartTime).Milliseconds()))
		completeCtx, completeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		var completeErr error
		if w.config.UseV2Endpoints != nil && *w.config.UseV2Endpoints {
			completeErr = w.apiClient.CompleteJobV2(completeCtx, job.JobID, w.config.WorkerID, resolveLeaseID(job), resolveJobAttempt(job))
		} else {
			completeErr = w.apiClient.CompleteJob(completeCtx, job.JobID, w.config.WorkerID, resolveLeaseID(job), resolveJobAttempt(job))
		}
		if completeErr != nil {
			w.logger.Warn("[JOB] Complete notification failed for %s: %v", job.JobID, completeErr)
		} else {
			w.logger.Info("[JOB] Complete notification sent for %s", job.JobID)
		}
		completeCancel()
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
// runJobTask seleziona ed esegue il task appropriato in base a job.JobType.
// Job types supportati: render, process_video, process_audio, health_check.
func (w *Worker) runJobTask(ctx context.Context, job *api.Job) (map[string]interface{}, error) {
	w.logger.Info("[JOB] Starting execution: id=%s type=%s", job.JobID, job.JobType)

	jobTimeout := 30 * time.Minute
	if job.TimeoutSecs > 0 {
		jobTimeout = time.Duration(job.TimeoutSecs) * time.Second
	}
	jobCtx, cancel := context.WithTimeout(ctx, jobTimeout)
	defer cancel()

	switch job.JobType {
	case "render":
		w.logger.Info("[JOB] Phase: render pipeline")
		return w.runRenderJob(jobCtx, job)
	case "process_video":
		w.logger.Info("[JOB] Phase: video pipeline")
		return w.runVideoJob(jobCtx, job)
	case "process_audio":
		w.logger.Info("[JOB] Phase: audio pipeline")
		return w.runAudioJob(jobCtx, job)
	case "health_check":
		w.logger.Info("[JOB] Phase: health_check")
		return map[string]interface{}{"status": "healthy", "worker_id": w.config.WorkerID}, nil
	default:
		return nil, fmt.Errorf("unknown job type: %s", job.JobType)
	}
}

// executeWorkflowJob is a shared implementation for render/video/audio jobs.
func (w *Worker) executeWorkflowJob(ctx context.Context, job *api.Job, jobLabel string, defaultExt string) (map[string]interface{}, error) {
	p := extractRenderJobParams(job.Parameters)
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

	workflow := video.NewVideoGenerationWorkflow(&config.WorkerConfig{
		WorkerID:   w.config.WorkerID,
		WorkerName: w.config.WorkerName,
		MasterURL:  w.config.MasterURL,
		LogLevel:   w.config.LogLevel,
	}, wfLogger)

	workflow.SetProgressCallback(func(percent, scene, total int, stage string) {
		w.progressPercent.Store(int32(percent))
		w.progressScene.Store(int32(scene))
		w.progressTotal.Store(int32(total))
		w.progressStage.Store(stage)
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

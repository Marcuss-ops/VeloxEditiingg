// Package worker provides job processing logic for the worker agent.
//
// PR-3.9 notes:
//
//   - The legacy render / process_video / process_audio helpers
//     (executeWorkflowJob, runRenderJob, runVideoJob, runAudioJob,
//     newVideoWorkflow, videoWorkflow interface) are GONE. They were
//     duplicate routing: every job type now resolves through
//     executor.Registry → TaskRunner, with worker.executeTask keeping
//     only the concurrency / active-jobs / cancel / transport surface.
//   - All legacy job types (render / process_video / process_audio)
//     resolve to the scene.composite.v1 executor that the production
//     composition root (cmd/velox-worker-agent/main.go) registers
//     against the canonical pipeline.Runner.
//   - worker.executeTask no longer imports pkg/video; the
//     pipeline dependency lives in main.go where the SceneComposite
//     adapter is wired.
//
// fix/remove-legacy-execution-path: the legacy JobOffer → JobResult path
// has been fully removed. Every execution now flows through the task-native
// dispatch (TaskOffer → executeTask → TaskResult).
// The _task_id / _attempt_id hidden keys in Parameters are gone — taskID
// and attemptID flow as explicit parameters from the caller.
package worker

import (
	"context"
	"fmt"
	"strings"
	"time"

	"velox-shared/controltransport"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/logger"
)

// executeTask executes a task and reports the result via typed TaskResult.
// taskID and attemptID are passed explicitly by the caller (receiveLoop
// TaskLeaseGranted handler) — they no longer live as hidden keys in
// job.Parameters.
func (w *Worker) executeTask(ctx context.Context, job *api.Job, taskID, attemptID string) {
	if err := w.concurrencyLimiter.Acquire(ctx, job.JobID, job.Priority); err != nil {
		w.logger.Warn("[CONCURRENCY] Failed to acquire slot for job %s: %v", job.JobID, err)
		return
	}
	defer w.concurrencyLimiter.Release()

	if !w.canTransitionTo(StatusBusy) {
		w.logger.Warn("Cannot accept task: invalid state transition from %s to busy", w.Status())
		return
	}

	activeTask := &ActiveTaskExecution{
		TaskID:    taskID,
		AttemptID: attemptID,
		JobID:     job.JobID,
		Job:       job,
		LeaseID:   resolveLeaseID(job),
		StartedAt: time.Now(),
	}
	w.activeTasksMu.Lock()
	w.activeTasks[taskID] = activeTask
	w.taskIDsByJob[job.JobID] = append(w.taskIDsByJob[job.JobID], taskID)
	w.activeTasksMu.Unlock()
	defer func() {
		w.activeTasksMu.Lock()
		delete(w.activeTasks, taskID)
		taskIDs := w.taskIDsByJob[job.JobID]
		for i, tid := range taskIDs {
			if tid == taskID {
				w.taskIDsByJob[job.JobID] = append(taskIDs[:i], taskIDs[i+1:]...)
				break
			}
		}
		if len(w.taskIDsByJob[job.JobID]) == 0 {
			delete(w.taskIDsByJob, job.JobID)
		}
		w.activeTasksMu.Unlock()
	}()

	jobCtx, jobCancel := context.WithCancel(ctx)
	activeTask.Cancel = jobCancel
	defer jobCancel()

	telemetry.GetPrometheusMetrics().SetWorkerStatus(w.config.WorkerID, 2)
	telemetry.GetPrometheusMetrics().SetWorkerActiveJobs(w.config.WorkerID, float64(w.concurrencyLimiter.ActiveJobCount()))

	logger.LogJobStart(w.config.WorkerID, job.JobID, job.JobType, job.Priority)

	startTime := time.Now()

	w.logger.Info("[TASK] Executing task %s (job=%s attempt=%s)", taskID, job.JobID, attemptID)

	// runJobTask returns typed TaskExecutionReport.
	report, execErr := w.runJobTask(jobCtx, job)

	duration := time.Since(startTime)

	if execErr != nil {
		logger.LogJobFailedWithType(w.config.WorkerID, job.JobID, job.JobType, execErr, duration)
		w.setStatus(StatusError)
		w.tasksFailed.Add(1)
		telemetry.RecordJobFailure(duration.Milliseconds())
		telemetry.GetPrometheusMetrics().RecordJobRuntime(job.JobType, float64(duration.Milliseconds()))
	} else {
		logger.LogJobSuccess(w.config.WorkerID, job.JobID, job.JobType, duration)
		w.tasksCompleted.Add(1)
		telemetry.RecordJobSuccess(duration.Milliseconds())
		telemetry.GetPrometheusMetrics().RecordJobRuntime(job.JobType, float64(duration.Milliseconds()))
	}

	telemetry.GetPrometheusMetrics().SetWorkerStatus(w.config.WorkerID, 1)
	telemetry.GetPrometheusMetrics().SetWorkerActiveJobs(w.config.WorkerID, float64(w.concurrencyLimiter.ActiveJobCount()))

	submitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ackStartTime := time.Now()

	// Always send typed TaskResult — the legacy JobResult path is gone.
	w.submitTaskResult(submitCtx, job, taskID, attemptID, report, execErr)

	telemetry.GetPrometheusMetrics().RecordJobCompleteAck(job.JobType, float64(time.Since(ackStartTime).Milliseconds()))

	if execErr != nil {
		time.Sleep(2 * time.Second)
		if w.canTransitionTo(StatusIdle) {
			w.setStatus(StatusIdle)
		}
	}
}

// submitTaskResult sends a typed pb.TaskResult via the transport.
// Includes output_artifacts, execution_metrics, and phase_markers from the
// TaskExecutionReport.
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
				// fix/artifact-format-alignment: master expects artifact_id,
				// artifact_type, artifact_path, size_bytes, sha256 — not uri/hash/type.
				artifacts = append(artifacts, map[string]interface{}{
					"artifact_id":   ref.Hash,
					"artifact_type": ref.Type,
					"artifact_path": ref.URI,
					"size_bytes":    int64(0),
					"sha256":        ref.Hash,
				})
			}
			resultPayload["output_artifacts"] = artifacts
		}
		// fix/execution-metrics-emit: populate typed execution_metrics from the
		// report so the master's ingestion service persists them alongside the
		// atomic Task+Attempt close.
		if report.TypedMetrics != nil {
			m := report.TypedMetrics
			resultPayload["execution_metrics"] = map[string]interface{}{
				"input_bytes":              m.InputBytes,
				"output_bytes":             m.OutputBytes,
				"bytes_from_drive":         m.BytesFromDrive,
				"bytes_from_blobstore":     m.BytesFromBlobstore,
				"bytes_from_local_cache":   m.BytesFromLocalCache,
				"cpu_time_ms":              m.CpuTimeMs,
				"peak_rss_bytes":           m.PeakRssBytes,
				"frames_decoded":           m.FramesDecoded,
				"frames_composited":        m.FramesComposited,
				"frames_encoded":           m.FramesEncoded,
				"ffmpeg_speed_ratio":       m.FfmpegSpeedRatio,
				"encode_passes":            m.EncodePasses,
				"final_concat_stream_copy": m.FinalConcatStreamCopy,
				"concat_mode":              m.ConcatMode,
				"cpu_price_per_second":     m.CpuPricePerSecond,
				"storage_price_per_gb":     m.StoragePricePerGb,
				"network_price_per_gb":     m.NetworkPricePerGb,
			}
		}
		// fix/phase-markers-emit: populate canonical phase_markers for the
		// master's observability aggregation (scorecard v1).
		if len(report.PhaseMarkers) > 0 {
			markers := make([]map[string]interface{}, 0, len(report.PhaseMarkers))
			for _, pm := range report.PhaseMarkers {
				markers = append(markers, map[string]interface{}{
					"name":         pm.Name,
					"started_at":   pm.StartedAt.Format(time.RFC3339),
					"completed_at": pm.CompletedAt.Format(time.RFC3339),
					"status":       pm.Status,
					"notes":        pm.Notes,
				})
			}
			resultPayload["phase_markers"] = markers
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

// runJobTask executes the actual job task.
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
func (w *Worker) dispatchTaskRunner(ctx context.Context, job *api.Job) (*taskrunner.TaskExecutionReport, error) {
	if w.taskRunner == nil {
		return nil, fmt.Errorf("worker has no taskRunner configured; call worker.New with options to install one")
	}

	var spec executor.TaskSpec
	// fix/executor-version: read master-supplied executor_version from the
	// TaskOffer (stored in _executor_version). Default to 1 when not supplied.
	specVersion := 1
	if v, ok := job.Parameters["_executor_version"].(int); ok && v > 0 {
		specVersion = v
	}
	if specPayload, ok := job.Parameters["task_spec"].(map[string]interface{}); ok && len(specPayload) > 0 {
		// Task-native path: spec arrives pre-compiled from master.
		spec = executor.TaskSpec{
			Version:    specVersion,
			JobID:      job.JobID,
			ExecutorID: strings.TrimSpace(job.JobType),
			Payload:    specPayload,
		}
	} else {
		// Legacy path: derive spec from job fields.
		spec = executor.TaskSpec{
			Version:    specVersion,
			JobID:      job.JobID,
			ExecutorID: job.JobType,
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

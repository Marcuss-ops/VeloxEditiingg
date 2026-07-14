// Package worker provides task processing logic for the worker agent.
// Every execution flows through the task-native dispatch:
// TaskOffer → executeTask → TaskResult, using PendingTaskExecution
// as the canonical typed carrier for all identity and spec data.
package worker

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/logger"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// executeTask executes a task and reports the result via typed TaskResult.
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

	activeTask := &ActiveTaskExecution{
		TaskID:    taskID,
		AttemptID: attemptID,
		JobID:     pte.JobID,
		Task:      pte,
		LeaseID:   pte.LeaseID,
		StartedAt: time.Now(),
	}
	w.activeTasksMu.Lock()
	w.activeTasks[taskID] = activeTask
	w.taskIDsByJob[pte.JobID] = append(w.taskIDsByJob[pte.JobID], taskID)
	w.activeTasksMu.Unlock()
	defer func() {
		w.activeTasksMu.Lock()
		delete(w.activeTasks, taskID)
		taskIDs := w.taskIDsByJob[pte.JobID]
		for i, tid := range taskIDs {
			if tid == taskID {
				w.taskIDsByJob[pte.JobID] = append(taskIDs[:i], taskIDs[i+1:]...)
				break
			}
		}
		if len(w.taskIDsByJob[pte.JobID]) == 0 {
			delete(w.taskIDsByJob, pte.JobID)
		}
		w.activeTasksMu.Unlock()
	}()

	jobCtx, jobCancel := context.WithCancel(ctx)
	activeTask.Cancel = jobCancel
	defer jobCancel()

	telemetry.GetPrometheusMetrics().SetWorkerStatus(w.config.WorkerID, 2)
	telemetry.GetPrometheusMetrics().SetWorkerActiveJobs(w.config.WorkerID, float64(w.concurrencyLimiter.ActiveJobCount()))

	logger.LogJobStart(w.config.WorkerID, pte.JobID, pte.ExecutorID, 0)

	startTime := time.Now()

	w.logger.Info("[TASK] Executing task %s (job=%s attempt=%s)", taskID, pte.JobID, attemptID)

	report, execErr := w.runJobTask(jobCtx, pte)

	duration := time.Since(startTime)

	if execErr != nil {
		logger.LogJobFailedWithType(w.config.WorkerID, pte.JobID, pte.ExecutorID, execErr, duration)
		w.setStatus(StatusError)
		w.tasksFailed.Add(1)
		telemetry.RecordJobFailure(duration.Milliseconds())
		telemetry.GetPrometheusMetrics().RecordJobRuntime(pte.ExecutorID, float64(duration.Milliseconds()))
	} else {
		if uploadErr := w.uploadTaskOutputs(jobCtx, pte, report); uploadErr != nil {
			execErr = fmt.Errorf("upload task outputs: %w", uploadErr)
			logger.LogJobFailedWithType(w.config.WorkerID, pte.JobID, pte.ExecutorID, execErr, duration)
			w.setStatus(StatusError)
			w.tasksFailed.Add(1)
			telemetry.RecordJobFailure(duration.Milliseconds())
			telemetry.GetPrometheusMetrics().RecordJobRuntime(pte.ExecutorID, float64(duration.Milliseconds()))
		} else {
			logger.LogJobSuccess(w.config.WorkerID, pte.JobID, pte.ExecutorID, duration)
			w.tasksCompleted.Add(1)
			telemetry.RecordJobSuccess(duration.Milliseconds())
			telemetry.GetPrometheusMetrics().RecordJobRuntime(pte.ExecutorID, float64(duration.Milliseconds()))
		}
	}

	telemetry.GetPrometheusMetrics().SetWorkerStatus(w.config.WorkerID, 1)
	telemetry.GetPrometheusMetrics().SetWorkerActiveJobs(w.config.WorkerID, float64(w.concurrencyLimiter.ActiveJobCount()))

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

// submitTaskResult sends a typed pb.TaskResult via the transport.
func (w *Worker) submitTaskResult(ctx context.Context, pte *PendingTaskExecution, taskID, attemptID string, report *taskrunner.TaskExecutionReport, execErr error) {
	status := "succeeded"
	var errorCode, errorDetail string
	if execErr != nil {
		status = "failed"
		errorDetail = execErr.Error()
		if report != nil {
			errorCode = report.ErrorCode
		}
	}

	tr := &pb.TaskResult{
		TaskId:        taskID,
		JobId:         pte.JobID,
		AttemptId:     attemptID,
		Status:        status,
		ErrorCode:     errorCode,
		ErrorDetail:   errorDetail,
		ExecutorId:    pte.ExecutorID,
		LeaseId:       pte.LeaseID,
		AttemptNumber: int32(pte.AttemptNumber),
		Revision:      int32(pte.Revision),
	}

	// Stamp PerformanceReport metadata. The worker emits exactly one report
	// per attempt; report_version tracks re-emissions (always 1 on first
	// send) and report_schema_version tracks the report shape.
	tr.ReportSchemaVersion = 1
	tr.ReportVersion = 1

	if report != nil {
		tr.ExecutorKey = report.ExecutorKey

		// Build typed execution_metrics.
		if report.TypedMetrics != nil {
			m := *report.TypedMetrics
			// Fall back to the first output artifact's hash when the
			// executor didn't explicitly stamp output_sha256.
			if m.OutputSha256 == "" && len(report.Outputs) > 0 {
				m.OutputSha256 = report.Outputs[0].Hash
			}
			tr.ExecutionMetrics = m.ToProto()
		}

		// Build typed phase_markers.
		for _, pm := range report.PhaseMarkers {
			tr.PhaseMarkers = append(tr.PhaseMarkers, &pb.PhaseMarker{
				Name:        pm.Name,
				StartedAt:   timestamppb.New(pm.StartedAt),
				CompletedAt: timestamppb.New(pm.CompletedAt),
				Status:      pm.Status,
				Notes:       pm.Notes,
			})
		}

		// Build per-segment C++ sidecar timings.
		for _, seg := range report.Segments {
			tr.SegmentTimings = append(tr.SegmentTimings, &pb.SegmentTiming{
				SegmentIndex:     int32(seg.SegmentIndex),
				SceneWorkerIndex: int32(seg.SceneWorkerIndex),
				SourceType:       seg.SourceType,
				DurationMs:       seg.DurationMS,
				AssetDownloadMs:  seg.AssetDownloadMS,
				FfmpegEncodeMs:   seg.FfmpegEncodeMS,
				SourceBytes:      seg.SourceBytes,
				OutputBytes:      seg.OutputBytes,
				FramesEncoded:    seg.FramesEncoded,
				Codec:            seg.Codec,
				Preset:           seg.Preset,
				FfmpegThreads:    int32(seg.FfmpegThreads),
				Status:           seg.Status,
				ErrorCode:        seg.ErrorCode,
				ErrorMessage:     seg.ErrorMessage,
				SourceUrlHash:    seg.SourceURLHash,
				CacheKey:         seg.CacheKey,
				InputDurationMs:  seg.InputDurationMS,
				OutputDurationMs: seg.OutputDurationMS,
				MetadataJson:     seg.MetadataJSON,
			})
		}

		// Build output_artifacts as repeated structpb.Struct.
		// artifact_id is now separate from sha256; SizeBytes carries real byte count.
		for _, ref := range report.Outputs {
			artifactID := ref.ArtifactID
			if artifactID == "" {
				// Backward-compat fallback: use Hash when ArtifactID is not set.
				artifactID = ref.Hash
			}
			if s, err := structpb.NewStruct(map[string]interface{}{
				"artifact_id":   artifactID,
				"artifact_type": ref.Type,
				"artifact_path": ref.URI,
				"size_bytes":    ref.SizeBytes,
				"sha256":        ref.Hash,
			}); err == nil {
				tr.OutputArtifacts = append(tr.OutputArtifacts, s)
			}
		}
	}

	// Compute the report hash over the canonical protojson serialization of
	// the final TaskResult. The hash field itself is empty during hashing,
	// then stamped onto the wire message so the master can use it for
	// idempotency and conflict detection.
	tr.ReportHash = ""
	reportJSON, err := protojson.Marshal(tr)
	if err != nil {
		w.logger.Error("[TASK] Failed to marshal TaskResult to protojson for %s: %v", taskID, err)
	} else {
		tr.ReportHash = fmt.Sprintf("%x", sha256.Sum256(reportJSON))
	}

	resultMsg := controltransport.NewTypedMessage(
		controltransport.MsgTaskResult,
		w.config.WorkerID,
		w.config.ProtocolVersion,
		tr,
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

// runJobTask executes the actual task via the TaskRunner.
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
func (w *Worker) dispatchTaskRunner(ctx context.Context, pte *PendingTaskExecution) (*taskrunner.TaskExecutionReport, error) {
	if w.taskRunner == nil {
		return nil, fmt.Errorf("worker has no taskRunner configured; call worker.New with options to install one")
	}

	spec := pte.Spec

	report, runErr := w.taskRunner.Run(ctx, spec)
	if runErr != nil {
		return &report, fmt.Errorf("taskrunner.Run: %w", runErr)
	}
	if report.Status != "succeeded" {
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

// Package worker — typed TaskResult proto builder.
//
// task_result_builder.go owns submitTaskResult: the canonical
// post-execution wire-format path that turns a
// (taskrunner.TaskExecutionReport, execErr) pair into a
// pb.TaskResult, stamps the report hash, and sends it via the
// transport. The function is the SINGLE entry point that emits
// TaskResult; the dispatch / execution / lifecycle helpers
// never touch the wire format directly.
//
// Canonical mappings (preserved verbatim from the original
// job_executor.go):
//
//   - execErr == nil                          → status="succeeded"
//   - execErr is context.Canceled             → status="cancelled"
//   - execErr is any other error              → status="failed"
//   - errorDetail = execErr.Error() (when execErr != nil)
//   - errorCode = report.ErrorCode (when report != nil)
//
// Report-hash computation: the hash is computed over the canonical
// protojson serialization of the final TaskResult, with the hash
// field itself empty during the hash input. The hash is then stamped
// onto the wire message so the master can use it for idempotency and
// conflict detection.
package worker

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/taskrunner"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// submitTaskResult sends a typed pb.TaskResult via the transport.
func (w *Worker) submitTaskResult(ctx context.Context, pte *PendingTaskExecution, taskID, attemptID string, report *taskrunner.TaskExecutionReport, execErr error) {
	status := "succeeded"
	var errorCode, errorDetail string
	if execErr != nil {
		status = "failed"
		if errors.Is(execErr, context.Canceled) {
			status = "cancelled"
		}
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

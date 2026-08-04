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
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/taskrunner"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// submitTaskResult sends a typed pb.TaskResult via the transport.
func (w *Worker) submitTaskResult(ctx context.Context, pte *PendingTaskExecution, taskID, attemptID string, report *taskrunner.TaskExecutionReport, execErr error) {
	resultStartedAt := time.Now()
	status := "succeeded"
	var errorCode, errorDetail string
	if report != nil && report.Status == "failed" {
		// Preserve a failed report even when the execution wrapper has no
		// separate error. This is important for partial renders whose
		// terminal failure was already classified by TaskRunner.
		status = "failed"
		errorCode = report.ErrorCode
		errorDetail = report.ErrorDetail
	}
	if execErr != nil {
		status = "failed"
		if errors.Is(execErr, context.Canceled) {
			status = "cancelled"
		}
		errorDetail = execErr.Error()
		if report != nil && report.ErrorCode != "" {
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
		attachWorkerIdentityAndTimings(w, report)
		tr.ExecutorKey = report.ExecutorKey

		// Build typed execution_metrics. Reports assembled outside TaskRunner
		// may only have the legacy dotted map (notably failed/cancelled
		// paths), so derive the typed mirror at this boundary as a fallback.
		if report.TypedMetrics == nil && len(report.Metrics) > 0 {
			report.TypedMetrics = taskrunner.TypedMetricsFromMap(report.Metrics)
		}
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

		// Build the full detailed phase stream (proto field 20). This is
		// the block-1 replacement for the legacy partial_phase_metrics
		// (field 19); legacy masters ignore it, block-1 masters ingest it
		// into task_execution_events. Lease identity is stamped here (the
		// runner does not know it); the master overrides all identity
		// fields at ingest. Keep this conversion independent of report
		// status: failed attempts carry the same complete prefix/event
		// stream as successful attempts.
		tr.PhaseTimings = appendDetailedPhaseTimings(
			tr.PhaseTimings,
			report.DetailedPhases,
			pte.LeaseID,
			pte.ExecutorID,
			int32(pte.ExecutorVersion),
		)

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
				StartedOffsetMs:  seg.StartedOffsetMS,
				FinishedOffsetMs: seg.FinishedOffsetMS,
				WorkerSlot:       int32(seg.WorkerSlot),
				CpuThreads:       int32(seg.CPUThreads),
				ParallelGroup:    seg.ParallelGroup,
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
		w.logArtifactProtocol("TASK_RESULT_SEND_FAILED", pte, resultStartedAt, "", "", "", map[string]interface{}{
			"status":      status,
			"report_hash": tr.GetReportHash(),
			"error":       submitErr.Error(),
		})
	} else {
		artifactCount := artifactReportOutputCount(report)
		w.logger.Info("[TASK] TaskResult submitted for %s (status: %s, artifacts: %d)", taskID, status, artifactCount)
		w.logArtifactProtocol("TASK_RESULT_SENT", pte, resultStartedAt, "", "", "", map[string]interface{}{
			"status":         status,
			"report_hash":    tr.GetReportHash(),
			"artifact_count": artifactCount,
		})
	}
}

// appendDetailedPhaseTimings converts every worker-reported detailed phase
// without filtering or coalescing. Repeated events such as ten distinct
// engine.encode segment operations must remain ten distinct wire entries.
// The helper is deliberately status-agnostic so successful and failed
// TaskResults use exactly the same cardinality and ordering contract.
func appendDetailedPhaseTimings(
	dst []*pb.PhaseTimingDetailed,
	phases []taskrunner.DetailedPhaseTiming,
	leaseID string,
	executorID string,
	executorVersion int32,
) []*pb.PhaseTimingDetailed {
	for _, phase := range phases {
		p := phase.ToProto()
		// Native sidecar events do not know the task offer identity. Stamp
		// the worker's canonical execution tuple here when the event did not
		// already carry it. The master still overwrites all identity fields
		// from task_attempts at ingest; this makes the wire report complete
		// without allowing worker echoes to become authoritative.
		if p.ExecutorId == "" && executorID != "" {
			p.ExecutorId = executorID
		}
		if p.ExecutorVersion == 0 && executorVersion > 0 {
			p.ExecutorVersion = executorVersion
		}
		if p.LeaseId == "" && leaseID != "" {
			p.LeaseId = leaseID
		}
		dst = append(dst, p)
	}
	return dst
}

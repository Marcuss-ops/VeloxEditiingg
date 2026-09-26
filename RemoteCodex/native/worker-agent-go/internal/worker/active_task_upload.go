package worker

// active_task_upload.go is the thin orchestrator for declared artifact upload.
// It delegates to:
//   - active_task_upload_progressive.go — progressive upload negotiation
//   - active_task_upload_completion.go  — completion message sending

import (
	"context"
	"errors"
	"fmt"
	"time"

	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/publisher"
	"velox-worker-agent/internal/spool"
	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/internal/telemetry"
)

// uploadDeclaredArtifacts uploads every declared output artifact, resolves
// transport per target, and returns the list of completion messages.
func (w *Worker) uploadDeclaredArtifacts(ctx context.Context, pte *PendingTaskExecution, report *taskrunner.TaskExecutionReport, plan *pb.ArtifactUploadPlan, entries []spool.SpoolEntry, resumable map[string]bool, started time.Time, earlyResults map[int]*publisher.UploadResult) ([]*pb.ArtifactUploadCompleted, error) {
	completed := make([]*pb.ArtifactUploadCompleted, 0, len(report.Outputs))

	// Compute total upload size for progress tracking.
	var totalUploadBytes int64
	for _, ref := range report.Outputs {
		totalUploadBytes += ref.SizeBytes
	}
	var uploadedBytes int64

	for i, ref := range report.Outputs {
		targetPB := plan.GetTargets()[i]
		if err := w.stashUploadPlan(ctx, entries[i], plan, targetPB); err != nil {
			return nil, fmt.Errorf("worker artifact upload: stash target %d: %w", i, err)
		}

		target := publisher.UploadTarget{
			DeclarationID: targetPB.GetDeclarationId(),
			ArtifactID:    targetPB.GetArtifactId(),
			UploadID:      targetPB.GetUploadId(),
			TransportID:   targetPB.GetTransportId(),
			UploadURL:     targetPB.GetUploadUrl(),
			ChunkSize:     targetPB.GetChunkSize(),
			ExpiresAtUnix: targetPB.GetExpiresAtUnix(),
		}

		transport, err := w.publisherRegistry.Resolve(target.TransportID)
		if err != nil {
			w.logArtifactProtocol("ARTIFACT_TRANSPORT_RESOLVE_FAILED", pte, started, plan.GetCommitId(), target.ArtifactID, target.UploadID, map[string]interface{}{"transport_id": target.TransportID, "error": err.Error()})
			return nil, fmt.Errorf("worker artifact upload: resolve %q: %w", target.TransportID, err)
		}

		w.logArtifactProtocol("ARTIFACT_TRANSFER_STARTED", pte, started, plan.GetCommitId(), target.ArtifactID, target.UploadID, map[string]interface{}{"artifact_type": ref.Type, "size_bytes": ref.SizeBytes})

		result := earlyResults[i]
		if result == nil {
			artifactBase := uploadedBytes
			request := publisher.UploadRequest{LocalPath: ref.URI, Target: target, WorkerSHA256: ref.Hash, CommitToken: plan.GetCommitToken()}
			request.Progress = func(artifactBytes int64) {
				w.updateUploadProgress(pte.TaskID, artifactBase+artifactBytes, totalUploadBytes, i+1, len(report.Outputs), started)
			}
			result, err = uploadWithNegotiatedPath(ctx, transport, request, w.config.ProgressivePartConcurrency)
		}
		if err != nil {
			w.logArtifactProtocol("ARTIFACT_TRANSFER_FAILED", pte, started, plan.GetCommitId(), target.ArtifactID, target.UploadID, map[string]interface{}{"artifact_type": ref.Type, "error": err.Error()})
			w.spillVolatileToNVMe(ctx, entries[i])
			resumable[entries[i].SpoolID] = true
			if recErr := w.outputSpool.RecordUploadFailure(ctx, entries[i].SpoolID, err.Error(), time.Now().Add(uploadResumeBackoff(0))); recErr != nil {
				w.logger.Warn("[ARTIFACT_RESUME] record upload failure failed spool=%s: %v", entries[i].SpoolID, recErr)
			}
			return nil, fmt.Errorf("worker artifact upload: transfer %q: %w", ref.Type, err)
		}

		if result == nil || result.UploadID == "" || result.UploadedBytes != ref.SizeBytes {
			w.logArtifactProtocol("ARTIFACT_TRANSFER_INVALID_RESULT", pte, started, plan.GetCommitId(), target.ArtifactID, target.UploadID, map[string]interface{}{"artifact_type": ref.Type, "error": "invalid byte count or upload id"})
			return nil, fmt.Errorf("worker artifact upload: transfer %q returned invalid byte count or upload id", ref.Type)
		}

		if err := w.outputSpool.MarkUploaded(ctx, entries[i].SpoolID); err != nil && !errors.Is(err, spool.ErrCASConflict) {
			return nil, fmt.Errorf("worker artifact upload: mark target %d uploaded: %w", i, err)
		}

		w.logArtifactProtocol("ARTIFACT_TRANSFER_COMPLETED", pte, started, plan.GetCommitId(), target.ArtifactID, result.UploadID, map[string]interface{}{
			"artifact_type":      ref.Type,
			"uploaded_bytes":     result.UploadedBytes,
			"upload_ms":          result.Breakdown.UploadMS,
			"upload_mbps":        result.Breakdown.UploadMbps,
			"chunk_count":        result.Breakdown.ChunkCount,
			"retry_count":        result.Breakdown.RetryCount,
			"remote_finalize_ms": result.Breakdown.RemoteFinalizeMS,
		})
		if i == 0 && report.RawMetrics != nil {
			w.logger.Info("[METRICS] task=%s attempt=%s concat_mode=%s packet_copy_ratio=%.2f encode_passes=%d segments_total=%d segments_packet_copy=%d segments_reencoded=%d cpu_user_ms=%d cpu_system_ms=%d audio_packet_copy=%d progressive_first_part_ms=%d progressive_parts_before_render=%d progressive_bytes_before_render=%d progressive_overlap_ms=%d output_bytes=%d output_sha256=%s", pte.TaskID, pte.AttemptID, report.RawMetrics.ConcatMode, report.RawMetrics.PacketCopyRatio, report.RawMetrics.EncodePasses, report.RawMetrics.SegmentsTotal, report.RawMetrics.SegmentsPacketCopy, report.RawMetrics.SegmentsReencoded, report.RawMetrics.CpuUserMs, report.RawMetrics.CpuSystemMs, report.RawMetrics.AudioPacketCopy, result.Breakdown.FirstPartStartedMS, result.Breakdown.PartsUploadedBeforeRenderEnd, result.Breakdown.BytesUploadedBeforeRenderEnd, result.Breakdown.OverlapMS, ref.SizeBytes, ref.Hash)
		}

		uploadedBytes += result.UploadedBytes
		w.updateUploadProgress(pte.TaskID, uploadedBytes, totalUploadBytes, i+1, len(report.Outputs), started)

		completed = append(completed, &pb.ArtifactUploadCompleted{
			TaskId:        pte.TaskID,
			AttemptId:     pte.AttemptID,
			CommitId:      plan.GetCommitId(),
			LeaseId:       pte.LeaseID,
			UploadId:      result.UploadID,
			UploadedBytes: result.UploadedBytes,
			WorkerSha256:  ref.Hash,
		})

		// ── Accumulate progressive upload metrics ────────────────────────
		recordUploadMetrics(report.RawMetrics, result.Breakdown, i)
	}

	return completed, nil
}

// recordUploadMetrics adds transport-owned upload observations to the
// attempt report. OutputBytes deliberately is not updated here: the renderer
// already owns that metric and records the primary output size during
// manifest verification. Adding UploadedBytes at this point double-counts the
// same artifact (the historical symptom was output_bytes == 2 * file_size).
func recordUploadMetrics(raw *telemetry.RawExecutionMetrics, breakdown publisher.UploadBreakdown, artifactIndex int) {
	if raw == nil {
		return
	}
	raw.UploadMbpsAvg = breakdown.UploadMbps
	// Progressive upload overlap: use the main (first) artifact's values.
	// For multi-artifact jobs, sum the parts/bytes and take the max overlap.
	if artifactIndex == 0 {
		raw.ProgressiveOverlapFirstPartMs = breakdown.FirstPartStartedMS
		raw.ProgressiveOverlapPartsBeforeRender = breakdown.PartsUploadedBeforeRenderEnd
		raw.ProgressiveOverlapBytesBeforeRender = breakdown.BytesUploadedBeforeRenderEnd
		raw.ProgressiveOverlapMs = breakdown.OverlapMS
		raw.TrailerToOpenMs = breakdown.TrailerToOpenMS
		raw.MuxToOpenUS = breakdown.MuxToOpenUS
		return
	}
	raw.ProgressiveOverlapPartsBeforeRender += breakdown.PartsUploadedBeforeRenderEnd
	raw.ProgressiveOverlapBytesBeforeRender += breakdown.BytesUploadedBeforeRenderEnd
	if breakdown.OverlapMS > raw.ProgressiveOverlapMs {
		raw.ProgressiveOverlapMs = breakdown.OverlapMS
	}
}

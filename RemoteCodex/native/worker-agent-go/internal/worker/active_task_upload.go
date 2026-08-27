package worker

import (
	"context"
	"fmt"
	"os"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/publisher"
	"velox-worker-agent/internal/spool"
	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/internal/telemetry"
)

func (w *Worker) uploadDeclaredArtifacts(ctx context.Context, pte *PendingTaskExecution, report *taskrunner.TaskExecutionReport, plan *pb.ArtifactUploadPlan, entries []spool.SpoolEntry, resumable map[string]bool, started time.Time) ([]*pb.ArtifactUploadCompleted, error) {
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
		target := publisher.UploadTarget{DeclarationID: targetPB.GetDeclarationId(), ArtifactID: targetPB.GetArtifactId(), UploadID: targetPB.GetUploadId(), TransportID: targetPB.GetTransportId(), UploadURL: targetPB.GetUploadUrl(), ChunkSize: targetPB.GetChunkSize(), ExpiresAtUnix: targetPB.GetExpiresAtUnix()}
		transport, err := w.publisherRegistry.Resolve(target.TransportID)
		if err != nil {
			w.logArtifactProtocol("ARTIFACT_TRANSPORT_RESOLVE_FAILED", pte, started, plan.GetCommitId(), target.ArtifactID, target.UploadID, map[string]interface{}{"transport_id": target.TransportID, "error": err.Error()})
			return nil, fmt.Errorf("worker artifact upload: resolve %q: %w", target.TransportID, err)
		}
		w.logArtifactProtocol("ARTIFACT_TRANSFER_STARTED", pte, started, plan.GetCommitId(), target.ArtifactID, target.UploadID, map[string]interface{}{"artifact_type": ref.Type, "size_bytes": ref.SizeBytes})
		result, err := uploadWithNegotiatedPath(ctx, transport, publisher.UploadRequest{LocalPath: ref.URI, Target: target, WorkerSHA256: ref.Hash, CommitToken: plan.GetCommitToken()}, w.config.ProgressivePartConcurrency)
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
		if err := w.outputSpool.MarkUploaded(ctx, entries[i].SpoolID); err != nil {
			return nil, fmt.Errorf("worker artifact upload: mark target %d uploaded: %w", i, err)
		}
		w.logArtifactProtocol("ARTIFACT_TRANSFER_COMPLETED", pte, started, plan.GetCommitId(), target.ArtifactID, result.UploadID, map[string]interface{}{"artifact_type": ref.Type, "uploaded_bytes": result.UploadedBytes, "upload_ms": result.Breakdown.UploadMS, "upload_mbps": result.Breakdown.UploadMbps, "chunk_count": result.Breakdown.ChunkCount, "retry_count": result.Breakdown.RetryCount, "remote_finalize_ms": result.Breakdown.RemoteFinalizeMS})
		uploadedBytes += result.UploadedBytes
		// Update upload progress in the active task's cumulative metrics
		// so the heartbeat carries per-artifact upload visibility.
		w.updateUploadProgress(pte.TaskID, uploadedBytes, totalUploadBytes, i+1, len(report.Outputs), started)
		completed = append(completed, &pb.ArtifactUploadCompleted{TaskId: pte.TaskID, AttemptId: pte.AttemptID, CommitId: plan.GetCommitId(), LeaseId: pte.LeaseID, UploadId: result.UploadID, UploadedBytes: result.UploadedBytes, WorkerSha256: ref.Hash})
		if pteReport := report.RawMetrics; pteReport != nil {
			pteReport.UploadMbpsAvg = result.Breakdown.UploadMbps
			pteReport.OutputBytes += result.Breakdown.UploadBytes
			// Progressive upload overlap: use the main (first) artifact's values.
			// For multi-artifact jobs, sum the parts/bytes and take the max overlap.
			if i == 0 {
				pteReport.ProgressiveOverlapFirstPartMs = result.Breakdown.FirstPartStartedMS
				pteReport.ProgressiveOverlapPartsBeforeRender = result.Breakdown.PartsUploadedBeforeRenderEnd
				pteReport.ProgressiveOverlapBytesBeforeRender = result.Breakdown.BytesUploadedBeforeRenderEnd
				pteReport.ProgressiveOverlapMs = result.Breakdown.OverlapMS
				pteReport.TrailerToOpenMs = result.Breakdown.TrailerToOpenMS
				pteReport.MuxToOpenUS = result.Breakdown.MuxToOpenUS
			} else {
				pteReport.ProgressiveOverlapPartsBeforeRender += result.Breakdown.PartsUploadedBeforeRenderEnd
				pteReport.ProgressiveOverlapBytesBeforeRender += result.Breakdown.BytesUploadedBeforeRenderEnd
				if result.Breakdown.OverlapMS > pteReport.ProgressiveOverlapMs {
					pteReport.ProgressiveOverlapMs = result.Breakdown.OverlapMS
				}
			}
		}
	}
	return completed, nil
}

// uploadWithNegotiatedPath keeps the existing V1 publication contract as the
// compatibility path. Progressive upload is selected only when the resolved
// transport advertises the capability and implements the optional interface;
// otherwise the ordinary Upload method is used automatically.
func uploadWithNegotiatedPath(ctx context.Context, transport publisher.Transport, req publisher.UploadRequest, progressivePartConcurrency int) (*publisher.UploadResult, error) {
	progress := artifactProgressForTask(ctx, req.Target.ArtifactID)
	var trailerToOpenMS int64
	if !progress.FinalizedAt.IsZero() {
		trailerToOpenMS = time.Since(progress.FinalizedAt).Milliseconds()
		if trailerToOpenMS < 0 {
			trailerToOpenMS = 0
		}
	}
	if !publisher.SupportsProgressive(transport) {
		res, err := transport.Upload(ctx, req)
		if err == nil && res != nil {
			res.Breakdown.TrailerToOpenMS = trailerToOpenMS
		}
		return res, err
	}

	progressive, ok := transport.(publisher.ProgressiveTransport)
	if !ok {
		return nil, fmt.Errorf("worker artifact upload: progressive capability negotiated but transport %q does not implement ProgressiveTransport", transport.ID())
	}
	file := publisher.NewGrowingFile()
	if progress.SafeOffsetBytes <= 0 {
		res, err := transport.Upload(ctx, req)
		if err == nil && res != nil {
			res.Breakdown.TrailerToOpenMS = trailerToOpenMS
		}
		return res, err
	}
	// Use the path from progress updates (the .partial path during the mux)
	// when available; fall back to the declared local path.  The .partial
	// file is the same inode as the final path after publishAtomic renames
	// it, so the fd remains valid across the rename.
	openPath := req.LocalPath
	if progress.Path != "" {
		openPath = progress.Path
	}
	file.Update(progress.SafeOffsetBytes, progress.Finalized, 0)
	if progress.Finalized {
		file.MarkDurable(progress.SafeOffsetBytes)
	}
	session, err := progressive.BeginProgressive(ctx, publisher.ProgressiveUploadRequest{
		Target:       req.Target,
		Artifact:     req.Target.ArtifactID,
		ExpectedSize: 0,
		CommitToken:  req.CommitToken,
	})
	if err != nil {
		return nil, fmt.Errorf("worker artifact upload: begin progressive %q: %w", transport.ID(), err)
	}
	st, err := os.Stat(openPath)
	if err != nil {
		_ = session.Abort(ctx)
		return nil, err
	}
	// mux_to_open_us: latency from when the first progress event with a
	// path was received from the C++ engine to when the Go side opened the
	// file for progressive upload. This closes the visibility gap between
	// trailer_to_publish_us (C++ finalization) and the Go upload start.
	var muxToOpenUS int64
	if !progress.FirstProgressAt.IsZero() {
		muxToOpenUS = time.Since(progress.FirstProgressAt).Microseconds()
		if muxToOpenUS < 0 {
			muxToOpenUS = 0
		}
	}
	if !progress.FinalizedAt.IsZero() {
		trailerToOpenMS = time.Since(progress.FinalizedAt).Milliseconds()
		if trailerToOpenMS < 0 {
			trailerToOpenMS = 0
		}
	}
	if progress.Finalized {
		file.Update(st.Size(), true, st.Size())
		file.MarkDurable(st.Size())
	}
	result, err := publisher.RunProgressiveUploadWithJournalAndStoreOptions(ctx, openPath, req.Target.ChunkSize, file, session, progressiveJournalPath(req), nil, "", publisher.ProgressiveUploadOptions{Workers: progressivePartConcurrency}, req.Progress)
	if err != nil {
		_ = session.Abort(ctx)
		return nil, err
	}
	result.Breakdown.TrailerToOpenMS = trailerToOpenMS
	result.Breakdown.MuxToOpenUS = muxToOpenUS
	telemetry.GetPrometheusMetrics().RecordProgressiveUploadTiming(
		time.Duration(result.Breakdown.FirstPartStartedMS)*time.Millisecond,
		result.Breakdown.PartsUploadedBeforeRenderEnd,
		result.Breakdown.BytesUploadedBeforeRenderEnd,
		time.Duration(result.Breakdown.OverlapMS)*time.Millisecond,
	)
	telemetry.GetPrometheusMetrics().RecordMuxToOpenUS(muxToOpenUS)
	return result, nil
}

func progressiveJournalPath(req publisher.UploadRequest) string {
	if req.Target.UploadID == "" || req.LocalPath == "" {
		return ""
	}
	return req.LocalPath + "." + req.Target.UploadID + ".progressive.json"
}

func (w *Worker) sendArtifactCompletions(ctx context.Context, pte *PendingTaskExecution, plan *pb.ArtifactUploadPlan, completed []*pb.ArtifactUploadCompleted, started time.Time) error {
	for i, completion := range completed {
		if err := w.transportSend(ctx, w.typedArtifactMessage(controltransport.MsgArtifactUploadCompleted, completion)); err != nil {
			w.logArtifactProtocol("ARTIFACT_COMPLETION_SEND_FAILED", pte, started, completion.GetCommitId(), "", completion.GetUploadId(), map[string]interface{}{"error": err.Error()})
			return fmt.Errorf("worker artifact upload: report completion: %w", err)
		}
		artifactID := ""
		if i < len(plan.GetTargets()) && plan.GetTargets()[i] != nil {
			artifactID = plan.GetTargets()[i].GetArtifactId()
		}
		w.logArtifactProtocol("ARTIFACT_COMPLETION_SENT", pte, started, completion.GetCommitId(), artifactID, completion.GetUploadId(), map[string]interface{}{"uploaded_bytes": completion.GetUploadedBytes()})
	}
	return nil
}

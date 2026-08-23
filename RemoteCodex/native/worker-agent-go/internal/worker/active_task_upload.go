package worker

import (
	"context"
	"fmt"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/publisher"
	"velox-worker-agent/internal/spool"
	"velox-worker-agent/internal/taskrunner"
)

func (w *Worker) uploadDeclaredArtifacts(ctx context.Context, pte *PendingTaskExecution, report *taskrunner.TaskExecutionReport, plan *pb.ArtifactUploadPlan, entries []spool.SpoolEntry, resumable map[string]bool, started time.Time) ([]*pb.ArtifactUploadCompleted, error) {
	completed := make([]*pb.ArtifactUploadCompleted, 0, len(report.Outputs))
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
		result, err := transport.Upload(ctx, publisher.UploadRequest{LocalPath: ref.URI, Target: target, WorkerSHA256: ref.Hash, CommitToken: plan.GetCommitToken()})
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
		w.logArtifactProtocol("ARTIFACT_TRANSFER_COMPLETED", pte, started, plan.GetCommitId(), target.ArtifactID, result.UploadID, map[string]interface{}{"artifact_type": ref.Type, "uploaded_bytes": result.UploadedBytes})
		completed = append(completed, &pb.ArtifactUploadCompleted{TaskId: pte.TaskID, AttemptId: pte.AttemptID, CommitId: plan.GetCommitId(), LeaseId: pte.LeaseID, UploadId: result.UploadID, UploadedBytes: result.UploadedBytes, WorkerSha256: ref.Hash})
	}
	return completed, nil
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

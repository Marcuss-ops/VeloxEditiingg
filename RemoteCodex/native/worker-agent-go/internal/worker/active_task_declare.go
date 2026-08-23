package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/publisher"
	"velox-worker-agent/internal/spool"
	"velox-worker-agent/internal/taskrunner"
)

func (w *Worker) declareArtifactOutputs(ctx context.Context, pte *PendingTaskExecution, manifests []*pb.OutputManifest, ackCh <-chan controltransport.ControlMessage, entries []spool.SpoolEntry, resumable map[string]bool, started time.Time, deadline time.Time) (*pb.ArtifactUploadPlan, error) {
	declared := &pb.TaskOutputDeclared{TaskId: pte.TaskID, JobId: pte.JobID, AttemptId: pte.AttemptID, LeaseId: pte.LeaseID, AttemptNumber: int32(pte.AttemptNumber), Revision: int32(pte.Revision), Manifests: manifests}
	if err := w.transportSend(ctx, w.typedArtifactMessage(controltransport.MsgTaskOutputDeclared, declared)); err != nil {
		w.markDeclareResumable(ctx, entries, resumable, err)
		w.logArtifactProtocol("ARTIFACT_DECLARE_SEND_FAILED", pte, started, "", "", "", map[string]interface{}{"manifest_count": len(manifests), "error": err.Error()})
		return nil, fmt.Errorf("worker sidecar upload: declare receipt: %w", err)
	}
	w.logArtifactProtocol("ARTIFACT_DECLARE_SENT", pte, started, "", "", "", map[string]interface{}{"manifest_count": len(manifests)})
	msg, err := w.waitForRegisteredArtifactAck(ctx, ackCh, deadline)
	if err != nil {
		w.markDeclareResumable(ctx, entries, resumable, err)
		w.logArtifactProtocol("ARTIFACT_UPLOAD_PLAN_WAIT_FAILED", pte, started, "", "", "", map[string]interface{}{"error": err.Error()})
		return nil, fmt.Errorf("worker sidecar upload: wait for upload plan: %w", err)
	}
	plan, ok := msg.TypedPayload.(*pb.ArtifactUploadPlan)
	if msg.Type != controltransport.MsgArtifactUploadPlan || !ok || plan == nil {
		return nil, fmt.Errorf("worker sidecar upload: expected artifact upload plan, got %s/%T", msg.Type, msg.TypedPayload)
	}
	if err := validateUploadPlan(plan, pte, len(manifests)); err != nil {
		return nil, err
	}
	w.logArtifactProtocol("ARTIFACT_UPLOAD_PLAN_RECEIVED", pte, started, plan.GetCommitId(), "", "", map[string]interface{}{"target_count": len(plan.GetTargets())})
	return plan, nil
}

func validateUploadPlan(plan *pb.ArtifactUploadPlan, pte *PendingTaskExecution, outputCount int) error {
	if plan == nil || pte == nil {
		return errors.New("worker artifact upload: upload plan is nil")
	}
	if plan.GetTaskId() != pte.TaskID || plan.GetAttemptId() != pte.AttemptID || plan.GetLeaseId() != pte.LeaseID || len(plan.GetTargets()) != outputCount {
		return fmt.Errorf("worker artifact upload: upload plan identity or target count mismatch")
	}
	return nil
}

func (w *Worker) markDeclareResumable(ctx context.Context, entries []spool.SpoolEntry, resumable map[string]bool, cause error) {
	for _, e := range entries {
		resumable[e.SpoolID] = true
		w.spillVolatileToNVMe(ctx, e)
		if err := w.outputSpool.RecordUploadFailure(ctx, e.SpoolID, "declare receipt: "+cause.Error(), time.Now().Add(uploadResumeBackoff(e.UploadAttemptCount))); err != nil {
			w.logger.Warn("[ARTIFACT_RESUME] record declare failure failed spool=%s: %v", e.SpoolID, err)
		}
	}
}

func (w *Worker) registerOutputSpool(ctx context.Context, pte *PendingTaskExecution, report *taskrunner.TaskExecutionReport) ([]spool.SpoolEntry, error) {
	if w.outputSpool == nil {
		return nil, fmt.Errorf("durable output spool is not configured")
	}
	entries := make([]spool.SpoolEntry, 0, len(report.Outputs))
	for i, ref := range report.Outputs {
		kind, _ := outputKindAndMime(ref.Type)
		entry, _, err := w.outputSpool.Ensure(ctx, spool.SpoolEntry{TaskID: pte.TaskID, AttemptID: pte.AttemptID, WorkerSpoolKey: fmt.Sprintf("%s:output:%d", pte.TaskID, i), OutputKind: kind, LocalPath: ref.URI, SHA256: ref.Hash, SizeBytes: ref.SizeBytes, Status: spool.StatusRendering, StorageTier: w.outputStorageTier(ref.URI)})
		if err != nil {
			return nil, err
		}
		if err := w.outputSpool.MarkReady(ctx, entry.SpoolID, ref.Hash, ref.SizeBytes); err != nil {
			return nil, err
		}
		entry.Status = spool.StatusOutputReady
		entry.SHA256 = ref.Hash
		entry.SizeBytes = ref.SizeBytes
		entries = append(entries, *entry)
	}
	return entries, nil
}

func (w *Worker) stashUploadPlan(ctx context.Context, entry spool.SpoolEntry, plan *pb.ArtifactUploadPlan, targetPB *pb.UploadTarget) error {
	if targetPB == nil || targetPB.GetDeclarationId() == "" || targetPB.GetUploadId() == "" || targetPB.GetTransportId() == "" {
		return errors.New("worker artifact upload: target is incomplete")
	}
	target := publisher.UploadTarget{DeclarationID: targetPB.GetDeclarationId(), ArtifactID: targetPB.GetArtifactId(), UploadID: targetPB.GetUploadId(), TransportID: targetPB.GetTransportId(), UploadURL: targetPB.GetUploadUrl(), ChunkSize: targetPB.GetChunkSize(), ExpiresAtUnix: targetPB.GetExpiresAtUnix()}
	targetJSON, err := json.Marshal(target)
	if err != nil {
		return err
	}
	if err := w.outputSpool.StashUploadPlan(ctx, entry.SpoolID, plan.GetCommitId(), target.UploadID, string(targetJSON), plan.GetCommitToken()); err != nil {
		return err
	}
	if err := w.outputSpool.MarkUploadPending(ctx, entry.SpoolID, target.UploadID); err != nil {
		return err
	}
	return w.outputSpool.MarkUploading(ctx, entry.SpoolID, 0)
}

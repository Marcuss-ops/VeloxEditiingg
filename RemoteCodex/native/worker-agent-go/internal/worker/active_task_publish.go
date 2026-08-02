package worker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/publisher"
	"velox-worker-agent/internal/spool"
	"velox-worker-agent/internal/taskrunner"
)

// active_task_publish.go owns the typed Artifact Commit Protocol
// publication path (publishArtifactsV1 + the durable spool
// registration helpers registerOutputSpool / rejectOutputSpool).
// The upload lifecycle helpers live in active_task_lifecycle.go.

// publishArtifactsV1 declares and uploads every local output through the
// typed Master artifact protocol. The progress sidecar is therefore a true
// secondary artifact in the same attempt commit as the primary video.
func (w *Worker) publishArtifactsV1(ctx context.Context, pte *PendingTaskExecution, report *taskrunner.TaskExecutionReport) error {
	if report == nil || w.transport == nil || w.publisherRegistry == nil {
		return nil
	}

	for _, ref := range report.Outputs {
		if ref.URI == "" || ref.Hash == "" || ref.SizeBytes <= 0 {
			return fmt.Errorf("worker artifact upload: output %q has incomplete local manifest", ref.Type)
		}
		if _, err := os.Stat(ref.URI); err != nil {
			return fmt.Errorf("worker artifact upload: output %q is not readable: %w", ref.URI, err)
		}
	}

	// Register every local output durably before the Master receives the
	// declaration. Rows and files remain available through commit so a
	// restart can resume or audit the receipt.
	spoolEntries, err := w.registerOutputSpool(ctx, pte, report)
	if err != nil {
		return fmt.Errorf("worker artifact upload: register durable spool: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			w.rejectOutputSpool(spoolEntries, "publication_failed", "typed artifact publication did not reach commit acknowledgement")
		}
	}()

	manifests := make([]*pb.OutputManifest, 0, len(report.Outputs))
	for i, ref := range report.Outputs {
		spoolKey := fmt.Sprintf("%s:output:%d", pte.TaskID, i)
		outputKind := ref.Type
		mimeType := "application/octet-stream"
		if ref.Type == "render.output" {
			outputKind = "final_video"
			mimeType = "video/mp4"
		} else if ref.Type == "engine.progress.sidecar" {
			outputKind = "engine_progress_sidecar"
			mimeType = "application/json"
		}
		manifests = append(manifests, &pb.OutputManifest{
			OutputKind:     outputKind,
			LogicalName:    filepath.Base(ref.URI),
			MimeType:       mimeType,
			SizeBytes:      ref.SizeBytes,
			Sha256:         ref.Hash,
			WorkerSpoolKey: spoolKey,
		})
	}

	declared := &pb.TaskOutputDeclared{
		TaskId:        pte.TaskID,
		JobId:         pte.JobID,
		AttemptId:     pte.AttemptID,
		LeaseId:       pte.LeaseID,
		AttemptNumber: int32(pte.AttemptNumber),
		Revision:      int32(pte.Revision),
		Manifests:     manifests,
	}

	ackCh := w.registerPendingArtifactAck(pte.TaskID)
	defer w.unregisterPendingArtifactAck(pte.TaskID)
	if err := w.transport.Send(ctx, controltransport.NewTypedMessage(
		controltransport.MsgTaskOutputDeclared,
		w.config.WorkerID,
		w.config.ProtocolVersion,
		declared,
	)); err != nil {
		return fmt.Errorf("worker sidecar upload: declare receipt: %w", err)
	}

	deadline := time.Now().Add(5 * time.Minute)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	msg, err := w.waitForRegisteredArtifactAck(ctx, ackCh, deadline)
	if err != nil {
		return fmt.Errorf("worker sidecar upload: wait for upload plan: %w", err)
	}
	plan, ok := msg.TypedPayload.(*pb.ArtifactUploadPlan)
	if msg.Type != controltransport.MsgArtifactUploadPlan || !ok || plan == nil {
		return fmt.Errorf("worker sidecar upload: expected artifact upload plan, got %s/%T", msg.Type, msg.TypedPayload)
	}
	if plan.GetTaskId() != pte.TaskID || plan.GetAttemptId() != pte.AttemptID || plan.GetLeaseId() != pte.LeaseID || len(plan.GetTargets()) != len(report.Outputs) {
		return fmt.Errorf("worker artifact upload: upload plan identity or target count mismatch")
	}

	completed := make([]*pb.ArtifactUploadCompleted, 0, len(report.Outputs))
	for i, ref := range report.Outputs {
		targetPB := plan.GetTargets()[i]
		if targetPB == nil || targetPB.GetDeclarationId() == "" || targetPB.GetUploadId() == "" || targetPB.GetTransportId() == "" {
			return fmt.Errorf("worker artifact upload: target %d is incomplete", i)
		}
		if err := w.outputSpool.MarkUploadPending(ctx, spoolEntries[i].SpoolID, targetPB.GetUploadId()); err != nil {
			return fmt.Errorf("worker artifact upload: mark target %d pending: %w", i, err)
		}
		if err := w.outputSpool.MarkUploading(ctx, spoolEntries[i].SpoolID, 0); err != nil {
			return fmt.Errorf("worker artifact upload: mark target %d uploading: %w", i, err)
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
			return fmt.Errorf("worker artifact upload: resolve %q: %w", target.TransportID, err)
		}
		result, err := transport.Upload(ctx, publisher.UploadRequest{
			LocalPath:    ref.URI,
			Target:       target,
			WorkerSHA256: ref.Hash,
			CommitToken:  plan.GetCommitToken(),
		})
		if err != nil {
			return fmt.Errorf("worker artifact upload: transfer %q: %w", ref.Type, err)
		}
		if result == nil || result.UploadID == "" || result.UploadedBytes != ref.SizeBytes {
			return fmt.Errorf("worker artifact upload: transfer %q returned invalid byte count or upload id", ref.Type)
		}
		if err := w.outputSpool.MarkUploaded(ctx, spoolEntries[i].SpoolID); err != nil {
			return fmt.Errorf("worker artifact upload: mark target %d uploaded: %w", i, err)
		}
		completed = append(completed, &pb.ArtifactUploadCompleted{
			TaskId:        pte.TaskID,
			AttemptId:     pte.AttemptID,
			CommitId:      plan.GetCommitId(),
			LeaseId:       pte.LeaseID,
			UploadId:      result.UploadID,
			UploadedBytes: result.UploadedBytes,
			WorkerSha256:  ref.Hash,
		})
	}
	for _, completion := range completed {
		if err := w.transport.Send(ctx, controltransport.NewTypedMessage(
			controltransport.MsgArtifactUploadCompleted,
			w.config.WorkerID,
			w.config.ProtocolVersion,
			completion,
		)); err != nil {
			return fmt.Errorf("worker artifact upload: report completion: %w", err)
		}
	}
	msg, err = w.waitForRegisteredArtifactAck(ctx, ackCh, deadline)
	if err != nil {
		return fmt.Errorf("worker artifact upload: wait for commit ack: %w", err)
	}
	commitAck, ok := msg.TypedPayload.(*pb.TaskCommitAck)
	if msg.Type != controltransport.MsgTaskCommitAck || !ok || commitAck == nil {
		return fmt.Errorf("worker artifact upload: expected task commit ack, got %s/%T", msg.Type, msg.TypedPayload)
	}
	if commitAck.GetTaskId() != pte.TaskID || commitAck.GetAttemptId() != pte.AttemptID || commitAck.GetJobId() != pte.JobID || commitAck.GetLeaseId() != pte.LeaseID || commitAck.GetRevision() != int32(pte.Revision) || commitAck.GetCommitId() != plan.GetCommitId() {
		return fmt.Errorf("worker artifact upload: commit ack fence mismatch")
	}
	for i := range spoolEntries {
		if err := w.outputSpool.MarkCommitted(ctx, spoolEntries[i].SpoolID); err != nil {
			return fmt.Errorf("worker artifact upload: mark target %d committed: %w", i, err)
		}
	}
	committed = true
	w.logger.Info("[TASK] %d output artifacts committed for task %s (job=%s attempt=%s)",
		len(completed), pte.TaskID, pte.JobID, pte.AttemptID)
	return nil
}

func (w *Worker) registerOutputSpool(ctx context.Context, pte *PendingTaskExecution, report *taskrunner.TaskExecutionReport) ([]spool.SpoolEntry, error) {
	if w.outputSpool == nil {
		return nil, fmt.Errorf("durable output spool is not configured")
	}
	entries := make([]spool.SpoolEntry, 0, len(report.Outputs))
	for i, ref := range report.Outputs {
		entry, err := w.outputSpool.Insert(ctx, spool.SpoolEntry{
			TaskID:         pte.TaskID,
			AttemptID:      pte.AttemptID,
			WorkerSpoolKey: fmt.Sprintf("%s:output:%d", pte.TaskID, i),
			LocalPath:      ref.URI,
			SHA256:         ref.Hash,
			SizeBytes:      ref.SizeBytes,
			Status:         spool.StatusRendering,
		})
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

func (w *Worker) rejectOutputSpool(entries []spool.SpoolEntry, code, message string) {
	if w.outputSpool == nil {
		return
	}
	for _, entry := range entries {
		_ = w.outputSpool.MarkRejected(context.Background(), entry.SpoolID, code, message)
	}
}

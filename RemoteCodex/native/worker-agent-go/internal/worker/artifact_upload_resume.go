// Package worker — artifact-upload resume loop.
//
// A failed artifact upload (network blip, master restart, upload transport
// error) leaves a spool row mid-upload (UPLOAD_PENDING / UPLOADING) with the
// full upload target + commit token persisted (StashUploadPlan) and — when
// the artifact was tmpfs-backed — a durable NVMe copy at local_path
// (spillVolatileToNVMe). This loop re-drives those rows:
//
//   - re-upload the bytes from the CURRENT local_path (the repointed NVMe
//     path after a spill) using the persisted target + commit token;
//   - on success MarkUploaded, then send ArtifactUploadCompleted and wait for
//     the master's TaskCommitAck before MarkCommitted;
//   - on failure schedule the next attempt with exponential backoff, and
//     after a bounded number of attempts MarkRejected (terminal, non-resumable).
//
// Retry is bounded + backoff (not infinite): a permanently bad target or a
// down master converges to REJECTED so the master's attempt reaper can
// reschedule the render instead of the worker spinning forever.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/publisher"
	"velox-worker-agent/internal/spool"
)

const (
	// uploadResumeInitial is the first retry delay after a failure.
	uploadResumeInitial = 2 * time.Second
	// uploadResumeMax is the cap on the exponential backoff.
	uploadResumeMax = 2 * time.Minute
	// uploadResumeMaxAttempts is the bounded retry budget before a row is
	// MarkRejected. Attempts beyond this are permanent failures.
	uploadResumeMaxAttempts = 5
	// uploadResumeInterval is the resume-loop tick cadence. Retries are still
	// gated per-row by next_upload_attempt_at, so a short tick only decides
	// which rows are due.
	uploadResumeInterval = 2 * time.Second
	// uploadResumeBatch caps the number of due rows driven per tick.
	uploadResumeBatch = 32
	// uploadResumeCommitWait is the bounded wait window for a TaskCommitAck
	// during a resume-driven completion. A lost ack must not block the whole
	// resume loop for minutes; the persisted UPLOADED row is retried later.
	uploadResumeCommitWait = 5 * time.Second
	// declareResumePlanWait is the bounded wait window for an
	// ArtifactUploadPlan after a resume-driven TaskOutputDeclared. A lost
	// plan must not block the declare pass; the rows stay OUTPUT_READY and
	// are re-declared on a later tick.
	declareResumePlanWait = 5 * time.Second
)

// uploadResumeBackoff returns the delay before the retry that follows
// `failures` recorded failures: 2s, 4s, 8s, … capped at 2m.
func uploadResumeBackoff(failures int) time.Duration {
	if failures < 0 {
		failures = 0
	}
	delay := uploadResumeInitial
	for i := 0; i < failures && delay < uploadResumeMax; i++ {
		delay *= 2
	}
	if delay > uploadResumeMax {
		return uploadResumeMax
	}
	return delay
}

// startArtifactUploadResumeLoop launches the session-scoped resume loop under
// sessionCtx. It is a no-op when the worker has no durable spool or no
// publisher registry (legacy/headless fixtures).
func (w *Worker) startArtifactUploadResumeLoop(ctx context.Context) {
	if w == nil || w.outputSpool == nil || w.publisherRegistry == nil {
		return
	}
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		ticker := time.NewTicker(uploadResumeInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-w.stopChan:
				return
			case <-ticker.C:
				// Declare pass first so a freshly-declared row (now UPLOAD_PENDING)
				// can be picked up by the upload pass in the same tick.
				if err := w.resumeDueDeclarations(ctx); err != nil && ctx.Err() == nil {
					w.logger.Warn("[ARTIFACT_RESUME] declare resume pass failed: %v", err)
				}
				if err := w.resumeDueArtifactUploads(ctx); err != nil && ctx.Err() == nil {
					w.logger.Warn("[ARTIFACT_RESUME] resume pass failed: %v", err)
				}
			}
		}
	}()
}

// resumeDueArtifactUploads lists the due mid-upload rows and drives each one
// forward (re-upload → complete commit), respecting the per-row backoff and
// bounded retry budget.
func (w *Worker) resumeDueArtifactUploads(ctx context.Context) error {
	if w == nil || w.outputSpool == nil || w.publisherRegistry == nil {
		return nil
	}
	entries, err := w.outputSpool.ListUploadResumeCandidates(ctx, uploadResumeBatch)
	if err != nil {
		return err
	}
	now := time.Now()
	for _, entry := range entries {
		if !entry.NextUploadAttemptAt.IsZero() && entry.NextUploadAttemptAt.After(now) {
			continue // backoff not elapsed yet
		}
		w.resumeArtifactUpload(ctx, entry)
	}
	return nil
}

// resumeDueDeclarations lists due OUTPUT_READY rows (no upload plan yet),
// groups them by (task_id, attempt_id) and re-sends TaskOutputDeclared for
// each group so the master's idempotent DeclareOutputs returns the (same)
// upload plan. On success the targets are stashed + rows marked
// UPLOAD_PENDING so the upload pass re-drives the bytes.
func (w *Worker) resumeDueDeclarations(ctx context.Context) error {
	if w == nil || w.outputSpool == nil || w.publisherRegistry == nil {
		return nil
	}
	entries, err := w.outputSpool.ListDeclareResumeCandidates(ctx, uploadResumeBatch)
	if err != nil {
		return err
	}
	now := time.Now()
	type attemptKey struct{ taskID, attemptID string }
	groups := make(map[attemptKey][]spool.SpoolEntry)
	var order []attemptKey
	for _, e := range entries {
		if !e.NextUploadAttemptAt.IsZero() && e.NextUploadAttemptAt.After(now) {
			continue // backoff not elapsed yet
		}
		k := attemptKey{e.TaskID, e.AttemptID}
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], e)
	}
	for _, k := range order {
		w.resumeDeclaration(ctx, groups[k])
	}
	return nil
}

func (w *Worker) resumeDeclaration(ctx context.Context, entries []spool.SpoolEntry) {
	if len(entries) == 0 {
		return
	}
	if w.publisherPool != nil {
		if err := w.publisherPool.Acquire(ctx); err != nil {
			return
		}
		defer w.publisherPool.Release()
	}
	// PublisherPool bounds concurrency, but only this mutex serializes a
	// foreground publication against the resume loop for the same worker spool.
	w.artifactUploadMu.Lock()
	defer w.artifactUploadMu.Unlock()

	taskID := entries[0].TaskID
	attemptID := entries[0].AttemptID

	w.activeTaskLeasesMu.RLock()
	lease := w.activeTaskLeases[taskID]
	w.activeTaskLeasesMu.RUnlock()
	if lease == nil || lease.AttemptID != attemptID {
		w.logger.Warn("[ARTIFACT_RESUME] no active lease for task=%s attempt=%s — deferring declare", taskID, attemptID)
		return
	}

	// Bounded budget, mirroring the upload resume path: after the retry budget
	// is exhausted the row is REJECTED so the master's attempt reaper can
	// reschedule the render instead of the worker spinning forever.
	if entries[0].UploadAttemptCount >= uploadResumeMaxAttempts {
		for _, e := range entries {
			if err := w.outputSpool.MarkRejected(ctx, e.SpoolID, "declare_retry_exhausted", "declare receipt retry budget exhausted"); err != nil && !errors.Is(err, spool.ErrCASConflict) {
				w.logger.Warn("[ARTIFACT_RESUME] reject exhausted declare spool=%s: %v", e.SpoolID, err)
			}
		}
		w.logger.Warn("[ARTIFACT_RESUME] declare retry budget exhausted task=%s attempt=%s — rejected", taskID, attemptID)
		return
	}

	manifests := make([]*pb.OutputManifest, 0, len(entries))
	for _, e := range entries {
		if e.OutputKind == "" {
			// Legacy row without the persisted manifest kind cannot rebuild the
			// declaration; leave it for the master attempt reaper.
			w.logger.Warn("[ARTIFACT_RESUME] output_kind empty spool=%s — cannot rebuild declare manifest", e.SpoolID)
			return
		}
		mimeType := mimeForOutputKind(e.OutputKind)
		if e.OutputKind == "engine_progress_sidecar" {
			mimeType = "application/json"
		}
		manifests = append(manifests, &pb.OutputManifest{
			OutputKind:     e.OutputKind,
			LogicalName:    filepath.Base(e.LocalPath),
			MimeType:       mimeType,
			SizeBytes:      e.SizeBytes,
			Sha256:         e.SHA256,
			WorkerSpoolKey: e.WorkerSpoolKey,
		})
	}

	declared := &pb.TaskOutputDeclared{
		TaskId:        taskID,
		JobId:         lease.JobID,
		AttemptId:     attemptID,
		LeaseId:       lease.LeaseID,
		AttemptNumber: int32(lease.AttemptNumber),
		Revision:      int32(lease.Revision),
		Manifests:     manifests,
	}

	ackCh := w.registerPendingArtifactAck(taskID)
	defer w.unregisterPendingArtifactAck(taskID)
	if err := w.transportSend(ctx, controltransport.NewTypedMessage(
		controltransport.MsgTaskOutputDeclared,
		w.config.WorkerID,
		w.config.ProtocolVersion,
		declared,
	)); err != nil {
		w.scheduleDeclarationRetry(ctx, entries, err)
		return
	}

	deadline := time.Now().Add(declareResumePlanWait)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	msg, err := w.waitForRegisteredArtifactAck(ctx, ackCh, deadline)
	if err != nil {
		w.scheduleDeclarationRetry(ctx, entries, err)
		return
	}
	plan, ok := msg.TypedPayload.(*pb.ArtifactUploadPlan)
	if msg.Type != controltransport.MsgArtifactUploadPlan || !ok || plan == nil {
		w.scheduleDeclarationRetry(ctx, entries, errors.New("expected artifact upload plan"))
		return
	}
	if plan.GetTaskId() != taskID || plan.GetAttemptId() != attemptID || plan.GetLeaseId() != lease.LeaseID || len(plan.GetTargets()) != len(entries) {
		w.scheduleDeclarationRetry(ctx, entries, errors.New("upload plan identity or target count mismatch"))
		return
	}

	for i, e := range entries {
		targetPB := plan.GetTargets()[i]
		if targetPB == nil || targetPB.GetDeclarationId() == "" || targetPB.GetUploadId() == "" || targetPB.GetTransportId() == "" {
			w.scheduleDeclarationRetry(ctx, entries, fmt.Errorf("upload plan target %d is incomplete", i))
			return
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
		targetJSON, jsonErr := json.Marshal(target)
		if jsonErr != nil {
			w.scheduleDeclarationRetry(ctx, entries, jsonErr)
			return
		}
		if err := w.outputSpool.StashUploadPlan(ctx, e.SpoolID, plan.GetCommitId(), target.UploadID, string(targetJSON), plan.GetCommitToken()); err != nil {
			if !errors.Is(err, spool.ErrCASConflict) {
				w.logger.Warn("[ARTIFACT_RESUME] stash target spool=%s: %v", e.SpoolID, err)
			}
			continue
		}
		if err := w.outputSpool.MarkUploadPending(ctx, e.SpoolID, target.UploadID); err != nil && !errors.Is(err, spool.ErrCASConflict) {
			w.logger.Warn("[ARTIFACT_RESUME] mark pending spool=%s: %v", e.SpoolID, err)
		}
	}
	w.logger.Info("[ARTIFACT_RESUME] declare resumed task=%s attempt=%s outputs=%d", taskID, attemptID, len(entries))
}

// scheduleDeclarationRetry records a non-fatal declare failure on every row
// in the group and bumps the bounded retry counter with exponential backoff.
// The rows stay OUTPUT_READY so the next declare pass re-sends the declaration.
func (w *Worker) scheduleDeclarationRetry(ctx context.Context, entries []spool.SpoolEntry, cause error) {
	for _, e := range entries {
		next := time.Now().Add(uploadResumeBackoff(e.UploadAttemptCount))
		if err := w.outputSpool.RecordUploadFailure(ctx, e.SpoolID, "declare receipt: "+cause.Error(), next); err != nil {
			if !errors.Is(err, spool.ErrCASConflict) {
				w.logger.Warn("[ARTIFACT_RESUME] record declare failure spool=%s: %v", e.SpoolID, err)
			}
			continue
		}
		w.logger.Warn("[ARTIFACT_RESUME] declare retry scheduled spool=%s in %s: %v", e.SpoolID, time.Until(next).Round(time.Second), cause)
	}
}

func (w *Worker) resumeArtifactUpload(ctx context.Context, entry spool.SpoolEntry) {
	if w.publisherPool != nil {
		if err := w.publisherPool.Acquire(ctx); err != nil {
			return
		}
		defer w.publisherPool.Release()
	}
	// Keep the spool lifecycle single-writer even when the publisher pool is
	// enabled; otherwise resume can race the foreground upload and win the CAS.
	w.artifactUploadMu.Lock()
	defer w.artifactUploadMu.Unlock()

	if entry.UploadTargetJSON == "" {
		// No persisted target (plan never stashed). Nothing to re-drive;
		// leave it for the master's attempt reaper.
		return
	}
	var target publisher.UploadTarget
	if err := json.Unmarshal([]byte(entry.UploadTargetJSON), &target); err != nil {
		w.logger.Warn("[ARTIFACT_RESUME] bad target JSON spool=%s: %v", entry.SpoolID, err)
		return
	}
	if target.UploadID == "" || target.TransportID == "" {
		w.logger.Warn("[ARTIFACT_RESUME] incomplete target spool=%s upload_id=%q transport=%q", entry.SpoolID, target.UploadID, target.TransportID)
		return
	}

	// Bounded budget: after the last upload/commit-resume attempt is exhausted
	// the row becomes a permanent failure (REJECTED) so the master re-schedules
	// the render. An UPLOADED row still gets a chance to resend completion
	// before this guard is applied by the caller's normal retry accounting.
	if entry.UploadAttemptCount >= uploadResumeMaxAttempts {
		if err := w.outputSpool.MarkRejected(ctx, entry.SpoolID, "upload_retry_exhausted", "artifact upload retry budget exhausted"); err != nil && !errors.Is(err, spool.ErrCASConflict) {
			w.logger.Warn("[ARTIFACT_RESUME] reject exhausted spool=%s: %v", entry.SpoolID, err)
		} else {
			w.logger.Warn("[ARTIFACT_RESUME] upload retry budget exhausted spool=%s — rejected", entry.SpoolID)
		}
		return
	}

	var result *publisher.UploadResult
	if entry.Status == spool.StatusUploaded {
		// The bytes are already accepted by the upload transport. Only the
		// completion/commit handshake is resumable now; re-uploading could
		// create a second upload session or consume an expired target.
		uploadedBytes := entry.UploadedBytes
		if uploadedBytes <= 0 {
			uploadedBytes = entry.SizeBytes
		}
		result = &publisher.UploadResult{UploadID: entry.UploadID, UploadedBytes: uploadedBytes}
	} else {
		transport, err := w.publisherRegistry.Resolve(target.TransportID)
		if err != nil {
			w.scheduleUploadRetry(ctx, entry, err)
			return
		}
		if entry.Status == spool.StatusUploadPending {
			if err := w.outputSpool.MarkUploading(ctx, entry.SpoolID, 0); err != nil {
				if !errors.Is(err, spool.ErrCASConflict) {
					w.logger.Warn("[ARTIFACT_RESUME] mark uploading spool=%s: %v", entry.SpoolID, err)
				}
				return
			}
		}

		w.logger.Info("[ARTIFACT_RESUME] resuming upload spool=%s attempt=%d path=%q", entry.SpoolID, entry.UploadAttemptCount+1, entry.LocalPath)
		var uploadErr error
		result, uploadErr = transport.Upload(ctx, publisher.UploadRequest{
			LocalPath:    entry.LocalPath,
			Target:       target,
			WorkerSHA256: entry.SHA256,
			CommitToken:  entry.CommitToken,
		})
		if uploadErr != nil {
			w.scheduleUploadRetry(ctx, entry, uploadErr)
			return
		}
		if result == nil || result.UploadID == "" {
			w.scheduleUploadRetry(ctx, entry, errors.New("upload transport returned empty result"))
			return
		}
		if err := w.outputSpool.MarkUploaded(ctx, entry.SpoolID); err != nil {
			if !errors.Is(err, spool.ErrCASConflict) {
				w.logger.Warn("[ARTIFACT_RESUME] mark uploaded spool=%s: %v", entry.SpoolID, err)
			}
			return
		}
		w.logger.Info("[ARTIFACT_RESUME] upload resumed spool=%s upload=%s bytes=%d", entry.SpoolID, result.UploadID, result.UploadedBytes)
	}

	// Complete the attempt commit: ArtifactUploadCompleted → TaskCommitAck →
	// MarkCommitted. The completion is idempotent per upload_id, so re-sending
	// on every tick until the ack lands is safe.
	if !w.completeResumedArtifactCommit(ctx, entry, target, result) {
		w.scheduleUploadRetry(ctx, entry, errors.New("artifact commit acknowledgement not received"))
	}
}

// scheduleUploadRetry records a non-fatal failure and bumps the bounded
// retry counter with exponential backoff.
func (w *Worker) scheduleUploadRetry(ctx context.Context, entry spool.SpoolEntry, cause error) {
	next := time.Now().Add(uploadResumeBackoff(entry.UploadAttemptCount))
	if err := w.outputSpool.RecordUploadFailure(ctx, entry.SpoolID, cause.Error(), next); err != nil {
		if !errors.Is(err, spool.ErrCASConflict) {
			w.logger.Warn("[ARTIFACT_RESUME] record failure spool=%s: %v", entry.SpoolID, err)
		}
		return
	}
	w.logger.Warn("[ARTIFACT_RESUME] upload retry %d scheduled spool=%s in %s: %v", entry.UploadAttemptCount+1, entry.SpoolID, time.Until(next).Round(time.Second), cause)
}

// completeResumedArtifactCommit sends ArtifactUploadCompleted for a resumed
// upload and, on a valid TaskCommitAck, MarkCommitted. The lease_id is read
// from the worker's active-lease map (restored cross-restart by the recovery
// snapshot); without it the completion cannot be fenced and the row simply
// stays UPLOADED for a later tick.
func (w *Worker) completeResumedArtifactCommit(ctx context.Context, entry spool.SpoolEntry, target publisher.UploadTarget, result *publisher.UploadResult) bool {
	w.activeTaskLeasesMu.RLock()
	lease := w.activeTaskLeases[entry.TaskID]
	w.activeTaskLeasesMu.RUnlock()
	if lease == nil {
		w.logger.Warn("[ARTIFACT_RESUME] no active lease for task=%s spool=%s — deferring commit completion", entry.TaskID, entry.SpoolID)
		return false
	}

	completed := &pb.ArtifactUploadCompleted{
		TaskId:        entry.TaskID,
		AttemptId:     entry.AttemptID,
		CommitId:      entry.CommitID,
		LeaseId:       lease.LeaseID,
		UploadId:      result.UploadID,
		UploadedBytes: result.UploadedBytes,
		WorkerSha256:  entry.SHA256,
	}
	// Register the ack channel BEFORE sending so a fast master (or the test
	// double) can dispatch TaskCommitAck synchronously back into it — the same
	// register-before-send ordering the publish path enforces.
	ackCh := w.registerPendingArtifactAck(entry.TaskID)
	defer w.unregisterPendingArtifactAck(entry.TaskID)
	if err := w.transportSend(ctx, controltransport.NewTypedMessage(
		controltransport.MsgArtifactUploadCompleted,
		w.config.WorkerID,
		w.config.ProtocolVersion,
		completed,
	)); err != nil {
		w.logger.Warn("[ARTIFACT_RESUME] send completion failed spool=%s: %v", entry.SpoolID, err)
		return false
	}
	deadline := time.Now().Add(uploadResumeCommitWait)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	msg, err := w.waitForRegisteredArtifactAck(ctx, ackCh, deadline)
	if err != nil {
		return false // leave UPLOADED; retry on a later tick
	}
	commitAck, ok := msg.TypedPayload.(*pb.TaskCommitAck)
	if msg.Type != controltransport.MsgTaskCommitAck || !ok || commitAck == nil {
		return false
	}
	if commitAck.GetTaskId() != entry.TaskID || commitAck.GetAttemptId() != entry.AttemptID || commitAck.GetCommitId() != entry.CommitID {
		w.logger.Warn("[ARTIFACT_RESUME] commit ack fence mismatch spool=%s", entry.SpoolID)
		return false
	}
	if err := w.outputSpool.MarkCommitted(ctx, entry.SpoolID); err != nil {
		if !errors.Is(err, spool.ErrCASConflict) {
			w.logger.Warn("[ARTIFACT_RESUME] mark committed spool=%s: %v", entry.SpoolID, err)
		}
		return false
	}
	w.releaseCommittedArtifact(entry)
	w.logger.Info("[ARTIFACT_RESUME] commit completed spool=%s upload=%s", entry.SpoolID, result.UploadID)
	return true
}

// transportSend sends through the current transport under the transport mutex
// read lock (nil-safe for headless fixtures).
func (w *Worker) transportSend(ctx context.Context, msg controltransport.ControlMessage) error {
	w.transportMu.RLock()
	transport := w.transport
	w.transportMu.RUnlock()
	if transport == nil {
		return errors.New("worker transport is unavailable")
	}
	return transport.Send(ctx, msg)
}

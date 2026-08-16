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

// resumeArtifactUpload drives one resumable spool row forward. It is
// idempotent and safe to call repeatedly: state transitions are CAS-gated, so
// a concurrent driver (e.g. the original publish path) cannot be corrupted.
func (w *Worker) resumeArtifactUpload(ctx context.Context, entry spool.SpoolEntry) {
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

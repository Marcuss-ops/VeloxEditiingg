package artifacts

// Finalize orchestrates verified artifact finalization: validate session, promote staged blob, CAS upload to FINALIZING, then delegate the atomic DB transition to FinalizationWriter.

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"

	"velox-server/internal/store"
)

// Finalize orchestrates verified artifact finalization. See the
// package doc for the linear pipeline; this method is the single
// public entry point.
func (s *Service) Finalize(ctx context.Context, cmd FinalizeArtifactCommand) (*store.Artifact, error) {
	session, idempotentArtifact, err := s.validateFinalizeSession(ctx, cmd)
	if err != nil {
		return nil, err
	}
	if idempotentArtifact != nil {
		return idempotentArtifact, nil
	}

	receivedSHA := session.ReceivedSHA256
	receivedSize := session.ReceivedSizeBytes

	mimeType := detectMIME(session.TemporaryStorageKey)
	if mimeType == "" || mimeType == "application/octet-stream" {
		mimeType = session.ExpectedMIME
	}
	if mimeType == "" {
		mimeType = http.DetectContentType(nil) // returns "application/octet-stream"
	}

	storageKey, err := PromoteToCanonical(s.blobStore, session.TemporaryStorageKey, receivedSHA, mimeToExt(mimeType))
	if err != nil {
		return nil, err
	}

	// Pre-commit ffprobe invariant (RW-PROD-008 A4). Runs AFTER blob
	// promotion (file is on disk) and BEFORE the CAS RECEIVED →
	// FINALIZING transition (so a gate failure aborts cleanly without
	// any DB write: artifacts.status stays STAGING, jobs.status stays
	// RUNNING, no job_deliveries stamped). The orphan-blob pattern the
	// 24h Reconciler sweep already handles ("un blob orfano eliminabile
	// è preferibile rispetto a (artifact READY con file inesistente)")
	// covers the promoted-but-never-committed file.
	//
	// Gated on the env var VELOX_FFPROBE_VERIFY_ON_FINALIZE=true —
	// no-op when unset. Mirrors the resolution order of
	// SQLiteFinalizeWriter::resolveDeliveryDestinationsTx (override
	// → job_delivery_plans WHERE enabled=1 → fallback all-enabled).
	if err := s.runPreCommitFFProbeInvariant(ctx, cmd.JobID, cmd.DestinationID,
		filepath.Join(s.blobStore.FinalDir(), filepath.FromSlash(storageKey)),
	); err != nil {
		return nil, err
	}

	// CAS-gate the RECEIVED → FINALIZING transition. This serializes
	// concurrent Finalize callers at the SQL layer — only the first
	// writer successfully flips status; the loser gets ErrTransitionConflict
	// and the caller retries. After the winner's tx commits + flips
	// FINALIZING → COMPLETED (in-tx, see FinalizeVerified step 7),
	// the loser's retry hits the idempotent COMPLETED short-circuit
	// path above.
	//
	// For status FINALIZING (a peer is mid-flight) we DO NOT skip the
	// CAS — we explicitly reject with ErrTransitionConflict so the
	// caller retries. Without this gate the loser would fall through
	// to PromoteToCanonical and run a SECOND os.Rename to the same
	// canonical path, racing the winner's first promote.
	switch session.Status {
	case string(store.UploadReceived):
		if err := s.repo.TransitionUploadStatus(ctx, cmd.UploadID, string(store.UploadReceived), string(store.UploadFinalizing)); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrTransitionConflict, translateStoreErr(err))
		}
	case string(store.UploadFinalizing):
		return nil, fmt.Errorf("%w: upload=%s peer-mid-flight",
			ErrTransitionConflict, cmd.UploadID)
	default:
		// Defence-in-depth: any other status (CREATED, UPLOADING,
		// EXPIRED, FAILED) cannot enter the finalization critical
		// section. validateFinalizeSession already filters these out,
		// but a future refactor without that filter must not silently
		// fall through.
		return nil, fmt.Errorf("%w: upload=%s status=%s unexpected",
			ErrUploadStateInvalid, cmd.UploadID, session.Status)
	}

	command := s.buildFinalizeVerifiedCommand(cmd, session, storageKey, receivedSHA, receivedSize, mimeType)
	art, err := s.finalizeWithDuplicateStorageFallback(ctx, command)
	if err != nil {
		return nil, err
	}
	return art, nil
}

// validateFinalizeSession loads + validates the upload session.
// 3-tuple return: dual-path COMPLETED vs FINALIZING dispatch.
//
//   - (session, nil, nil):    session is RECEIVED/FINALIZING; caller
//     proceeds with the finalize pipeline.
//   - (nil, artifact, nil):   idempotent COMPLETED path; caller
//     returns the artifact immediately (no further work).
//   - (nil, nil, err):        validation failed; caller propagates err.
//
// The 3-tuple is required so the orchestrator can dispatch the
// idempotent-vs-finalize decision without re-loading the session
// (which would be racy).
func (s *Service) validateFinalizeSession(ctx context.Context, cmd FinalizeArtifactCommand) (*store.UploadSession, *store.Artifact, error) {
	if cmd.UploadID == "" {
		return nil, nil, fmt.Errorf("artifacts: Finalize: empty uploadID")
	}
	session, err := s.repo.GetUploadSession(ctx, cmd.UploadID)
	if err != nil {
		return nil, nil, translateStoreErr(err)
	}
	if session == nil {
		return nil, nil, fmt.Errorf("%w: upload_id=%s", ErrUploadNotFound, cmd.UploadID)
	}
	if session.JobID != cmd.JobID {
		return nil, nil, fmt.Errorf("%w: session_job=%s cmd_job=%s",
			ErrTransitionConflict, session.JobID, cmd.JobID)
	}

	// ----- idempotent COMPLETED path (spec: "doppia finalizzazione") -----
	// If the previous tx already committed, the session row is COMPLETED;
	// we re-load the post-tx artifact and return it when auth fields
	// match. Different worker/lease/revision still error (a duplicate
	// retry from a *different* worker must NOT silently succeed).
	if session.Status == "COMPLETED" {
		if session.WorkerID != cmd.WorkerID {
			return nil, nil, fmt.Errorf("%w: completed upload=%s worker=%s->%s",
				ErrTransitionConflict, cmd.UploadID, session.WorkerID, cmd.WorkerID)
		}
		if session.LeaseID != cmd.LeaseID {
			return nil, nil, fmt.Errorf("%w: completed upload=%s lease_mismatch",
				ErrTransitionConflict, cmd.UploadID)
		}
		if session.ExpectedRevision != 0 &&
			session.ExpectedRevision != cmd.ExpectedRevision {
			return nil, nil, fmt.Errorf("%w: completed upload=%s revision_mismatch",
				ErrTransitionConflict, cmd.UploadID)
		}
		if cmd.AttemptNumber != 0 && session.AttemptNumber != cmd.AttemptNumber {
			return nil, nil, fmt.Errorf("%w: completed upload=%s attempt=%d->%d",
				ErrAttemptMismatch, cmd.UploadID, session.AttemptNumber, cmd.AttemptNumber)
		}
		art, lerr := s.artifactReader.GetByID(ctx, session.ArtifactID)
		if lerr != nil {
			return nil, nil, lerr
		}
		if art == nil {
			return nil, nil, fmt.Errorf("%w: completed upload=%s but artifact missing",
				ErrTransitionConflict, cmd.UploadID)
		}
		// Note: the pre-commit gate in the main Finalize path runs the
		// ffprobe check on the first finalize. This COMPLETED short-
		// circuit simply returns the cached artifact — no ffprobe re-run
		// here, since the gate fired (and possibly tripped) before the
		// tx commit that produced this exact artifact.
		return nil, art, nil
	}

	if session.Status != "RECEIVED" && session.Status != "FINALIZING" {
		return nil, nil, fmt.Errorf("%w: upload_id=%s status=%s",
			ErrUploadStateInvalid, cmd.UploadID, session.Status)
	}
	if session.WorkerID != cmd.WorkerID {
		return nil, nil, fmt.Errorf("%w: upload=%s expected_worker=%s got=%s",
			ErrWrongJobOwner, cmd.UploadID, session.WorkerID, cmd.WorkerID)
	}
	if session.LeaseID != cmd.LeaseID {
		return nil, nil, fmt.Errorf("%w: upload=%s lease_mismatch", ErrLeaseInvalid, cmd.UploadID)
	}
	if session.ExpectedRevision != 0 && session.ExpectedRevision != cmd.ExpectedRevision {
		return nil, nil, fmt.Errorf("%w: upload=%s expected_revision=%d got=%d",
			ErrRevisionMismatch, cmd.UploadID, session.ExpectedRevision, cmd.ExpectedRevision)
	}
	if cmd.AttemptNumber != 0 && session.AttemptNumber != cmd.AttemptNumber {
		return nil, nil, fmt.Errorf("%w: upload=%s expected_attempt=%d got=%d",
			ErrAttemptMismatch, cmd.UploadID, session.AttemptNumber, cmd.AttemptNumber)
	}

	receivedSHA := session.ReceivedSHA256
	receivedSize := session.ReceivedSizeBytes
	if receivedSHA == "" || receivedSize == 0 {
		return nil, nil, fmt.Errorf("%w: upload=%s has empty received snapshot (Receive() not completed?)",
			ErrUploadStateInvalid, cmd.UploadID)
	}

	return session, nil, nil
}

// buildFinalizeVerifiedCommand constructs the writer command from
// pre-validated session data + the just-promoted storage key + the
// just-detected MIME type. Pure struct mapping — no fallible
// operations in any field, so the error return is omitted.
// Reintroduce it deliberately (not by accident) if a future change
// adds a fallible mapping (e.g. keyed-JSON-derived artifact_id).
func (s *Service) buildFinalizeVerifiedCommand(
	cmd FinalizeArtifactCommand,
	session *store.UploadSession,
	storageKey, receivedSHA string,
	receivedSize int64,
	mimeType string,
) FinalizeVerifiedCommand {
	return FinalizeVerifiedCommand{
		UploadID:         cmd.UploadID,
		ArtifactID:       session.ArtifactID,
		JobID:            cmd.JobID,
		WorkerID:         cmd.WorkerID,
		LeaseID:          cmd.LeaseID,
		AttemptNumber:    session.AttemptNumber,
		ExpectedRevision: session.ExpectedRevision,

		StorageProvider: "local",
		StorageKey:      storageKey,
		SHA256:          receivedSHA,
		SizeBytes:       receivedSize,
		MIMEType:        mimeType,

		VerifiedAt: s.clock.Now(),
	}
}

// finalizeWithDuplicateStorageFallback invokes the atomic verified-
// writer once. If the writer returns the UNIQUE-constraint conflict
// that signals another artifact row already owns the same
// (storage_provider, storage_key) tuple, the helper:
//
//  1. Derives an alt storage_key from the artifact id.
//  2. Materializes a hardlink (or copy fallback) of the original
//     canonical blob to the alt key on disk.
//  3. Mutates the command's StorageKey field and retries the writer.
//
// Any non-conflict error is propagated verbatim so the orchestrator
// surfaces it unchanged.
func (s *Service) finalizeWithDuplicateStorageFallback(ctx context.Context, command FinalizeVerifiedCommand) (*store.Artifact, error) {
	out, err := s.finalizeWriter.FinalizeVerified(ctx, command)
	if err == nil {
		return out, nil
	}
	if !isArtifactStorageKeyConflict(err) {
		return nil, err
	}
	altStorageKey, keyErr := makeDuplicateStorageKey(command.StorageKey, command.ArtifactID)
	if keyErr != nil {
		return nil, err
	}
	if dupErr := s.materializeDuplicateFinalBlob(command.StorageKey, altStorageKey); dupErr != nil {
		return nil, fmt.Errorf("artifacts: duplicate final blob fallback: %w", dupErr)
	}
	command.StorageKey = altStorageKey
	return s.finalizeWriter.FinalizeVerified(ctx, command)
}

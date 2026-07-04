package artifacts

// Package artifacts / service_finalize.go
//
// Finalize orchestrates the verified-finalization pipeline. The 388-
// line monolith has been split into private helpers per the action-
// plan P0 refactor:
//
//  1. validateFinalizeSession           (this file, private) — upload
//     session load + auth match (job-id / worker / lease / revision /
//     attempt) + idempotent COMPLETED shortcut (returns the
//     post-Finalize artifact via ArtifactReader when the previous tx
//     already committed).
//
//  2. detectMIME                         (this file, private) — content
//     sniff of the staged blob; falls back to BeginUpload-declared
//     mime; falls back to application/octet-stream.
//
//  3. PromoteToCanonical                 (storage.go, package-private)
//     — promotes the blob to its content-addressable canonical
//     storage_key BEFORE the SQL tx.
//
//  4. CAS RECEIVED → FINALIZING          (this file's orchestrator)
//     — sql-level gate against concurrent Finalize callers.
//
//  5. buildFinalizeVerifiedCommand       (this file, private) — pure
//     struct mapping for the verified-writer command.
//
//  6. finalizeWithDuplicateStorageFallback (this file, private) —
//     single-shot FinalizeVerified with a UNIQUE-constraint
//     retry against an alt storage_key. The three
//     supporting helpers (isArtifactStorageKeyConflict,
//     makeDuplicateStorageKey, materializeDuplicateFinalBlob) live
//     in service_duplicate_blob.go of the same package.
//
// Blob promotion runs BEFORE the SQL tx. If the SQL tx rolls back
// after the duplicate-fallback retry, the reconciler deletes the
// orphan blob; "un blob orfano eliminabile è preferibile rispetto a
// (artifact READY con file inesistente)".

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"velox-server/internal/store"
)

// Finalize orchestrates the verified-finalization pipeline. Linear:
//
//	validate session → [idempotent COMPLETED short-circuit]
//	detect MIME → promote canonical blob
//	CAS RECEIVED → FINALIZING
//	build verified-writer command → write (with duplicate-key fallback)
//
// Public surface of `package artifacts` is unchanged: Finalize, its
// parameters, and its return values are byte-identical to the prior
// monolithic implementation.
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
	return s.finalizeWithDuplicateStorageFallback(ctx, command)
}

// validateFinalizeSession loads + validates the upload session.
//
// 3-tuple return semantics (deviates from the original spec's
// `(ctx, sessionID, requesterID) error` to preserve behavior):
//
//   - (session, nil, nil):    session is RECEIVED/FINALIZING; caller
//     proceeds with the finalize pipeline.
//   - (nil, artifact, nil):   idempotent COMPLETED path; caller
//     returns the artifact immediately (no further work).
//   - (nil, nil, err):        validation failed; caller propagates err.
//
// Why 3 values and not just error: the COMPLETED-vs-FINALIZING
// branching in the original monolith needs to surface BOTH the
// "yes, just return this cached artifact" AND the "yes, proceed
// with the pipeline" decisions. Collapsing to a single error return
// would force the caller to re-load the session and dispatch the
// idempotent-vs-finalize decision itself — fragile and racy.
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
// just-detected MIME type. Pure struct mapping — no error possible.
//
// Note: the original spec called for an `error` return; it is
// intentionally elided here because every field is computed from
// in-memory validated inputs (no fallible operations like JSON
// marshal or DB reads). If a future change introduces a fallible
// mapping (e.g. keyed-JSON-derived artifact_id), reintroduce the
// error return deliberately, not by accident.
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
// (storage_provider, storage_key) tuple (rare; the reconciler should
// have cleaned the orphan — but rare upgrades + storage migrations
// have surfaced this conflict historically), the helper:
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

// detectMIME sniffs the first 512 bytes and returns the canonical
// MIME type. Falls back to "" when the file cannot be read.
//
// Module-local helper (not in service_mime.go) to minimize file
// count in this package — the function is small, pure, and only
// called from Finalize. Moving it out is mechanical if a future
// refactor adds call sites in other methods.
func detectMIME(path string) string {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return ""
	}
	defer f.Close()
	var sniff [512]byte
	n, _ := io.ReadFull(f, sniff[:])
	return http.DetectContentType(sniff[:n])
}

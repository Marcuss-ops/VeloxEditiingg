// Package spool — store_transitions.go
//
// Lifecycle transitions of the worker_output_spool row. Every public
// method on *Store in this file is a CAS-gated status move: the SQL
// UPDATE always carries a `WHERE status = expected_from` so a late
// upload thread cannot overwrite a final REJECTED or CLEANED state.
// Same `package spool` so the transition helper and the stringsOrDash
// formatter keep cross-file private-symbol access to Status,
// ErrCASConflict, ErrInvalidStatus declared in store.go.
//
// Owned funcs:
//
//	MarkReady       RENDERING     → OUTPUT_READY
//	MarkUploadPending OUTPUT_READY → UPLOAD_PENDING
//	MarkUploading   UPLOAD_PENDING → UPLOADING
//	RecordProgress  (no status move; UPLOADING → bumped UploadedBytes)
//	MarkUploaded    UPLOADING     → UPLOADED
//	MarkCommitted   UPLOADED      → COMMITTED
//	MarkRejected    any mid-upload → REJECTED  (terminal guard)
//	MarkCleaned     COMMITTED|REJECTED → CLEANED
//	Delete          (admin/cleanup)
//
// Read-side (Insert, Get, List*) lives in `store_queries.go`.
package spool

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// ────────────────────────────────────────────────────────────────────────
// Lifecycle transitions — every step is CAS-gated on the
// expected_from status.
// ────────────────────────────────────────────────────────────────────────

// MarkReady transitions RENDERING → OUTPUT_READY, stamping the
// SHA-256 (mandatory) and SizeBytes. Idempotent if the row is already
// OUTPUT_READY (returns nil ErrCASConflict).
func (s *Store) MarkReady(ctx context.Context, spoolID, sha256Hex string, sizeBytes int64) error {
	if len(sha256Hex) != 64 {
		return fmt.Errorf("spool.MarkReady: sha256 must be 64 hex chars (got %d)", len(sha256Hex))
	}
	return s.transition(ctx, spoolID, StatusRendering, StatusOutputReady, map[string]any{
		"sha256":     sha256Hex,
		"size_bytes": sizeBytes,
	})
}

// MarkUploadPending transitions OUTPUT_READY → UPLOAD_PENDING,
// stamping the master-assigned upload_id.
func (s *Store) MarkUploadPending(ctx context.Context, spoolID, uploadID string) error {
	if uploadID == "" {
		return fmt.Errorf("spool.MarkUploadPending: upload_id empty")
	}
	return s.transition(ctx, spoolID, StatusOutputReady, StatusUploadPending, map[string]any{
		"upload_id": uploadID,
	})
}

// MarkUploading transitions UPLOAD_PENDING → UPLOADING plus stashes
// the running bytes counter.
func (s *Store) MarkUploading(ctx context.Context, spoolID string, uploadedBytes int64) error {
	return s.transition(ctx, spoolID, StatusUploadPending, StatusUploading, map[string]any{
		"uploaded_bytes": uploadedBytes,
	})
}

// StampContent records the final content identity without changing the
// lifecycle state. Early progressive uploads need this when the normal
// declaration arrives after bytes have already started moving.
func (s *Store) StampContent(ctx context.Context, spoolID, sha256Hex string, sizeBytes int64) error {
	if len(sha256Hex) != 64 || sizeBytes <= 0 {
		return fmt.Errorf("spool.StampContent: invalid content identity")
	}
	res, err := s.db.ExecContext(ctx, `
		UPDATE worker_output_spool
		   SET sha256 = ?, size_bytes = ?, updated_at = ?
		 WHERE spool_id = ? AND status IN ('RENDERING','OUTPUT_READY','UPLOAD_PENDING','UPLOADING','UPLOADED')`,
		sha256Hex, sizeBytes, time.Now().UTC().Format(time.RFC3339Nano), spoolID)
	if err != nil {
		return fmt.Errorf("spool.StampContent: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("%w: spool=%s", ErrCASConflict, spoolID)
	}
	return nil
}

// StashEarlyUploadPlan persists the pre-declaration target and moves the row
// into the normal upload-resume set. CommitID remains empty until the normal
// TaskOutputDeclared plan arrives; resumeArtifactUpload deliberately uploads
// bytes but defers the fenced completion message until then.
func (s *Store) StashEarlyUploadPlan(ctx context.Context, spoolID, uploadID, targetJSON, commitToken string) error {
	if spoolID == "" || uploadID == "" {
		return fmt.Errorf("spool.StashEarlyUploadPlan: identity empty")
	}
	res, err := s.db.ExecContext(ctx, `
		UPDATE worker_output_spool
		   SET upload_id = ?, upload_target_json = ?, commit_token = ?,
		       status = 'UPLOAD_PENDING', updated_at = ?
		 WHERE spool_id = ? AND status = 'RENDERING'`,
		uploadID, targetJSON, commitToken, time.Now().UTC().Format(time.RFC3339Nano), spoolID)
	if err != nil {
		return fmt.Errorf("spool.StashEarlyUploadPlan: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("%w: spool=%s (expected status=RENDERING)", ErrCASConflict, spoolID)
	}
	return nil
}

// StashUploadPlan persists the master's per-artifact upload target (and the
// attempt's commit_id + short-lived commit token). It accepts both the normal
// OUTPUT_READY declaration window and an early row already in
// UPLOAD_PENDING/UPLOADING/UPLOADED, so the final declaration can enrich a
// target persisted before rendering finished.
func (s *Store) StashUploadPlan(ctx context.Context, spoolID, commitID, uploadID, targetJSON, commitToken string) error {
	if spoolID == "" {
		return fmt.Errorf("spool.StashUploadPlan: spool_id empty")
	}
	if uploadID == "" {
		return fmt.Errorf("spool.StashUploadPlan: upload_id empty")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := s.db.ExecContext(ctx, `
		UPDATE worker_output_spool
		   SET commit_id = ?, upload_id = ?, upload_target_json = ?,
		       commit_token = ?, updated_at = ?
		 WHERE spool_id = ? AND status IN ('OUTPUT_READY','UPLOAD_PENDING','UPLOADING','UPLOADED')`,
		commitID, uploadID, targetJSON, commitToken, now, spoolID,
	)
	if err != nil {
		return fmt.Errorf("spool.StashUploadPlan: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("%w: spool=%s (expected resumable status)", ErrCASConflict, spoolID)
	}
	return nil
}

// RecordUploadFailure stamps a non-fatal declare, upload, or
// commit-completion failure onto a resumable row WITHOUT moving its status.
// OUTPUT_READY is included so the declare-resume loop can back off a failed
// TaskOutputDeclared send; UPLOADED is included because the bytes may already
// be accepted while the TaskCommitAck was lost (that row must retry the
// completion message, not start a second upload). It bumps the bounded
// attempt counter and schedules the next retry instant (nextAttemptAt; zero =
// immediate). Terminal rows are never re-opened.
func (s *Store) RecordUploadFailure(ctx context.Context, spoolID, lastError string, nextAttemptAt time.Time) error {
	if spoolID == "" {
		return fmt.Errorf("spool.RecordUploadFailure: spool_id empty")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := s.db.ExecContext(ctx, `
		UPDATE worker_output_spool
		   SET last_error = ?, upload_attempt_count = upload_attempt_count + 1,
		       next_upload_attempt_at = ?, updated_at = ?
			 WHERE spool_id = ? AND status IN ('OUTPUT_READY','UPLOAD_PENDING','UPLOADING','UPLOADED')`,
		lastError, formatUploadAttemptAt(nextAttemptAt), now, spoolID,
	)
	if err != nil {
		return fmt.Errorf("spool.RecordUploadFailure: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("%w: spool=%s (expected mid-upload state)", ErrCASConflict, spoolID)
	}
	return nil
}

// RecordProgress bumps UploadedBytes while still in UPLOADING. NOT a
// status transition; idempotent.
func (s *Store) RecordProgress(ctx context.Context, spoolID string, uploadedBytes int64) error {
	if uploadedBytes < 0 {
		return fmt.Errorf("spool.RecordProgress: uploaded_bytes < 0")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := s.db.ExecContext(ctx, `
		UPDATE worker_output_spool
		   SET uploaded_bytes = ?, updated_at = ?
		 WHERE spool_id = ? AND status = ?`,
		uploadedBytes, now, spoolID, string(StatusUploading),
	)
	if err != nil {
		return fmt.Errorf("spool.RecordProgress: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("%w: spool=%s (expected status=UPLOADING)", ErrCASConflict, spoolID)
	}
	return nil
}

// MarkUploaded transitions UPLOADING → UPLOADED. The supervisor's
// audit contract binds this state to the master CompleteUpload ack.
func (s *Store) MarkUploaded(ctx context.Context, spoolID string) error {
	return s.transition(ctx, spoolID, StatusUploading, StatusUploaded, nil)
}

// MarkCommitted transitions UPLOADED → COMMITTED. The row stays
// alive until MarkCleaned runs after the row was acknowledged by
// the master and the local file was deleted.
func (s *Store) MarkCommitted(ctx context.Context, spoolID string) error {
	return s.transition(ctx, spoolID, StatusUploaded, StatusCommitted, nil)
}

// MarkRejected transitions any mid-upload state to REJECTED. The
// LastError field is populated; the row stays alive for forensics.
//
// Per spec the reject path is `any_of(OUTPUT_READY | UPLOAD_PENDING |
// UPLOADING | UPLOADED) → REJECTED`. RENDERING (no artifact on disk
// yet) and the terminal states (COMMITTED, CLEANED, REJECTED) are
// explicitly excluded so a late reject cannot overwrite an
// already-final state and so a render that never produced output
// does not get a phantom REJECTED row.
func (s *Store) MarkRejected(ctx context.Context, spoolID, code, message string) error {
	if spoolID == "" {
		return fmt.Errorf("spool.MarkRejected: spool_id empty")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	lastError := stringsOrDash(code, message)
	res, err := s.db.ExecContext(ctx, `
		UPDATE worker_output_spool
		   SET status = ?, last_error = ?, updated_at = ?
		 WHERE spool_id = ? AND status IN ('OUTPUT_READY','UPLOAD_PENDING','UPLOADING','UPLOADED')`,
		string(StatusRejected), lastError, now, spoolID,
	)
	if err != nil {
		return fmt.Errorf("spool.MarkRejected: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("%w: spool=%s (expected mid-upload state)", ErrCASConflict, spoolID)
	}
	return nil
}

// MarkSpilled repoints a volatile (tmpfs) artifact to a durable NVMe path
// after a spill (upload failure or graceful shutdown). It is CAS-gated to
// the mid-upload states so a terminal row (COMMITTED / REJECTED / CLEANED)
// cannot be repointed. The caller is responsible for copying the bytes and
// unlinking the tmpfs source; this transition only flips the durable
// pointer + tier.
func (s *Store) MarkSpilled(ctx context.Context, spoolID, newLocalPath string) error {
	if spoolID == "" {
		return fmt.Errorf("spool.MarkSpilled: spool_id empty")
	}
	if newLocalPath == "" {
		return fmt.Errorf("spool.MarkSpilled: new_local_path empty")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := s.db.ExecContext(ctx, `
		UPDATE worker_output_spool
		   SET local_path = ?, storage_tier = ?, updated_at = ?
		 WHERE spool_id = ? AND status IN ('OUTPUT_READY','UPLOAD_PENDING','UPLOADING','UPLOADED')`,
		newLocalPath, string(StorageTierNvmeDurable), now, spoolID,
	)
	if err != nil {
		return fmt.Errorf("spool.MarkSpilled: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("%w: spool=%s (expected mid-upload state)", ErrCASConflict, spoolID)
	}
	return nil
}

// MarkCleaned transitions COMMITTED | REJECTED → CLEANED. After
// Cleaned the row is audit-only and the local_path is expected to be
// empty (caller is responsible for unlinking the file).
func (s *Store) MarkCleaned(ctx context.Context, spoolID string) error {
	if spoolID == "" {
		return fmt.Errorf("spool.MarkCleaned: spool_id empty")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := s.db.ExecContext(ctx, `
		UPDATE worker_output_spool
		   SET status = ?, local_path = '', updated_at = ?
		 WHERE spool_id = ? AND status IN ('COMMITTED','REJECTED')`,
		string(StatusCleaned), now, spoolID,
	)
	if err != nil {
		return fmt.Errorf("spool.MarkCleaned: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("%w: spool=%s (expected COMMITTED|REJECTED)", ErrCASConflict, spoolID)
	}
	return nil
}

// Delete hard-deletes the row. Reserved for cleanup tools and tests.
func (s *Store) Delete(ctx context.Context, spoolID string) error {
	if _, err := s.db.ExecContext(ctx,
		`DELETE FROM worker_output_spool WHERE spool_id = ?`, spoolID); err != nil {
		return fmt.Errorf("spool.Delete: %w", err)
	}
	return nil
}

// transition is the canonical CAS-gated status move. Sets the optional
// column overrides (pass nil if no extra columns are needed) and
// stamps updated_at. Column overrides are iterated in deterministic
// alphabetical key order so the SQL placeholder sequence is stable
// across runs (helps test debugging and log diffing).
func (s *Store) transition(ctx context.Context, spoolID string, from, to Status, extras map[string]any) error {
	if !to.IsValid() {
		return fmt.Errorf("%w: %q", ErrInvalidStatus, to)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)

	// Build the SET clause from the optional overrides. The two
	// shapes collapse into one — always iterate `extras` (Go's
	// randomized map iteration is OK because we sort the keys
	// below for placeholder stability).
	keys := make([]string, 0, len(extras))
	for k := range extras {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var setExtras string
	var args []any
	for _, k := range keys {
		setExtras += ", " + k + " = ?"
		args = append(args, extras[k])
	}
	args = append([]any{string(to)}, args...)
	args = append(args, now, spoolID, string(from))

	q := `UPDATE worker_output_spool
	         SET status = ?` + setExtras + `, updated_at = ?
	        WHERE spool_id = ? AND status = ?`
	res, err := s.db.ExecContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("spool.transition: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("%w: spool=%s (expected status=%s)", ErrCASConflict, spoolID, from)
	}
	return nil
}

// ────────────────────────────────────────────────────────────────────────
// helpers
// ────────────────────────────────────────────────────────────────────────

// stringsOrDash canonicalizes a (code, message) tuple into the
// LastError column. Either component missing becomes "-" so the
// audit string stays single-line and grep-friendly.
func stringsOrDash(code, message string) string {
	if code == "" && message == "" {
		return "-"
	}
	if message == "" {
		return code
	}
	if code == "" {
		return "- " + message
	}
	return code + ": " + message
}

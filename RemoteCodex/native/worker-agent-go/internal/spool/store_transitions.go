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

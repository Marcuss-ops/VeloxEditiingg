// Package spool — store_queries.go
//
// Read-side and create-side of the worker_output_spool store. Same
// `package spool` so private symbols (Status, SpoolEntry, Err*,
// Store.db) declared in store.go remain in scope without re-export.
// Owned funcs:
//
//   - Insert (+ newSpoolID for ID generation)
//   - Get
//   - ListByStatus / ListByAttempt / ListResumeCandidates
//   - scanSpool (+ parseRFC3339Nano time-codec) + selectSpoolCols /
//     selectSpoolBySpoolID / selectSpoolByStatus SQL constants
//   - isUniqueConflict (+ containsCI case-insensitive substring match)
//
// Lifecycle transitions (MarkReady … MarkCleaned + the CAS helper)
// live in `store_transitions.go`.
package spool

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

// ────────────────────────────────────────────────────────────────────────
// Insert / lookup / list.
// ────────────────────────────────────────────────────────────────────────

// Ensure registers a spool entry idempotently. It is the SINGLE
// registration surface publishers use — callers MUST NOT catch
// ErrDuplicateSpool and branch on it (that logic lives here, once).
//
// Semantics:
//
//	row does not exist
//	    → INSERT (same as Insert) → created=true
//	row exists with the same identity AND compatible content
//	    → RETURN existing row → created=false
//	row exists with the same identity but INCOMPATIBLE content
//	    (sha256 / size_bytes differ) → *ErrIncompatibleSpool
//
// "Compatible content" means the existing row's content fingerprint
// (sha256 + size_bytes) either matches the incoming entry OR was never
// stamped (the row was created but MarkReady never ran — the caller's
// MarkReady completes it). local_path is deliberately NOT part of the
// compatibility check: it is a physical location that may legitimately
// change (spill, re-render to a different staging dir) without making
// the logical output a different artifact. The durable identity is the
// content.
//
// The returned row is the authoritative one (the existing row on a
// duplicate, the freshly-inserted row otherwise). created reports
// whether this call performed the INSERT.
func (s *Store) Ensure(ctx context.Context, e SpoolEntry) (*SpoolEntry, bool, error) {
	if e.TaskID == "" || e.AttemptID == "" || e.WorkerSpoolKey == "" {
		return nil, false, fmt.Errorf("spool.Ensure: TaskID, AttemptID, WorkerSpoolKey are required")
	}
	// Fast path: the row already exists for the identity tuple.
	existing, err := s.getByIdentity(ctx, e.TaskID, e.AttemptID, e.WorkerSpoolKey)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, false, fmt.Errorf("spool.Ensure: lookup: %w", err)
	}
	if existing != nil {
		if !spoolContentMatches(existing, &e) {
			return nil, false, fmt.Errorf(
				"%w: (task_id=%s attempt_id=%s worker_spool_key=%s) existing_sha256=%s incoming_sha256=%s existing_size=%d incoming_size=%d",
				ErrIncompatibleSpool, e.TaskID, e.AttemptID, e.WorkerSpoolKey,
				existing.SHA256, e.SHA256, existing.SizeBytes, e.SizeBytes)
		}
		return existing, false, nil
	}

	created, err := s.Insert(ctx, e)
	if err != nil {
		// A concurrent Ensure may have inserted the row between our lookup
		// and insert. Re-read and classify instead of surfacing the raw
		// duplicate to the caller.
		if errors.Is(err, ErrDuplicateSpool) {
			again, getErr := s.getByIdentity(ctx, e.TaskID, e.AttemptID, e.WorkerSpoolKey)
			if getErr != nil {
				return nil, false, fmt.Errorf("spool.Ensure: post-conflict lookup: %w", getErr)
			}
			if again == nil {
				return nil, false, fmt.Errorf("spool.Ensure: duplicate reported but row not found: %w", err)
			}
			if !spoolContentMatches(again, &e) {
				return nil, false, fmt.Errorf(
					"%w: (task_id=%s attempt_id=%s worker_spool_key=%s) existing_sha256=%s incoming_sha256=%s existing_size=%d incoming_size=%d",
					ErrIncompatibleSpool, e.TaskID, e.AttemptID, e.WorkerSpoolKey,
					again.SHA256, e.SHA256, again.SizeBytes, e.SizeBytes)
			}
			return again, false, nil
		}
		return nil, false, err
	}
	return created, true, nil
}

// getByIdentity looks up the single row for the UNIQUE identity tuple
// (task_id, attempt_id, worker_spool_key), returning ErrNotFound when it
// does not exist.
func (s *Store) getByIdentity(ctx context.Context, taskID, attemptID, workerSpoolKey string) (*SpoolEntry, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+selectSpoolCols+` FROM worker_output_spool
		  WHERE task_id = ? AND attempt_id = ? AND worker_spool_key = ?`,
		taskID, attemptID, workerSpoolKey)
	return scanSpool(row)
}

// spoolContentMatches reports whether an existing row is a benign
// duplicate of the incoming entry: either the existing row never had
// content stamped (MarkReady never ran — the caller's MarkReady
// completes it) or its content fingerprint (sha256 + size_bytes)
// matches the incoming entry. local_path is intentionally ignored (it
// is a physical location that can legitimately change).
func spoolContentMatches(existing, incoming *SpoolEntry) bool {
	if existing == nil || incoming == nil {
		return false
	}
	if existing.SHA256 == "" && existing.SizeBytes == 0 {
		// Row created but never finalized; the caller stamps content.
		return true
	}
	return existing.SHA256 == incoming.SHA256 && existing.SizeBytes == incoming.SizeBytes
}

// Insert registers a new spool entry in StatusRendering. The unique
// tuple (task_id, attempt_id, worker_spool_key) prevents the same
// worker from double-spooling the same logical output.
//
// Insert is the low-level create primitive. Publishers MUST use Ensure
// (the idempotent surface) instead of Insert so a duplicate registration
// converges on the existing row rather than an ErrDuplicateSpool failure.
//
// Returns the SpoolEntry with SpoolID + CreatedAt stamped.
func (s *Store) Insert(ctx context.Context, e SpoolEntry) (*SpoolEntry, error) {
	if e.TaskID == "" || e.AttemptID == "" || e.WorkerSpoolKey == "" {
		return nil, fmt.Errorf("spool.Insert: TaskID, AttemptID, WorkerSpoolKey are required")
	}
	if e.Status == "" {
		e.Status = StatusRendering
	}
	if !e.Status.IsValid() {
		return nil, fmt.Errorf("%w: %q", ErrInvalidStatus, e.Status)
	}
	if e.StorageTier == "" {
		e.StorageTier = StorageTierNvmeDurable
	}
	if !e.StorageTier.IsValid() {
		return nil, fmt.Errorf("%w: storage_tier %q", ErrInvalidStatus, e.StorageTier)
	}
	if e.SpoolID == "" {
		e.SpoolID = newSpoolID()
	}
	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339Nano)
	e.CreatedAt = now
	e.UpdatedAt = now

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO worker_output_spool (
		    spool_id, task_id, attempt_id, commit_id, worker_spool_key,
		    local_path, sha256, size_bytes, upload_id, uploaded_bytes,
		    status, storage_tier, last_error,
		    upload_target_json, commit_token, upload_attempt_count,
		    next_upload_attempt_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		e.SpoolID, e.TaskID, e.AttemptID, e.CommitID, e.WorkerSpoolKey,
		e.LocalPath, e.SHA256, e.SizeBytes, e.UploadID, e.UploadedBytes,
		string(e.Status), string(e.StorageTier), e.LastError,
		e.UploadTargetJSON, e.CommitToken, e.UploadAttemptCount,
		formatUploadAttemptAt(e.NextUploadAttemptAt), nowStr, nowStr,
	)
	if err != nil {
		if isUniqueConflict(err) {
			return nil, fmt.Errorf("%w: (task_id=%s attempt_id=%s worker_spool_key=%s)",
				ErrDuplicateSpool, e.TaskID, e.AttemptID, e.WorkerSpoolKey)
		}
		return nil, fmt.Errorf("spool.Insert: %w", err)
	}
	return &e, nil
}

// Get returns the row by SpoolID, or ErrNotFound.
func (s *Store) Get(ctx context.Context, spoolID string) (*SpoolEntry, error) {
	row := s.db.QueryRowContext(ctx, selectSpoolBySpoolID, spoolID)
	return scanSpool(row)
}

// ListByStatus returns all rows in a given status. Used by supervisor
// scans + observability bursts.
func (s *Store) ListByStatus(ctx context.Context, status Status) ([]SpoolEntry, error) {
	rows, err := s.db.QueryContext(ctx, selectSpoolByStatus, string(status))
	if err != nil {
		return nil, fmt.Errorf("spool.ListByStatus: %w", err)
	}
	defer rows.Close()
	var out []SpoolEntry
	for rows.Next() {
		e, err := scanSpool(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *e)
	}
	return out, rows.Err()
}

// ListByAttempt returns all rows for (TaskID, AttemptID), in time
// order.
func (s *Store) ListByAttempt(ctx context.Context, taskID, attemptID string) ([]SpoolEntry, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+selectSpoolCols+` FROM worker_output_spool
		  WHERE task_id = ? AND attempt_id = ?
		  ORDER BY created_at ASC`, taskID, attemptID)
	if err != nil {
		return nil, fmt.Errorf("spool.ListByAttempt: %w", err)
	}
	defer rows.Close()
	var out []SpoolEntry
	for rows.Next() {
		e, err := scanSpool(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *e)
	}
	return out, rows.Err()
}

// ListResumeCandidates returns rows that are eligible for resume on
// worker restart: anything between OUTPUT_READY and UPLOADED (mid-
// upload states). REJECTED / COMMITTED / CLEANED are excluded.
func (s *Store) ListResumeCandidates(ctx context.Context) ([]SpoolEntry, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+selectSpoolCols+` FROM worker_output_spool
		  WHERE status IN ('OUTPUT_READY','UPLOAD_PENDING','UPLOADING','UPLOADED')
		  ORDER BY created_at ASC`)
	if err != nil {
		return nil, fmt.Errorf("spool.ListResumeCandidates: %w", err)
	}
	defer rows.Close()
	var out []SpoolEntry
	for rows.Next() {
		e, err := scanSpool(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *e)
	}
	return out, rows.Err()
}

// ListUploadResumeCandidates returns mid-upload rows that have a persisted
// upload target (StashUploadPlan ran), so the worker can re-drive their
// upload or re-send the commit completion from the (possibly repointed)
// local_path. UPLOADED rows are included because the bytes may already be
// accepted by the transport while the master TaskCommitAck was lost. Ordered by
// next_upload_attempt_at so the most-overdue retry is first. The caller
// filters the exact due-instant in Go (next_upload_attempt_at is
// RFC3339Nano; zero means due immediately).
func (s *Store) ListUploadResumeCandidates(ctx context.Context, limit int) ([]SpoolEntry, error) {
	if limit <= 0 {
		limit = 32
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+selectSpoolCols+` FROM worker_output_spool
		  WHERE status IN ('UPLOAD_PENDING','UPLOADING','UPLOADED')
		    AND upload_target_json != ''
		  ORDER BY next_upload_attempt_at ASC, created_at ASC
		  LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("spool.ListUploadResumeCandidates: %w", err)
	}
	defer rows.Close()
	var out []SpoolEntry
	for rows.Next() {
		e, err := scanSpool(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *e)
	}
	return out, rows.Err()
}

// ListVolatileUncommitted returns tmpfs-backed rows that have not reached a
// terminal state. The graceful-shutdown spill iterates this set so a SIGTERM
// moves every still-volatile artifact onto durable NVMe before /dev/shm
// disappears at reboot. COMMITTED / REJECTED / CLEANED rows are excluded
// (they are terminal: the post-commit cleanup owns their file lifecycle).
func (s *Store) ListVolatileUncommitted(ctx context.Context) ([]SpoolEntry, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+selectSpoolCols+` FROM worker_output_spool
		  WHERE storage_tier = 'TMPFS_VOLATILE'
		    AND status IN ('OUTPUT_READY','UPLOAD_PENDING','UPLOADING','UPLOADED')
		  ORDER BY created_at ASC`)
	if err != nil {
		return nil, fmt.Errorf("spool.ListVolatileUncommitted: %w", err)
	}
	defer rows.Close()
	var out []SpoolEntry
	for rows.Next() {
		e, err := scanSpool(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *e)
	}
	return out, rows.Err()
}

const selectSpoolCols = `spool_id, task_id, attempt_id, commit_id,
    worker_spool_key, local_path, sha256, size_bytes, upload_id,
    uploaded_bytes, status, storage_tier, last_error,
    upload_target_json, commit_token, upload_attempt_count,
    next_upload_attempt_at, created_at, updated_at`

const selectSpoolBySpoolID = `SELECT ` + selectSpoolCols +
	` FROM worker_output_spool WHERE spool_id = ?`
const selectSpoolByStatus = `SELECT ` + selectSpoolCols +
	` FROM worker_output_spool WHERE status = ? ORDER BY created_at ASC`

// scanDBI abstracts *sql.Row + *sql.Rows so both Get and the iterator
// callers share one scanner.
type scanDBI interface {
	Scan(...interface{}) error
}

func scanSpool(r scanDBI) (*SpoolEntry, error) {
	var (
		e            SpoolEntry
		sizeB        sql.NullInt64
		uploadB      sql.NullInt64
		statusS      string
		tierS        string
		attemptCount int
		nextAttemptS string
		created      string
		updated      string
	)
	err := r.Scan(
		&e.SpoolID, &e.TaskID, &e.AttemptID, &e.CommitID, &e.WorkerSpoolKey,
		&e.LocalPath, &e.SHA256, &sizeB, &e.UploadID, &uploadB,
		&statusS, &tierS, &e.LastError,
		&e.UploadTargetJSON, &e.CommitToken, &attemptCount, &nextAttemptS,
		&created, &updated,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("spool.scanSpool: %w", err)
	}
	e.SizeBytes = sizeB.Int64
	e.UploadedBytes = uploadB.Int64
	e.Status = Status(statusS)
	e.StorageTier = StorageTier(tierS)
	if e.StorageTier == "" {
		e.StorageTier = StorageTierNvmeDurable
	}
	e.UploadAttemptCount = attemptCount
	if nextAttemptS != "" {
		if e.NextUploadAttemptAt, err = parseRFC3339Nano(nextAttemptS); err != nil {
			return nil, fmt.Errorf("spool.scanSpool: next_upload_attempt_at: %w", err)
		}
	}
	if e.CreatedAt, err = parseRFC3339Nano(created); err != nil {
		return nil, fmt.Errorf("spool.scanSpool: created_at: %w", err)
	}
	if e.UpdatedAt, err = parseRFC3339Nano(updated); err != nil {
		return nil, fmt.Errorf("spool.scanSpool: updated_at: %w", err)
	}
	return &e, nil
}

// ────────────────────────────────────────────────────────────────────────
// helpers used by the read + create path.
// ────────────────────────────────────────────────────────────────────────

// newSpoolID returns a 16-byte hex sequence. Same construction idiom
// as DataServer/internal/completion/coordinator.go::newUUIDLowerHex;
// collision property is fine for a local single-instance database.
func newSpoolID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		for i := range b {
			b[i] = byte(i + 1)
		}
	}
	return hex.EncodeToString(b[:])
}

// parseRFC3339Nano accepts RFC3339Nano (with nanos) and plain RFC3339
// (second precision) — both forms can land from older code paths.
func parseRFC3339Nano(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339, s)
}

// formatUploadAttemptAt renders the next-upload-attempt instant for the
// spool column. A zero time renders as "" (due immediately).
func formatUploadAttemptAt(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// isUniqueConflict returns true when err is a SQLite UNIQUE constraint
// violation. The mattn/go-sqlite3 driver reports this with the
// sub-string "UNIQUE constraint failed".
func isUniqueConflict(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, frag := range []string{"UNIQUE constraint failed", "constraint failed"} {
		if containsCI(msg, frag) {
			return true
		}
	}
	return false
}

func containsCI(haystack, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	if len(haystack) < len(needle) {
		return false
	}
	// case-insensitive substring match
	h := []byte(haystack)
	n := []byte(needle)
	for i := 0; i+len(n) <= len(h); i++ {
		match := true
		for j := 0; j < len(n); j++ {
			hh := h[i+j]
			nn := n[j]
			if hh >= 'A' && hh <= 'Z' {
				hh += 32
			}
			if nn >= 'A' && nn <= 'Z' {
				nn += 32
			}
			if hh != nn {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

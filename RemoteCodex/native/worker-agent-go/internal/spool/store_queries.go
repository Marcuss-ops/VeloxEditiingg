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

// Insert registers a new spool entry in StatusRendering. The unique
// tuple (task_id, attempt_id, worker_spool_key) prevents the same
// worker from double-spooling the same logical output.
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
		    status, last_error, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		e.SpoolID, e.TaskID, e.AttemptID, e.CommitID, e.WorkerSpoolKey,
		e.LocalPath, e.SHA256, e.SizeBytes, e.UploadID, e.UploadedBytes,
		string(e.Status), e.LastError, nowStr, nowStr,
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

const selectSpoolCols = `spool_id, task_id, attempt_id, commit_id,
    worker_spool_key, local_path, sha256, size_bytes, upload_id,
    uploaded_bytes, status, last_error, created_at, updated_at`

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
		e       SpoolEntry
		sizeB   sql.NullInt64
		uploadB sql.NullInt64
		statusS string
		created string
		updated string
	)
	err := r.Scan(
		&e.SpoolID, &e.TaskID, &e.AttemptID, &e.CommitID, &e.WorkerSpoolKey,
		&e.LocalPath, &e.SHA256, &sizeB, &e.UploadID, &uploadB,
		&statusS, &e.LastError, &created, &updated,
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

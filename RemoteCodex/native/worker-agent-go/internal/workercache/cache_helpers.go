package workercache

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// cache_helpers.go owns the storage plumbing of the workercache
// package: the schema DDL, the row projection, the scanner and the
// shared predicates. The public CRUD surface lives in cache.go.

// ────────────────────────────────────────────────────────────────────────
// schema + scanner helpers.
// ────────────────────────────────────────────────────────────────────────

// schemaDDL carries an explicit note on the active_job_id
// representation: a row is "leased" iff active_job_id is neither
// NULL nor the empty string. Two equivalent forms coexist:
//
//   - Just-inserted rows → Store writes ” (empty string).
//   - Post-Release rows  → Release writes NULL explicitly.
//
// The partial index below indexes only non-empty values so the
// cleaner can scan "currently leased" cheaply. Queries for the
// non-leased set must use
//
//	WHERE active_job_id IS NULL OR active_job_id = ''
//
// (or COALESCE(active_job_id, ”) = ”). Pick one representation
// in a future pass if the dual form becomes a footgun.
const schemaDDL = `
CREATE TABLE IF NOT EXISTS cached_assets (
    drive_file_id      TEXT PRIMARY KEY,
    local_path         TEXT NOT NULL,
    size_bytes         INTEGER NOT NULL DEFAULT 0,
    active_job_id      TEXT,
    download_complete  INTEGER NOT NULL DEFAULT 0,
    created_at         TEXT NOT NULL,
    last_used_at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cached_assets_active_job_id
    ON cached_assets(active_job_id)
    WHERE active_job_id IS NOT NULL AND active_job_id != '';
CREATE INDEX IF NOT EXISTS idx_cached_assets_last_used_at
    ON cached_assets(last_used_at);
`

const selectCols = `drive_file_id, local_path, size_bytes, active_job_id,
    download_complete, created_at, last_used_at`

// scanDBI lets scanEntry work for both *sql.Row and *sql.Rows.
type scanDBI interface {
	Scan(...interface{}) error
}

func scanEntry(r scanDBI) (*Entry, error) {
	var (
		e         Entry
		dlInt     int
		activeJob sql.NullString
		createdS  string
		usedS     string
	)
	err := r.Scan(
		&e.DriveFileID, &e.LocalPath, &e.SizeBytes, &activeJob,
		&dlInt, &createdS, &usedS,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("workercache.scanEntry: %w", err)
	}
	e.ActiveJobID = activeJob.String
	e.DownloadComplete = dlInt != 0
	if e.CreatedAt, err = parseRFC3339Nano(createdS); err != nil {
		return nil, fmt.Errorf("workercache.scanEntry: created_at: %w", err)
	}
	if e.LastUsedAt, err = parseRFC3339Nano(usedS); err != nil {
		return nil, fmt.Errorf("workercache.scanEntry: last_used_at: %w", err)
	}
	return &e, nil
}

// mustHaveAffected returns ErrNotFound if the result affected zero
// rows, otherwise nil.
func mustHaveAffected(res sql.Result, driveFileID, op string) error {
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("workercache.%s(%q): rows affected: %w", op, driveFileID, err)
	}
	if n == 0 {
		return fmt.Errorf("%w: drive_file_id=%s", ErrNotFound, driveFileID)
	}
	return nil
}

// parseRFC3339Nano accepts RFC3339Nano (preferred) and plain RFC3339
// (second precision) — both forms can land from older code paths or
// from external producers. Duplicated from internal/spool/ for this
// package's self-containment; consolidate to shared/sqliteutil when a
// third caller appears.
func parseRFC3339Nano(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339, s)
}

// isUniqueConflict returns true when err is a SQLite UNIQUE constraint
// violation. The mattn/go-sqlite3 driver reports this with the
// substring "UNIQUE constraint failed".
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

// containsCI is a case-insensitive substring match. Same idiom as
// internal/spool/store_queries.go::containsCI; placeholder for the
// future shared/sqliteutil consolidation.
func containsCI(haystack, needle string) bool {
	if needle == "" {
		return true
	}
	if len(haystack) < len(needle) {
		return false
	}
	h := []byte(haystack)
	n := []byte(needle)
	for i := 0; i+len(n) <= len(h); i++ {
		match := true
		for j := 0; j < len(n); j++ {
			hh := h[i+j]
			if hh >= 'A' && hh <= 'Z' {
				hh += 32
			}
			nn := n[j]
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

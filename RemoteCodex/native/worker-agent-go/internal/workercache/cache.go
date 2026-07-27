// Package workercache is the worker-side durable index over the
// clip-asset cache.
//
// The worker downloads Drive clips into a content-addressable
// on-disk directory. To avoid re-downloading the same file across
// jobs and across restarts, the in-flight and historical state of
// that local cache is tracked here. The cache key is the canonical
// Google Drive file ID (see DataServer/internal/assetref, although
// this package does not depend on it: callers are expected to have
// already normalised URLs before persisting).
//
// The package is a deliberate split from:
//
//   - internal/worker/asset_cache.go — short-lived, in-memory key/path
//     lookup for the download path; this package is the durable
//     ground truth that survives restart.
//   - internal/spool — tracks *output* artifacts produced by the
//     worker (separate lifecycle: RENDERING → COMMITTED → CLEANED);
//     this package tracks *input* Drive clips (download + reuse).
//
// SQLite is the storage substrate (matches DataServer/spool
// convention; mattn/go-sqlite3 driver with WAL + busy_timeout).
// Reads and writes serialise through *sql.DB; concurrent calls are
// safe.
//
// Schema invariants:
//
//  1. drive_file_id is the primary key. One row per cache entry;
//     duplicate inserts return ErrDuplicate so the caller can treat
//     a concurrent Resolve as a benign race.
//  2. download_complete transitions false → true atomically via
//     MarkDownloadComplete; the cleaner never deletes a row whose
//     download_complete is false (a half-written file is recoverable
//     by re-running Resolve).
//  3. active_job_id is the per-asset lease. While non-empty the
//     cleaner MUST skip the row. Acquire sets it; Release clears it
//     only if it is still owned by the same job (defensive: a stale
//     Release from JOB A does not wipe JOB B's lease).
//  4. last_used_at is bumped by every MarkUsed / MarkDownloadComplete /
//     Release; the cleaner uses it for the 3-minute grace period
//     (see Pass 11 plan; enforced in the cleaner, not here).
package workercache

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// Entry is the row shape for cached_assets as exposed to callers.
// ActiveJobID is empty when no job holds the lease on the asset.
type Entry struct {
	DriveFileID      string
	LocalPath        string
	SizeBytes        int64
	ActiveJobID      string
	DownloadComplete bool
	CreatedAt        time.Time
	LastUsedAt       time.Time
}

// Sentinel errors so callers can branch via errors.Is, not string match.
var (
	ErrNotFound  = errors.New("workercache: cached asset not found")
	ErrDuplicate = errors.New("workercache: cached asset already exists")
	ErrEmptyID   = errors.New("workercache: drive_file_id is required")
)

// Cache is the SQLite-backed index over cached worker assets.
type Cache struct {
	db *sql.DB
}

// Open creates or opens the cache database at path. WAL + busy timeout
// are tuned at open so concurrent goroutines (resolver, cleaner,
// supervisor scan) do not trip on `database is locked`. Passing
// ":memory:" returns an in-memory database suitable for tests.
// Open creates or opens the cache database at path. WAL + busy
// timeout are tuned at open so concurrent goroutines (resolver,
// cleaner, supervisor scan) do not trip on `database is locked`.
// We deliberately leave *sql.DB at its default pool
// (MaxOpenConns=0): with WAL each connection can hold the read
// lock, and busy_timeout queues contending writers. A bounded
// MaxOpenConns would suppress reader parallelism in exchange for
// stricter writer ordering; the busy_timeout trade is preferred
// here to match the internal/spool package convention. Passing
// ":memory:" returns an in-memory database suitable for tests.
func Open(path string) (*Cache, error) {
	dsn := path
	if path != ":memory:" {
		// DSN init params match DataServer/spool convention so
		// operators do not need to learn two flavours.
		dsn = path + "?_busy_timeout=5000&_journal_mode=WAL&_synchronous=NORMAL"
	}
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("workercache.Open: sql.Open: %w", err)
	}
	if _, err := db.Exec(schemaDDL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("workercache.Open: apply schema: %w", err)
	}
	return &Cache{db: db}, nil
}

// Close releases the underlying *sql.DB. The cache cannot be reused
// after Close.
func (c *Cache) Close() error {
	if c == nil || c.db == nil {
		return nil
	}
	return c.db.Close()
}

// DB returns the underlying *sql.DB. Reserved for advanced uses
// (migrations, supervisor scans that need to join across tables).
func (c *Cache) DB() *sql.DB { return c.db }

// Find returns the entry for driveFileID. The boolean reports
// presence: (zero Entry, false, nil) means the row is absent, not
// that an error occurred. An empty driveFileID returns ErrEmptyID.
func (c *Cache) Find(ctx context.Context, driveFileID string) (Entry, bool, error) {
	if driveFileID == "" {
		return Entry{}, false, ErrEmptyID
	}
	row := c.db.QueryRowContext(ctx,
		`SELECT `+selectCols+` FROM cached_assets WHERE drive_file_id = ?`,
		driveFileID)
	e, err := scanEntry(row)
	if errors.Is(err, ErrNotFound) {
		return Entry{}, false, nil
	}
	if err != nil {
		return Entry{}, false, fmt.Errorf("workercache.Find(%q): %w", driveFileID, err)
	}
	return *e, true, nil
}

// Store inserts a new entry. Returns ErrDuplicate if drive_file_id is
// already present; callers should treat this as a benign race (a
// concurrent Resolve already wrote the entry) and reload it via
// Find. CreatedAt and LastUsedAt default to time.Now().UTC() if
// zero. LocalPath is required.
func (c *Cache) Store(ctx context.Context, e Entry) error {
	if e.DriveFileID == "" {
		return ErrEmptyID
	}
	if e.LocalPath == "" {
		return fmt.Errorf("workercache.Store: local_path is required")
	}
	if e.CreatedAt.IsZero() {
		e.CreatedAt = time.Now().UTC()
	}
	if e.LastUsedAt.IsZero() {
		e.LastUsedAt = e.CreatedAt
	}
	if e.ActiveJobID != "" {
		return fmt.Errorf("workercache.Store: ActiveJobID must be empty for fresh entries (got %q); use Acquire to lease after Store", e.ActiveJobID)
	}
	dlInt := 0
	if e.DownloadComplete {
		dlInt = 1
	}
	_, err := c.db.ExecContext(ctx,
		`INSERT INTO cached_assets
		   (drive_file_id, local_path, size_bytes, active_job_id,
		    download_complete, created_at, last_used_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		e.DriveFileID, e.LocalPath, e.SizeBytes, e.ActiveJobID,
		dlInt, e.CreatedAt.Format(time.RFC3339Nano),
		e.LastUsedAt.Format(time.RFC3339Nano),
	)
	if err != nil {
		if isUniqueConflict(err) {
			return fmt.Errorf("%w: drive_file_id=%s", ErrDuplicate, e.DriveFileID)
		}
		return fmt.Errorf("workercache.Store(%q): %w", e.DriveFileID, err)
	}
	return nil
}

// MarkUsed bumps last_used_at to time.Now().UTC(). It does NOT make
// a not-yet-complete entry usable: callers should still check
// DownloadComplete before opening the local file. Returns
// ErrNotFound when no row matches.
func (c *Cache) MarkUsed(ctx context.Context, driveFileID string) error {
	if driveFileID == "" {
		return ErrEmptyID
	}
	res, err := c.db.ExecContext(ctx,
		`UPDATE cached_assets SET last_used_at = ?
		 WHERE drive_file_id = ?`,
		time.Now().UTC().Format(time.RFC3339Nano), driveFileID,
	)
	if err != nil {
		return fmt.Errorf("workercache.MarkUsed(%q): %w", driveFileID, err)
	}
	return mustHaveAffected(res, driveFileID, "MarkUsed")
}

// MarkDownloadComplete transitions a row to the complete state:
// sets local_path + size_bytes, flips download_complete → true, and
// bumps last_used_at so the asset is treated as recently-used.
//
// Resolver contract (Pass 10 will hardwire this):
//  1. Resolver inserts a placeholder row via Store with
//     local_path = "<dir>/<driveID>.mp4.part" and download_complete=false.
//     The cleaner never deletes such rows (download is in flight).
//  2. Resolve streams the bytes to the .part path, verifies media.
//  3. On success, resolver atomically renames .part → final filename
//     and then calls MarkDownloadComplete with the final
//     local_path + size_bytes.
//
// The cleaner MUST predicate on download_complete=1 before
// deleting, so a half-completed download survives a crash and is
// recoverable on the next Resolve. The local_path field is
// overwritten on each call, so a resumed download naturally
// updates the cached path.
//
// Returns ErrNotFound when no row matches.
func (c *Cache) MarkDownloadComplete(ctx context.Context, driveFileID, localPath string, sizeBytes int64) error {
	if driveFileID == "" {
		return ErrEmptyID
	}
	if localPath == "" {
		return fmt.Errorf("workercache.MarkDownloadComplete: local_path is required")
	}
	res, err := c.db.ExecContext(ctx,
		`UPDATE cached_assets
		   SET local_path = ?, size_bytes = ?, download_complete = 1, last_used_at = ?
		 WHERE drive_file_id = ?`,
		localPath, sizeBytes,
		time.Now().UTC().Format(time.RFC3339Nano), driveFileID,
	)
	if err != nil {
		return fmt.Errorf("workercache.MarkDownloadComplete(%q): %w", driveFileID, err)
	}
	return mustHaveAffected(res, driveFileID, "MarkDownloadComplete")
}

// Acquire marks the asset as in-use for jobID (sets active_job_id).
// The cleaner MUST skip rows with active_job_id != ” so the asset
// is preserved while the job runs. Acquire does not block: if a
// different job already holds the lease, this overwrites it. The
// Pass 11 singleflight wrapper prevents the race at the download
// layer; here we keep the update unconditional to match the design
// pseudocode. Returns ErrNotFound when no row matches.
func (c *Cache) Acquire(ctx context.Context, driveFileID, jobID string) error {
	if driveFileID == "" {
		return ErrEmptyID
	}
	if jobID == "" {
		return fmt.Errorf("workercache.Acquire: jobID is required")
	}
	res, err := c.db.ExecContext(ctx,
		`UPDATE cached_assets SET active_job_id = ?
		 WHERE drive_file_id = ?`,
		jobID, driveFileID,
	)
	if err != nil {
		return fmt.Errorf("workercache.Acquire(%q, %q): %w", driveFileID, jobID, err)
	}
	return mustHaveAffected(res, driveFileID, "Acquire")
}

// Release clears active_job_id IFF it equals jobID, and bumps
// last_used_at. A Release from a job that does not own the lease is
// a benign no-op: this protects against the race scenario where
// JOB A acquires, JOB B acquires (overwrites), JOB A releases — we
// must not wipe JOB B's lease. Returns ErrNotFound when the row
// itself is missing.
func (c *Cache) Release(ctx context.Context, driveFileID, jobID string) error {
	if driveFileID == "" {
		return ErrEmptyID
	}
	if jobID == "" {
		return fmt.Errorf("workercache.Release: jobID is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := c.db.ExecContext(ctx,
		`UPDATE cached_assets
		   SET active_job_id = NULL, last_used_at = ?
		 WHERE drive_file_id = ? AND active_job_id = ?`,
		now, driveFileID, jobID,
	)
	if err != nil {
		return fmt.Errorf("workercache.Release(%q, %q): %w", driveFileID, jobID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("workercache.Release(%q, %q): rows affected: %w", driveFileID, jobID, err)
	}
	if n == 0 {
		// Either the row is missing, or the lease was owned by a
		// different job (the latter is a benign no-op).
		var found int
		if err := c.db.QueryRowContext(ctx,
			`SELECT COUNT(1) FROM cached_assets WHERE drive_file_id = ?`,
			driveFileID).Scan(&found); err != nil {
			return fmt.Errorf("workercache.Release(%q, %q): probe: %w", driveFileID, jobID, err)
		}
		if found == 0 {
			return fmt.Errorf("%w: drive_file_id=%s", ErrNotFound, driveFileID)
		}
	}
	return nil
}

// Delete removes the row. Returns ErrNotFound when no row matches.
// The caller is responsible for removing the on-disk file (the
// cleaner pattern is: List → for each evictable row: Delete + os.Remove).
func (c *Cache) Delete(ctx context.Context, driveFileID string) error {
	if driveFileID == "" {
		return ErrEmptyID
	}
	res, err := c.db.ExecContext(ctx,
		`DELETE FROM cached_assets WHERE drive_file_id = ?`,
		driveFileID,
	)
	if err != nil {
		return fmt.Errorf("workercache.Delete(%q): %w", driveFileID, err)
	}
	return mustHaveAffected(res, driveFileID, "Delete")
}

// List returns all rows ordered by drive_file_id (deterministic for
// tests + supervisor scans).
func (c *Cache) List(ctx context.Context) ([]Entry, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT `+selectCols+` FROM cached_assets ORDER BY drive_file_id ASC`)
	if err != nil {
		return nil, fmt.Errorf("workercache.List: %w", err)
	}
	defer rows.Close()
	var out []Entry
	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *e)
	}
	return out, rows.Err()
}

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

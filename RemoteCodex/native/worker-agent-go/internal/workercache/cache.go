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
//     by re-running Resolve).	//  3. cached_asset_leases is the authoritative many-to-many lease
//     relation. While it contains a row for an asset the cleaner MUST
//     skip that asset. Acquire inserts a relation; Release removes only
//     the caller's relation.
//  4. last_used_at is bumped by every MarkUsed / MarkDownloadComplete /

//	Release; the cleaner uses it for the 3-minute grace period
//	(see Pass 11 plan; enforced in the cleaner, not here).
//
// The schema DDL, the row scanner and the helper predicates live in
// the sibling file cache_helpers.go.
package workercache

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Entry is the row shape for cached_assets as exposed to callers.
// ActiveJobID is a deterministic representative of the active leases, or
// empty when no job holds the lease. ActiveLeaseCount is authoritative.
// It is derived from cached_asset_leases and is not persisted on cached_assets;
// the name remains for compatibility with existing audit/eviction payloads.
type Entry struct {
	DriveFileID      string
	LocalPath        string
	SizeBytes        int64
	ActiveJobID      string
	DownloadComplete bool
	CreatedAt        time.Time
	LastUsedAt       time.Time
	ActiveLeaseCount int
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
	if err := applySchema(db); err != nil {
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
		   (drive_file_id, local_path, size_bytes,
		    download_complete, created_at, last_used_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		e.DriveFileID, e.LocalPath, e.SizeBytes,
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

// Acquire adds the (asset, job) relation to the authoritative lease table.
// Multiple jobs may hold the same asset lease concurrently. Returns
// ErrNotFound when no cached asset row matches.
func (c *Cache) Acquire(ctx context.Context, driveFileID, jobID string) error {
	if driveFileID == "" {
		return ErrEmptyID
	}
	if jobID == "" {
		return fmt.Errorf("workercache.Acquire: jobID is required")
	}
	res, err := c.db.ExecContext(ctx, `INSERT OR IGNORE INTO cached_asset_leases (drive_file_id, job_id, acquired_at) SELECT drive_file_id, ?, ? FROM cached_assets WHERE drive_file_id = ?`, jobID, time.Now().UTC().Format(time.RFC3339Nano), driveFileID)
	if err != nil {
		return fmt.Errorf("workercache.Acquire(%q, %q): lease insert: %w", driveFileID, jobID, err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("workercache.Acquire(%q, %q): rows affected: %w", driveFileID, jobID, err)
	} else if n == 0 {
		var found int
		if err := c.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM cached_assets WHERE drive_file_id = ?`, driveFileID).Scan(&found); err != nil {
			return fmt.Errorf("workercache.Acquire(%q, %q): probe: %w", driveFileID, jobID, err)
		}
		if found == 0 {
			return fmt.Errorf("%w: drive_file_id=%s", ErrNotFound, driveFileID)
		}
	}
	return nil
}

// Release removes only the (asset, job) lease relation and bumps
// last_used_at. Releasing another job's lease is a benign no-op.
// Returns ErrNotFound when the asset row itself is missing.
func (c *Cache) Release(ctx context.Context, driveFileID, jobID string) error {
	if driveFileID == "" {
		return ErrEmptyID
	}
	if jobID == "" {
		return fmt.Errorf("workercache.Release: jobID is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := c.db.ExecContext(ctx, `DELETE FROM cached_asset_leases WHERE drive_file_id = ? AND job_id = ?`, driveFileID, jobID); err != nil {
		return fmt.Errorf("workercache.Release(%q, %q): lease delete: %w", driveFileID, jobID, err)
	}
	res, err := c.db.ExecContext(ctx, `UPDATE cached_assets SET last_used_at = ? WHERE drive_file_id = ?`, now, driveFileID)
	if err != nil {
		return fmt.Errorf("workercache.Release(%q, %q): %w", driveFileID, jobID, err)
	}
	return mustHaveAffected(res, driveFileID, "Release")
}

// DeleteIfUnleased atomically removes an unleased cache row. The lease
// predicate closes the List→Delete race when another job acquires the same
// asset while cleanup is scanning.
func (c *Cache) DeleteIfUnleased(ctx context.Context, driveFileID string) error {
	if driveFileID == "" {
		return ErrEmptyID
	}
	res, err := c.db.ExecContext(ctx, `DELETE FROM cached_assets WHERE drive_file_id = ? AND NOT EXISTS (SELECT 1 FROM cached_asset_leases WHERE drive_file_id = ?)`, driveFileID, driveFileID)
	if err != nil {
		return fmt.Errorf("workercache.DeleteIfUnleased(%q): %w", driveFileID, err)
	}
	return mustHaveAffected(res, driveFileID, "DeleteIfUnleased")
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

// Size returns the current number of indexed entries and their recorded byte
// total. It is a read-only snapshot for low-cardinality cache gauges.
func (c *Cache) Size(ctx context.Context) (entries int, bytes int64, err error) {
	if c == nil || c.db == nil {
		return 0, 0, fmt.Errorf("workercache.Size: nil cache")
	}
	if err := c.db.QueryRowContext(ctx, `SELECT COUNT(1), COALESCE(SUM(size_bytes), 0) FROM cached_assets`).Scan(&entries, &bytes); err != nil {
		return 0, 0, fmt.Errorf("workercache.Size: %w", err)
	}
	return entries, bytes, nil
}

// ReadyKeys returns the canonical asset keys currently materialized on disk.
// It is a read-only, bounded heartbeat projection used by master placement;
// callers must not treat it as a lease or as proof that a file cannot be
// evicted before the task acquires its lease.
func (c *Cache) ReadyKeys(ctx context.Context) ([]string, error) {
	rows, err := c.db.QueryContext(ctx, `SELECT drive_file_id FROM cached_assets WHERE download_complete = 1 ORDER BY drive_file_id`)
	if err != nil {
		return nil, fmt.Errorf("workercache.ReadyKeys: %w", err)
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("workercache.ReadyKeys scan: %w", err)
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}

// Package workercache is the worker-side durable index over the
// clip-asset cache.
//
// The canonical worker download manager writes verified assets into a
// content-addressable on-disk directory. To avoid re-downloading the same
// asset across jobs and restarts, this package tracks the durable index and
// lease state. The current persisted key remains the historical
// AssetKey field; migration to namespaced asset_key is tracked separately.
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
//  1. asset_key is the primary key. One row per cache entry;
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

	"velox-shared/assetref"
)

// Entry is the row shape for cached_assets as exposed to callers.
// ActiveJobID is a deterministic representative of the active leases, or
// empty when no job holds the lease. ActiveLeaseCount is authoritative.
// It is derived from cached_asset_leases and is not persisted on cached_assets;
// the name remains for compatibility with existing audit/eviction payloads.
// AssetKey aliases the shared canonical asset identity for package-local
// callers that already depend on workercache.
type AssetKey = assetref.AssetKey

// ContentHash aliases the shared verified byte identity.
type ContentHash = assetref.ContentHash

type Entry struct {
	AssetKey               assetref.AssetKey
	ContentHash            assetref.ContentHash
	LocalPath              string
	SizeBytes              int64
	ActiveJobID            string
	DownloadComplete       bool
	CreatedAt              time.Time
	LastUsedAt             time.Time
	ActiveLeaseCount       int
	ActiveReservationCount int
}

// Blob is the physical content-addressed row of cached_blobs: the verified
// bytes at LocalPath, keyed by ContentHash. Multiple assets can reference one
// blob when their bytes are identical (dedup).
type Blob struct {
	ContentHash      assetref.ContentHash
	LocalPath        string
	SizeBytes        int64
	DownloadComplete bool
	CreatedAt        time.Time
	LastUsedAt       time.Time
	VerifiedAt       time.Time
}

// Sentinel errors so callers can branch via errors.Is, not string match.
var (
	ErrNotFound           = errors.New("workercache: cached asset not found")
	ErrDuplicate          = errors.New("workercache: cached asset already exists")
	ErrEmptyID            = errors.New("workercache: asset_key is required")
	ErrInvalidContentHash = errors.New("workercache: content_hash must be a SHA-256 digest")
	ErrLeaseNotFound      = errors.New("workercache: lease not found")
)

// Cache is the SQLite-backed index over cached worker assets.
type Cache struct {
	db *sql.DB
	fs cacheFileSystem
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
	return &Cache{db: db, fs: osCacheFileSystem{}}, nil
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

// Find returns the entry for assetKey. The boolean reports
// presence: (zero Entry, false, nil) means the row is absent, not
// that an error occurred. An empty assetKey returns ErrEmptyID.
func (c *Cache) Find(ctx context.Context, assetKey string) (Entry, bool, error) {
	if assetKey == "" {
		return Entry{}, false, ErrEmptyID
	}
	row := c.db.QueryRowContext(ctx,
		`SELECT `+selectCols+selectFrom+` WHERE a.asset_key = ?`,
		assetKey)
	e, err := scanEntry(row)
	if errors.Is(err, ErrNotFound) {
		return Entry{}, false, nil
	}
	if err != nil {
		return Entry{}, false, fmt.Errorf("workercache.Find(%q): %w", assetKey, err)
	}
	return *e, true, nil
}

// FindBlob returns the physical blob for a verified content hash. It is the
// content-addressed lookup a resolver uses when the asset_key mapping is
// unknown: a known SHA probes cached_blobs directly, so an asset whose bytes
// are already cached under another asset ID is still found. The boolean is
// false (with a zero Blob) when no blob exists, mirroring Find.
func (c *Cache) FindBlob(ctx context.Context, contentHash assetref.ContentHash) (Blob, bool, error) {
	if contentHash == "" {
		return Blob{}, false, nil
	}
	var (
		b         Blob
		dlInt     int
		createdS  string
		usedS     string
		verifiedS string
	)
	err := c.db.QueryRowContext(ctx,
		`SELECT content_hash, local_path, size_bytes, download_complete, created_at, last_used_at, verified_at
		   FROM cached_blobs WHERE content_hash = ?`,
		string(contentHash),
	).Scan(&b.ContentHash, &b.LocalPath, &b.SizeBytes, &dlInt, &createdS, &usedS, &verifiedS)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Blob{}, false, nil
		}
		return Blob{}, false, fmt.Errorf("workercache.FindBlob(%q): %w", contentHash, err)
	}
	b.DownloadComplete = dlInt != 0
	if b.CreatedAt, err = parseRFC3339Nano(createdS); err != nil {
		return Blob{}, false, fmt.Errorf("workercache.FindBlob(%q): created_at: %w", contentHash, err)
	}
	if b.LastUsedAt, err = parseRFC3339Nano(usedS); err != nil {
		return Blob{}, false, fmt.Errorf("workercache.FindBlob(%q): last_used_at: %w", contentHash, err)
	}
	if b.VerifiedAt, err = parseRFC3339Nano(verifiedS); err != nil {
		return Blob{}, false, fmt.Errorf("workercache.FindBlob(%q): verified_at: %w", contentHash, err)
	}
	return b, true, nil
}

// Store inserts a new entry. Returns ErrDuplicate if asset_key is
// already present; callers should treat this as a benign race (a
// concurrent Resolve already wrote the entry) and reload it via
// Find. CreatedAt and LastUsedAt default to time.Now().UTC() if
// zero. LocalPath is required.
func (c *Cache) Store(ctx context.Context, e Entry) error {
	if e.AssetKey == "" {
		return ErrEmptyID
	}
	if e.LocalPath == "" {
		return fmt.Errorf("workercache.Store: local_path is required")
	}
	if e.ContentHash != "" {
		canonical, err := assetref.ParseContentHash(string(e.ContentHash))
		if err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidContentHash, err)
		}
		e.ContentHash = canonical
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
	blobKey := string(e.ContentHash)
	if blobKey == "" {
		blobKey = legacyBlobKey(string(e.AssetKey))
	}
	verifiedAt := ""
	if e.DownloadComplete {
		verifiedAt = e.LastUsedAt.Format(time.RFC3339Nano)
	}

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("workercache.Store(%q): begin: %w", e.AssetKey, err)
	}
	rollback := func(cause error) error {
		_ = tx.Rollback()
		return cause
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO cached_assets
		   (asset_key, content_hash, created_at, last_used_at)
		 VALUES (?, ?, ?, ?)`,
		string(e.AssetKey), blobKey,
		e.CreatedAt.Format(time.RFC3339Nano),
		e.LastUsedAt.Format(time.RFC3339Nano),
	); err != nil {
		if isUniqueConflict(err) {
			return rollback(fmt.Errorf("%w: asset_key=%s", ErrDuplicate, e.AssetKey))
		}
		return rollback(fmt.Errorf("workercache.Store(%q): %w", e.AssetKey, err))
	}
	// First writer wins the blob path: assets sharing a content_hash must
	// reference one physical file, so a re-insert of the same blob keeps the
	// existing local_path.
	if _, err := tx.ExecContext(ctx,
		`INSERT OR IGNORE INTO cached_blobs
		   (content_hash, local_path, size_bytes, created_at, last_used_at, verified_at, download_complete)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		blobKey, e.LocalPath, e.SizeBytes,
		e.CreatedAt.Format(time.RFC3339Nano),
		e.LastUsedAt.Format(time.RFC3339Nano),
		verifiedAt, dlInt,
	); err != nil {
		return rollback(fmt.Errorf("workercache.Store(%q): blob: %w", e.AssetKey, err))
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("workercache.Store(%q): commit: %w", e.AssetKey, err)
	}
	return nil
}

// MarkUsed bumps last_used_at to time.Now().UTC(). It does NOT make
// a not-yet-complete entry usable: callers should still check
// DownloadComplete before opening the local file. Returns
// ErrNotFound when no row matches.
func (c *Cache) MarkUsed(ctx context.Context, assetKey string) error {
	if assetKey == "" {
		return ErrEmptyID
	}
	res, err := c.db.ExecContext(ctx,
		`UPDATE cached_assets SET last_used_at = ?
		 WHERE asset_key = ?`,
		time.Now().UTC().Format(time.RFC3339Nano), assetKey,
	)
	if err != nil {
		return fmt.Errorf("workercache.MarkUsed(%q): %w", assetKey, err)
	}
	return mustHaveAffected(res, assetKey, "MarkUsed")
}

// MarkBlobUsed bumps a blob's last_used_at, the LRU signal for blob-level
// eviction. It is the content-addressed counterpart of MarkUsed and is called
// on a blob-level cache hit so a shared blob used by several assets is not
// treated as cold while any of them is active.
func (c *Cache) MarkBlobUsed(ctx context.Context, contentHash assetref.ContentHash) error {
	if contentHash == "" {
		return ErrInvalidContentHash
	}
	res, err := c.db.ExecContext(ctx,
		`UPDATE cached_blobs SET last_used_at = ? WHERE content_hash = ?`,
		time.Now().UTC().Format(time.RFC3339Nano), string(contentHash),
	)
	if err != nil {
		return fmt.Errorf("workercache.MarkBlobUsed(%q): %w", contentHash, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("workercache.MarkBlobUsed(%q): rows affected: %w", contentHash, err)
	}
	if n == 0 {
		return fmt.Errorf("%w: content_hash=%s", ErrNotFound, contentHash)
	}
	return nil
}

// MarkDownloadComplete transitions a row to the complete state:
// sets local_path + size_bytes, flips download_complete → true, and
// bumps last_used_at so the asset is treated as recently-used.
//
// The canonical AssetDownloadManager transferer records a verified
// download with MarkDownloadComplete after atomic promotion. The cleaner
// MUST predicate on download_complete=1 before deleting, so an incomplete
// row survives a crash and can be reconciled without treating it as ready.
// The local_path field is overwritten on each successful promotion, so a
// resumed or repaired download naturally updates the cached path.
//
// Returns ErrNotFound when no row matches.
func (c *Cache) MarkDownloadComplete(ctx context.Context, assetKey, localPath string, sizeBytes int64) error {
	return c.markDownloadComplete(ctx, assetKey, localPath, sizeBytes, "")
}

// MarkDownloadCompleteWithHash records the verified content identity at the
// same transition that makes the file READY. A cache row never becomes
// complete without the manager having first verified and atomically promoted
// its bytes; an empty hash is retained only for legacy callers.
func (c *Cache) MarkDownloadCompleteWithHash(ctx context.Context, assetKey, localPath string, sizeBytes int64, hash assetref.ContentHash) error {
	if hash != "" {
		canonical, err := assetref.ParseContentHash(string(hash))
		if err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidContentHash, err)
		}
		hash = canonical
	}
	return c.markDownloadComplete(ctx, assetKey, localPath, sizeBytes, string(hash))
}

// PreserveContentHash returns the existing verified hash when a caller has no
// new digest, preventing legacy/cache-hit synchronization from erasing it.
func (c *Cache) PreserveContentHash(ctx context.Context, assetKey, localPath string, sizeBytes int64, hash assetref.ContentHash) error {
	if hash != "" {
		return c.MarkDownloadCompleteWithHash(ctx, assetKey, localPath, sizeBytes, hash)
	}
	entry, found, err := c.Find(ctx, assetKey)
	if err != nil {
		return err
	}
	if found && entry.ContentHash != "" {
		hash = entry.ContentHash
	}
	return c.MarkDownloadCompleteWithHash(ctx, assetKey, localPath, sizeBytes, hash)
}

func (c *Cache) markDownloadComplete(ctx context.Context, assetKey, localPath string, sizeBytes int64, hash string) error {
	if assetKey == "" {
		return ErrEmptyID
	}
	if localPath == "" {
		return fmt.Errorf("workercache.MarkDownloadComplete: local_path is required")
	}
	blobKey := hash
	if blobKey == "" {
		blobKey = legacyBlobKey(assetKey)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("workercache.MarkDownloadComplete(%q): begin: %w", assetKey, err)
	}
	rollback := func(cause error) error {
		_ = tx.Rollback()
		return cause
	}

	// Capture the previous mapping so a legacy→hash re-key can remove the now
	// orphaned legacy blob.
	var oldBlobKey string
	if err := tx.QueryRowContext(ctx,
		`SELECT content_hash FROM cached_assets WHERE asset_key = ?`, assetKey).Scan(&oldBlobKey); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return rollback(fmt.Errorf("%w: asset_key=%s", ErrNotFound, assetKey))
		}
		return rollback(fmt.Errorf("workercache.MarkDownloadComplete(%q): probe: %w", assetKey, err))
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO cached_blobs
		   (content_hash, local_path, size_bytes, created_at, last_used_at, verified_at, download_complete)
		 VALUES (?, ?, ?, ?, ?, ?, 1)
		 ON CONFLICT(content_hash) DO UPDATE SET
		   local_path = excluded.local_path,
		   size_bytes = excluded.size_bytes,
		   download_complete = 1,
		   last_used_at = excluded.last_used_at,
		   verified_at = excluded.verified_at`,
		blobKey, localPath, sizeBytes, now, now, now,
	); err != nil {
		return rollback(fmt.Errorf("workercache.MarkDownloadComplete(%q): blob: %w", assetKey, err))
	}
	res, err := tx.ExecContext(ctx,
		`UPDATE cached_assets SET content_hash = ?, last_used_at = ? WHERE asset_key = ?`,
		blobKey, now, assetKey,
	)
	if err != nil {
		return rollback(fmt.Errorf("workercache.MarkDownloadComplete(%q): %w", assetKey, err))
	}
	if err := mustHaveAffected(res, assetKey, "MarkDownloadComplete"); err != nil {
		return rollback(err)
	}
	if oldBlobKey != "" && oldBlobKey != blobKey {
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM cached_blobs WHERE content_hash = ? AND NOT EXISTS (SELECT 1 FROM cached_assets WHERE content_hash = ?)`,
			oldBlobKey, oldBlobKey); err != nil {
			return rollback(fmt.Errorf("workercache.MarkDownloadComplete(%q): cleanup orphan blob: %w", assetKey, err))
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("workercache.MarkDownloadComplete(%q): commit: %w", assetKey, err)
	}
	return nil
}

// Acquire adds the (asset, job) relation to the authoritative lease table.
// Multiple jobs may hold the same asset lease concurrently. Returns
// ErrNotFound when no cached asset row matches.
func (c *Cache) Acquire(ctx context.Context, assetKey, jobID string) error {
	if assetKey == "" {
		return ErrEmptyID
	}
	if jobID == "" {
		return fmt.Errorf("workercache.Acquire: jobID is required")
	}
	res, err := c.db.ExecContext(ctx, `INSERT OR IGNORE INTO cached_asset_leases (asset_key, job_id, acquired_at) SELECT asset_key, ?, ? FROM cached_assets WHERE asset_key = ?`, jobID, time.Now().UTC().Format(time.RFC3339Nano), assetKey)
	if err != nil {
		return fmt.Errorf("workercache.Acquire(%q, %q): lease insert: %w", assetKey, jobID, err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("workercache.Acquire(%q, %q): rows affected: %w", assetKey, jobID, err)
	} else if n == 0 {
		var found int
		if err := c.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM cached_assets WHERE asset_key = ?`, assetKey).Scan(&found); err != nil {
			return fmt.Errorf("workercache.Acquire(%q, %q): probe: %w", assetKey, jobID, err)
		}
		if found == 0 {
			return fmt.Errorf("%w: asset_key=%s", ErrNotFound, assetKey)
		}
	}
	return nil
}

// RenewLease is a cache-protection heartbeat, not a TTL extension: the
// authoritative cached_asset_leases relation has no expiry column and keeps
// the asset protected while present. It bumps last_used_at only when the
// (asset, job) lease relation still exists. Unlike MarkUsed, this fenced update cannot refresh an
// unleased asset, so a lost lease is visible to the caller and the render's
// renewal loop can report it instead of silently claiming success.
// Returns ErrNotFound when the asset row is missing and ErrLeaseNotFound when
// the asset exists but this job no longer owns a lease for it.
func (c *Cache) RenewLease(ctx context.Context, assetKey, jobID string) error {
	if assetKey == "" {
		return ErrEmptyID
	}
	if jobID == "" {
		return fmt.Errorf("workercache.RenewLease: jobID is required")
	}
	res, err := c.db.ExecContext(ctx, `
UPDATE cached_assets
   SET last_used_at = ?
 WHERE asset_key = ?
   AND EXISTS (
       SELECT 1 FROM cached_asset_leases
        WHERE asset_key = ? AND job_id = ?
   )`, time.Now().UTC().Format(time.RFC3339Nano), assetKey, assetKey, jobID)
	if err != nil {
		return fmt.Errorf("workercache.RenewLease(%q, %q): %w", assetKey, jobID, err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("workercache.RenewLease(%q, %q): rows affected: %w", assetKey, jobID, err)
	} else if n == 1 {
		return nil
	}
	var assetExists int
	if err := c.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM cached_assets WHERE asset_key = ?`, assetKey).Scan(&assetExists); err != nil {
		return fmt.Errorf("workercache.RenewLease(%q, %q): probe: %w", assetKey, jobID, err)
	}
	if assetExists == 0 {
		return fmt.Errorf("%w: asset_key=%s", ErrNotFound, assetKey)
	}
	return fmt.Errorf("%w: asset_key=%s job_id=%s", ErrLeaseNotFound, assetKey, jobID)
}

// Release removes only the (asset, job) lease relation and bumps
// last_used_at. Releasing another job's lease is a benign no-op.
// Returns ErrNotFound when the asset row itself is missing.
func (c *Cache) Release(ctx context.Context, assetKey, jobID string) error {
	if assetKey == "" {
		return ErrEmptyID
	}
	if jobID == "" {
		return fmt.Errorf("workercache.Release: jobID is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := c.db.ExecContext(ctx, `DELETE FROM cached_asset_leases WHERE asset_key = ? AND job_id = ?`, assetKey, jobID); err != nil {
		return fmt.Errorf("workercache.Release(%q, %q): lease delete: %w", assetKey, jobID, err)
	}
	res, err := c.db.ExecContext(ctx, `UPDATE cached_assets SET last_used_at = ? WHERE asset_key = ?`, now, assetKey)
	if err != nil {
		return fmt.Errorf("workercache.Release(%q, %q): %w", assetKey, jobID, err)
	}
	return mustHaveAffected(res, assetKey, "Release")
}

// Reserve protects an asset for an imminent job until expiresAt. Reservations
// are durable and participate in the same cleanup protection barrier as leases.
func (c *Cache) Reserve(ctx context.Context, assetKey, reservationID string, expiresAt time.Time) error {
	if assetKey == "" {
		return ErrEmptyID
	}
	if reservationID == "" || expiresAt.IsZero() {
		return fmt.Errorf("workercache.Reserve: reservation ID and expiry are required")
	}
	res, err := c.db.ExecContext(ctx, `INSERT OR REPLACE INTO cached_asset_reservations (asset_key, reservation_id, expires_at) SELECT asset_key, ?, ? FROM cached_assets WHERE asset_key = ?`, reservationID, expiresAt.UTC().Format(time.RFC3339Nano), assetKey)
	if err != nil {
		return fmt.Errorf("workercache.Reserve(%q, %q): %w", assetKey, reservationID, err)
	}
	return mustHaveAffected(res, assetKey, "Reserve")
}

// ReleaseReservation removes one future-job reservation. It is idempotent
// when the cache row still exists.
func (c *Cache) ReleaseReservation(ctx context.Context, assetKey, reservationID string) error {
	if assetKey == "" {
		return ErrEmptyID
	}
	if _, err := c.db.ExecContext(ctx, `DELETE FROM cached_asset_reservations WHERE asset_key = ? AND reservation_id = ?`, assetKey, reservationID); err != nil {
		return fmt.Errorf("workercache.ReleaseReservation(%q, %q): %w", assetKey, reservationID, err)
	}
	return nil
}

// DeleteIfUnleased atomically removes an unleased and unreserved cache row.
// The predicates close the List→Delete race when another job acquires a lease
// or future-job reservation while cleanup is scanning. Cleanup should prefer
// EvictIfUnleased so physical removal and index deletion share a write fence.
func (c *Cache) DeleteIfUnleased(ctx context.Context, assetKey string) error {
	if assetKey == "" {
		return ErrEmptyID
	}
	res, err := c.db.ExecContext(ctx, `DELETE FROM cached_assets WHERE asset_key = ? AND NOT EXISTS (SELECT 1 FROM cached_asset_leases WHERE asset_key = ?) AND NOT EXISTS (SELECT 1 FROM cached_asset_reservations WHERE asset_key = ? AND julianday(expires_at) > julianday('now'))`, assetKey, assetKey, assetKey)
	if err != nil {
		return fmt.Errorf("workercache.DeleteIfUnleased(%q): %w", assetKey, err)
	}
	if err := mustHaveAffected(res, assetKey, "DeleteIfUnleased"); err != nil {
		return err
	}
	c.deleteOrphanedBlobs(ctx)
	return nil
}

// deleteOrphanedBlobs removes blob rows no longer referenced by any asset.
// It is a best-effort hygiene pass: orphaned blobs are invisible to the
// asset-keyed read model, so a failed sweep is harmless and retried on a
// later delete/eviction.
func (c *Cache) deleteOrphanedBlobs(ctx context.Context) {
	_, _ = c.db.ExecContext(ctx,
		`DELETE FROM cached_blobs WHERE NOT EXISTS (SELECT 1 FROM cached_assets a WHERE a.content_hash = cached_blobs.content_hash)`)
}

// Delete removes the row. Returns ErrNotFound when no row matches.
// Cleanup uses EvictIfUnleased instead so physical removal and index deletion
// remain one fenced operation.
func (c *Cache) Delete(ctx context.Context, assetKey string) error {
	if assetKey == "" {
		return ErrEmptyID
	}
	res, err := c.db.ExecContext(ctx,
		`DELETE FROM cached_assets WHERE asset_key = ?`,
		assetKey,
	)
	if err != nil {
		return fmt.Errorf("workercache.Delete(%q): %w", assetKey, err)
	}
	if err := mustHaveAffected(res, assetKey, "Delete"); err != nil {
		return err
	}
	c.deleteOrphanedBlobs(ctx)
	return nil
}

// List returns all rows ordered by asset_key (deterministic for
// tests + supervisor scans).
func (c *Cache) List(ctx context.Context) ([]Entry, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT `+selectCols+selectFrom+` ORDER BY a.asset_key ASC`)
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
	if err := c.db.QueryRowContext(ctx, `SELECT (SELECT COUNT(1) FROM cached_assets), COALESCE((SELECT SUM(b.size_bytes) FROM cached_blobs b WHERE EXISTS (SELECT 1 FROM cached_assets a WHERE a.content_hash = b.content_hash)), 0)`).Scan(&entries, &bytes); err != nil {
		return 0, 0, fmt.Errorf("workercache.Size: %w", err)
	}
	return entries, bytes, nil
}

// ReadyKeys returns the canonical asset keys currently materialized on disk.
// It is a read-only, bounded heartbeat projection used by master placement;
// callers must not treat it as a lease or as proof that a file cannot be
// evicted before the task acquires its lease.
func (c *Cache) ReadyKeys(ctx context.Context) ([]string, error) {
	rows, err := c.db.QueryContext(ctx, `SELECT a.asset_key FROM cached_assets a JOIN cached_blobs b ON b.content_hash = a.content_hash WHERE b.download_complete = 1 ORDER BY a.asset_key`)
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

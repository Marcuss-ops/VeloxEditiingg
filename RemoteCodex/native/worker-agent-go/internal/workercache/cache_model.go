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
//     MarkBlobUsed / Release. The pressure controller uses the blob-level value
//     for LRU ordering; it is not a TTL eviction policy.
//
// The schema DDL, the row scanner and the helper predicates live in
// the sibling file cache_helpers.go.
package workercache

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
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
	// storedContentHash preserves the internal legacy:<asset> key so Find can
	// invalidate a broken physical blob without exposing synthetic identity.
	storedContentHash string
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
	ErrAssetNotReady      = errors.New("workercache: cached asset is not materialized")
)

// LeaseBinding is the physical binding validated while acquiring an asset
// lease. Callers that pass a cached asset to an executor should use this path
// rather than trusting a path resolved before protection was installed.
type LeaseBinding struct {
	AssetKey    assetref.AssetKey
	ContentHash assetref.ContentHash
	LocalPath   string
	SizeBytes   int64
}

// Cache is the SQLite-backed index over cached worker assets.
type Cache struct {
	db *sql.DB
	fs cacheFileSystem
	// root is the authorized physical cache tree for safe invalidation.
	root string
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
	return OpenWithRoot(path, "")
}

// OpenWithRoot opens the cache index and records the authorized physical blob
// root. Paths outside this tree are never removed during invalidation.
func OpenWithRoot(path, root string) (*Cache, error) {
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
	cleanRoot := ""
	if root != "" {
		absRoot, err := filepath.Abs(root)
		if err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("workercache.Open: resolve root: %w", err)
		}
		cleanRoot = filepath.Clean(absRoot)
	}
	return &Cache{db: db, fs: osCacheFileSystem{}, root: cleanRoot}, nil
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

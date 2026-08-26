package workercache

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"velox-shared/assetref"
)

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

	// A legacy/hashless entry uses the same deterministic local_path that the
	// verified transfer just promoted. Remove that orphaned physical index
	// before inserting the SHA-addressed row: local_path is UNIQUE, so waiting
	// until after INSERT would reject the legitimate legacy → SHA re-key.
	if oldBlobKey != "" && oldBlobKey != blobKey {
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM cached_blobs
			  WHERE content_hash = ?
			    AND local_path = ?
			    AND NOT EXISTS (
			      SELECT 1
			        FROM cached_assets
			       WHERE content_hash = ?
			         AND asset_key <> ?
			    )`,
			oldBlobKey, localPath, oldBlobKey, assetKey,
		); err != nil {
			return rollback(fmt.Errorf("workercache.MarkDownloadComplete(%q): remove legacy blob: %w", assetKey, err))
		}
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

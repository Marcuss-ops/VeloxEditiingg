package workercache

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
)

// EvictIfUnleased removes the physical file and its index row under one
// SQLite write fence. The write fence prevents Acquire/Reserve from winning
// between the physical removal and index deletion. A physical removal error
// rolls back the transaction and leaves the row available for a later retry.
// os.ErrNotExist is treated as an already-evicted file.
//
// With the v5 two-table model, the physical bytes belong to a blob that may be
// shared by several assets. The file is therefore removed only when the blob
// becomes orphaned (no remaining cached_assets row references it); otherwise
// the blob row and its file stay in place for the surviving assets.
func (c *Cache) EvictIfUnleased(ctx context.Context, assetKey, localPath string) error {
	if assetKey == "" {
		return ErrEmptyID
	}
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("workercache.EvictIfUnleased(%q): connection: %w", assetKey, err)
	}
	defer conn.Close()
	rollback := func(cause error) error {
		_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		return cause
	}
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return fmt.Errorf("workercache.EvictIfUnleased(%q): begin: %w", assetKey, err)
	}

	var blobKey, blobPath string
	err = conn.QueryRowContext(ctx, `SELECT a.content_hash, COALESCE(b.local_path, '')
		FROM cached_assets a
		LEFT JOIN cached_blobs b ON b.content_hash = a.content_hash
		WHERE a.asset_key = ?
		  AND NOT EXISTS (SELECT 1 FROM cached_asset_leases WHERE asset_key = ?)
		  AND NOT EXISTS (SELECT 1 FROM cached_asset_reservations WHERE asset_key = ? AND julianday(expires_at) > julianday('now'))`,
		assetKey, assetKey, assetKey).Scan(&blobKey, &blobPath)
	if errors.Is(err, sql.ErrNoRows) {
		return rollback(fmt.Errorf("%w: asset_key=%s", ErrNotFound, assetKey))
	}
	if err != nil {
		return rollback(fmt.Errorf("workercache.EvictIfUnleased(%q): eligibility: %w", assetKey, err))
	}

	res, err := conn.ExecContext(ctx, `DELETE FROM cached_assets WHERE asset_key = ?`, assetKey)
	if err != nil {
		return rollback(fmt.Errorf("workercache.EvictIfUnleased(%q): delete index: %w", assetKey, err))
	}
	if affected, err := res.RowsAffected(); err != nil {
		return rollback(fmt.Errorf("workercache.EvictIfUnleased(%q): rows affected: %w", assetKey, err))
	} else if affected != 1 {
		return rollback(fmt.Errorf("%w: asset_key=%s", ErrNotFound, assetKey))
	}

	// Remove the physical bytes only when the blob is no longer referenced.
	if blobKey != "" {
		var remaining int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(1) FROM cached_assets WHERE content_hash = ?`, blobKey).Scan(&remaining); err != nil {
			return rollback(fmt.Errorf("workercache.EvictIfUnleased(%q): orphan probe: %w", assetKey, err))
		}
		if remaining == 0 {
			removePath := blobPath
			if removePath == "" {
				removePath = localPath
			}
			if removePath != "" {
				if err := c.fs.Remove(removePath); err != nil && !errors.Is(err, os.ErrNotExist) {
					return rollback(fmt.Errorf("workercache.EvictIfUnleased(%q): physical remove: %w", assetKey, err))
				}
			}
			if _, err := conn.ExecContext(ctx, `DELETE FROM cached_blobs WHERE content_hash = ?`, blobKey); err != nil {
				return rollback(fmt.Errorf("workercache.EvictIfUnleased(%q): delete blob: %w", assetKey, err))
			}
		}
	}

	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return fmt.Errorf("workercache.EvictIfUnleased(%q): commit: %w", assetKey, err)
	}
	return nil
}

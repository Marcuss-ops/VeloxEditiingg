package workercache

import (
	"context"
	"errors"
	"fmt"
	"os"
)

// EvictIfUnleased removes the physical file and its index row under one
// SQLite write fence. The write fence prevents Acquire/Reserve from winning
// between the physical removal and index deletion. A physical removal error
// rolls back the transaction and leaves the row available for a later retry.
// os.ErrNotExist is treated as an already-evicted file.
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
	var eligible int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(1) FROM cached_assets WHERE asset_key = ? AND NOT EXISTS (SELECT 1 FROM cached_asset_leases WHERE asset_key = ?) AND NOT EXISTS (SELECT 1 FROM cached_asset_reservations WHERE asset_key = ? AND julianday(expires_at) > julianday('now'))`, assetKey, assetKey, assetKey).Scan(&eligible); err != nil {
		return rollback(fmt.Errorf("workercache.EvictIfUnleased(%q): eligibility: %w", assetKey, err))
	}
	if eligible == 0 {
		return rollback(fmt.Errorf("%w: asset_key=%s", ErrNotFound, assetKey))
	}
	if err := c.fs.Remove(localPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return rollback(fmt.Errorf("workercache.EvictIfUnleased(%q): physical remove: %w", assetKey, err))
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
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return fmt.Errorf("workercache.EvictIfUnleased(%q): commit: %w", assetKey, err)
	}
	return nil
}

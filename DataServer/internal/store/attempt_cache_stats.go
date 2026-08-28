package store

import (
	"context"
	"database/sql"
	"fmt"

	"velox-server/internal/taskattempts"
)

// PersistCacheStats hoists the worker's dotted-key cache counters into a
// typed row so the byte_hit_ratio can be computed in SQL. Idempotent
// INSERT OR REPLACE keyed by attempt_id.
func (r *SQLiteTaskAttemptRepository) PersistCacheStats(ctx context.Context, stats taskattempts.AttemptCacheStats) error {
	if stats.AttemptID == "" {
		return nil
	}
	if err := stats.NormalizeCacheAccounting(); err != nil {
		return fmt.Errorf("cache stats validate: %w", err)
	}
	_, err := r.store.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO task_attempt_cache_stats (
			attempt_id, cache_hits, cache_misses, cache_evictions,
			cache_corruptions, cache_bytes_used, cache_entries,
			cache_lookups, unique_assets_requested,
			cache_download_count, cache_download_bytes
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		stats.AttemptID, stats.CacheHits, stats.CacheMisses, stats.CacheEvictions,
		stats.CacheCorruptions, stats.CacheBytesUsed, stats.CacheEntries,
		stats.CacheLookups, stats.UniqueAssetsRequested,
		stats.CacheDownloadCount, stats.CacheDownloadBytes,
	)
	if err != nil {
		return fmt.Errorf("cache stats persist: %w", err)
	}
	return nil
}

// GetCacheStats returns the typed cache snapshot for an attempt, or
// (nil, nil) on miss.
func (r *SQLiteTaskAttemptRepository) GetCacheStats(ctx context.Context, attemptID string) (*taskattempts.AttemptCacheStats, error) {
	if attemptID == "" {
		return nil, nil
	}
	row := r.store.db.QueryRowContext(ctx,
		`SELECT attempt_id, cache_hits, cache_misses, cache_evictions,
		        cache_corruptions, cache_bytes_used, cache_entries,
		        cache_lookups, unique_assets_requested,
		        cache_download_count, cache_download_bytes
		 FROM task_attempt_cache_stats WHERE attempt_id = ?`,
		attemptID,
	)
	var s taskattempts.AttemptCacheStats
	err := row.Scan(
		&s.AttemptID, &s.CacheHits, &s.CacheMisses, &s.CacheEvictions,
		&s.CacheCorruptions, &s.CacheBytesUsed, &s.CacheEntries,
		&s.CacheLookups, &s.UniqueAssetsRequested,
		&s.CacheDownloadCount, &s.CacheDownloadBytes,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("cache stats get: %w", err)
	}
	return &s, nil
}

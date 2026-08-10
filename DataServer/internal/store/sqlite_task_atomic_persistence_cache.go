package store

// sqlite_task_atomic_persistence_cache.go: cache-stats + cost-basis
// attempt write helpers used by IngestTaskResultAtomic. Split out of
// sqlite_task_atomic_persistence_attempt.go.

import (
	"context"
	"database/sql"
	"fmt"

	"velox-server/internal/taskgraph"
)

// persistAttemptCacheStats persists the per-attempt cache snapshot.
func persistAttemptCacheStats(ctx context.Context, tx *sql.Tx, cmd taskgraph.IngestResultCommand) error {
	stats := cmd.CacheStats
	if stats.AttemptID == "" {
		return nil
	}
	if err := stats.NormalizeCacheAccounting(); err != nil {
		return fmt.Errorf("task ingest atomic cache stats validate: %w", err)
	}
	_, err := tx.ExecContext(ctx,
		`INSERT OR REPLACE INTO task_attempt_cache_stats (
			attempt_id, cache_hits, cache_misses, cache_evictions,
			cache_corruptions, cache_bytes_used, cache_entries,
			cache_lookups, unique_assets_requested
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		stats.AttemptID, stats.CacheHits, stats.CacheMisses,
		stats.CacheEvictions, stats.CacheCorruptions,
		stats.CacheBytesUsed, stats.CacheEntries,
		stats.CacheLookups, stats.UniqueAssetsRequested,
	)
	if err != nil {
		return fmt.Errorf("task ingest atomic cache stats: %w", err)
	}
	return nil
}

// persistAttemptCostBasis persists the cost-model envelope.
func persistAttemptCostBasis(ctx context.Context, tx *sql.Tx, cmd taskgraph.IngestResultCommand) error {
	if cmd.CostBasis.AttemptID == "" {
		return nil
	}
	_, err := tx.ExecContext(ctx,
		`INSERT OR REPLACE INTO task_attempt_cost_basis (
			attempt_id, cpu_price_per_second, storage_price_per_gb, network_price_per_gb,
			cpu_time_seconds_total, storage_gb_written, network_gb_egressed, output_minutes_total
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		cmd.CostBasis.AttemptID, cmd.CostBasis.CPUPricePerSecond,
		cmd.CostBasis.StoragePricePerGB, cmd.CostBasis.NetworkPricePerGB,
		cmd.CostBasis.CPUTimeSecondsTotal, cmd.CostBasis.StorageGBWritten,
		cmd.CostBasis.NetworkGBEgressed, cmd.CostBasis.OutputMinutesTotal,
	)
	if err != nil {
		return fmt.Errorf("task ingest atomic cost basis: %w", err)
	}
	return nil
}

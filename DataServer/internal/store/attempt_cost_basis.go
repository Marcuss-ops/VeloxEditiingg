package store

import (
	"context"
	"database/sql"
	"fmt"

	"velox-server/internal/taskattempts"
)

// PersistCostBasis hoists the cost-model envelope for one attempt; the
// master derives cost_per_output_minute from this row via ComputeCostBasis.
func (r *SQLiteTaskAttemptRepository) PersistCostBasis(ctx context.Context, basis taskattempts.AttemptCostBasis) error {
	if basis.AttemptID == "" {
		return nil
	}
	_, err := r.store.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO task_attempt_cost_basis (
			attempt_id, cpu_price_per_second, storage_price_per_gb, network_price_per_gb,
			cpu_time_seconds_total, storage_gb_written, network_gb_egressed, output_minutes_total
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		basis.AttemptID, basis.CPUPricePerSecond, basis.StoragePricePerGB, basis.NetworkPricePerGB,
		basis.CPUTimeSecondsTotal, basis.StorageGBWritten, basis.NetworkGBEgressed, basis.OutputMinutesTotal,
	)
	if err != nil {
		return fmt.Errorf("cost basis persist: %w", err)
	}
	return nil
}

// GetCostBasis returns the typed cost envelope for an attempt, or
// (nil, nil) on miss.
func (r *SQLiteTaskAttemptRepository) GetCostBasis(ctx context.Context, attemptID string) (*taskattempts.AttemptCostBasis, error) {
	if attemptID == "" {
		return nil, nil
	}
	row := r.store.db.QueryRowContext(ctx,
		`SELECT attempt_id, cpu_price_per_second, storage_price_per_gb, network_price_per_gb,
		        cpu_time_seconds_total, storage_gb_written, network_gb_egressed, output_minutes_total
		 FROM task_attempt_cost_basis WHERE attempt_id = ?`,
		attemptID,
	)
	var b taskattempts.AttemptCostBasis
	err := row.Scan(
		&b.AttemptID, &b.CPUPricePerSecond, &b.StoragePricePerGB, &b.NetworkPricePerGB,
		&b.CPUTimeSecondsTotal, &b.StorageGBWritten, &b.NetworkGBEgressed, &b.OutputMinutesTotal,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("cost basis get: %w", err)
	}
	b.Compute()
	return &b, nil
}

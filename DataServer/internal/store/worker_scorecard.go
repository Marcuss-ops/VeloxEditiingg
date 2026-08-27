// worker_scorecard.go owns the worker_capacity_scorecards row lifecycle:
// persist (upsert) a computed scorecard and query it back for registry
// hydration or admin-API exposure.

package store

import (
	"context"
	"database/sql"
	"fmt"
)

// ScorecardRow is the persisted projection of a CapacityScorecard.
type ScorecardRow struct {
	WorkerID    string
	RenderSlots int
	PrefetchSlots int
	PublisherSlots int
	RAMSlots    int
	CPUSlots    int
	DiskSlots   int
	NetworkSlots int
	LimitingResource string
	TotalRAMBytes     int64
	AvailableRAMBytes int64
	EffectiveCPUCores int32
	DiskReadMbps      float64
	DiskWriteMbps     float64
	DownloadMbps      float64
	UploadMbps        float64
	RAMPerJobBytes       int64
	CPUCoresPerJob       float64
	DiskMBpsPerJob       float64
	NetworkMbpsPerJob    float64
	RenderWallMsPerJob   int64
	PrefetchWallMsPerJob int64
	PublishWallMsPerJob  int64
	ComputedAt string
}

// UpsertScorecard persists a computed CapacityScorecard for a worker.
func (s *SQLiteStore) UpsertScorecard(ctx context.Context, row ScorecardRow) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("upsert scorecard: store not initialized")
	}
	if row.WorkerID == "" {
		return fmt.Errorf("upsert scorecard: worker_id is required")
	}
	if row.ComputedAt == "" {
		row.ComputedAt = nowRFC3339()
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO worker_capacity_scorecards (
		worker_id, render_slots, prefetch_slots, publisher_slots,
		ram_slots, cpu_slots, disk_slots, network_slots, limiting_resource,
		total_ram_bytes, available_ram_bytes, effective_cpu_cores,
		disk_read_mbps, disk_write_mbps, download_mbps, upload_mbps,
		ram_per_job_bytes, cpu_cores_per_job, disk_mbps_per_job, network_mbps_per_job,
		render_wall_ms_per_job, prefetch_wall_ms_per_job, publish_wall_ms_per_job,
		computed_at
	) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
	ON CONFLICT(worker_id) DO UPDATE SET
		render_slots=excluded.render_slots,
		prefetch_slots=excluded.prefetch_slots,
		publisher_slots=excluded.publisher_slots,
		ram_slots=excluded.ram_slots,
		cpu_slots=excluded.cpu_slots,
		disk_slots=excluded.disk_slots,
		network_slots=excluded.network_slots,
		limiting_resource=excluded.limiting_resource,
		total_ram_bytes=excluded.total_ram_bytes,
		available_ram_bytes=excluded.available_ram_bytes,
		effective_cpu_cores=excluded.effective_cpu_cores,
		disk_read_mbps=excluded.disk_read_mbps,
		disk_write_mbps=excluded.disk_write_mbps,
		download_mbps=excluded.download_mbps,
		upload_mbps=excluded.upload_mbps,
		ram_per_job_bytes=excluded.ram_per_job_bytes,
		cpu_cores_per_job=excluded.cpu_cores_per_job,
		disk_mbps_per_job=excluded.disk_mbps_per_job,
		network_mbps_per_job=excluded.network_mbps_per_job,
		render_wall_ms_per_job=excluded.render_wall_ms_per_job,
		prefetch_wall_ms_per_job=excluded.prefetch_wall_ms_per_job,
		publish_wall_ms_per_job=excluded.publish_wall_ms_per_job,
		computed_at=excluded.computed_at`,
		row.WorkerID, row.RenderSlots, row.PrefetchSlots, row.PublisherSlots,
		row.RAMSlots, row.CPUSlots, row.DiskSlots, row.NetworkSlots, row.LimitingResource,
		row.TotalRAMBytes, row.AvailableRAMBytes, row.EffectiveCPUCores,
		row.DiskReadMbps, row.DiskWriteMbps, row.DownloadMbps, row.UploadMbps,
		row.RAMPerJobBytes, row.CPUCoresPerJob, row.DiskMBpsPerJob, row.NetworkMbpsPerJob,
		row.RenderWallMsPerJob, row.PrefetchWallMsPerJob, row.PublishWallMsPerJob,
		row.ComputedAt,
	)
	return err
}

// GetScorecard returns the persisted scorecard for a worker, or nil if not found.
func (s *SQLiteStore) GetScorecard(ctx context.Context, workerID string) (*ScorecardRow, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("get scorecard: store not initialized")
	}
	if workerID == "" {
		return nil, fmt.Errorf("get scorecard: worker_id is required")
	}
	row := &ScorecardRow{}
	err := s.db.QueryRowContext(ctx, `SELECT
		worker_id, render_slots, prefetch_slots, publisher_slots,
		ram_slots, cpu_slots, disk_slots, network_slots, limiting_resource,
		total_ram_bytes, available_ram_bytes, effective_cpu_cores,
		disk_read_mbps, disk_write_mbps, download_mbps, upload_mbps,
		ram_per_job_bytes, cpu_cores_per_job, disk_mbps_per_job, network_mbps_per_job,
		render_wall_ms_per_job, prefetch_wall_ms_per_job, publish_wall_ms_per_job,
		computed_at
	FROM worker_capacity_scorecards WHERE worker_id = ?`, workerID).Scan(
		&row.WorkerID, &row.RenderSlots, &row.PrefetchSlots, &row.PublisherSlots,
		&row.RAMSlots, &row.CPUSlots, &row.DiskSlots, &row.NetworkSlots, &row.LimitingResource,
		&row.TotalRAMBytes, &row.AvailableRAMBytes, &row.EffectiveCPUCores,
		&row.DiskReadMbps, &row.DiskWriteMbps, &row.DownloadMbps, &row.UploadMbps,
		&row.RAMPerJobBytes, &row.CPUCoresPerJob, &row.DiskMBpsPerJob, &row.NetworkMbpsPerJob,
		&row.RenderWallMsPerJob, &row.PrefetchWallMsPerJob, &row.PublishWallMsPerJob,
		&row.ComputedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get scorecard: %w", err)
	}
	return row, nil
}

// GetScorecardsBulk returns persisted scorecards for multiple workers.
func (s *SQLiteStore) GetScorecardsBulk(ctx context.Context, workerIDs []string) (map[string]*ScorecardRow, error) {
	out := make(map[string]*ScorecardRow, len(workerIDs))
	if len(workerIDs) == 0 || s == nil || s.db == nil {
		return out, nil
	}
	args := make([]interface{}, 0, len(workerIDs))
	placeholders := make([]byte, 0, len(workerIDs)*2)
	for i, id := range workerIDs {
		if i > 0 {
			placeholders = append(placeholders, ',')
		}
		placeholders = append(placeholders, '?')
		args = append(args, id)
	}
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`SELECT
		worker_id, render_slots, prefetch_slots, publisher_slots,
		ram_slots, cpu_slots, disk_slots, network_slots, limiting_resource,
		total_ram_bytes, available_ram_bytes, effective_cpu_cores,
		disk_read_mbps, disk_write_mbps, download_mbps, upload_mbps,
		ram_per_job_bytes, cpu_cores_per_job, disk_mbps_per_job, network_mbps_per_job,
		render_wall_ms_per_job, prefetch_wall_ms_per_job, publish_wall_ms_per_job,
		computed_at
	FROM worker_capacity_scorecards WHERE worker_id IN (%s)`, string(placeholders)), args...)
	if err != nil {
		return out, fmt.Errorf("get scorecards bulk: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		row := &ScorecardRow{}
		if err := rows.Scan(
			&row.WorkerID, &row.RenderSlots, &row.PrefetchSlots, &row.PublisherSlots,
			&row.RAMSlots, &row.CPUSlots, &row.DiskSlots, &row.NetworkSlots, &row.LimitingResource,
			&row.TotalRAMBytes, &row.AvailableRAMBytes, &row.EffectiveCPUCores,
			&row.DiskReadMbps, &row.DiskWriteMbps, &row.DownloadMbps, &row.UploadMbps,
			&row.RAMPerJobBytes, &row.CPUCoresPerJob, &row.DiskMBpsPerJob, &row.NetworkMbpsPerJob,
			&row.RenderWallMsPerJob, &row.PrefetchWallMsPerJob, &row.PublishWallMsPerJob,
			&row.ComputedAt,
		); err != nil {
			return out, fmt.Errorf("get scorecards bulk scan: %w", err)
		}
		out[row.WorkerID] = row
	}
	return out, rows.Err()
}

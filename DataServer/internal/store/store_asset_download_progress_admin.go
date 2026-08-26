package store

import (
	"context"
	"fmt"
)

// ListAssetDownloadProgressForJob returns the latest asset download state
// for all assets in a job, without M2M client ownership filtering.
// Intended for admin/operator read paths (fleetctl, observability /live).
func (s *SQLiteStore) ListAssetDownloadProgressForJob(ctx context.Context, jobID string) ([]AssetDownloadProgressView, error) {
	if s == nil || s.db == nil || jobID == "" {
		return nil, fmt.Errorf("asset download progress: store not initialized or job_id empty")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT r.job_id, d.worker_id, d.asset_key, d.asset_id, d.role, d.state,
		       d.bytes_downloaded, d.bytes_total, d.bytes_per_second, d.eta_seconds,
		       d.attempt, d.shared_waiters, d.cache_hit, d.updated_at,
		       d.error_code, d.error_detail, r.task_id, r.scene_ids_json
		FROM job_asset_refs r
		JOIN worker_asset_downloads d ON d.worker_id=r.worker_id AND d.asset_key=r.asset_key
		WHERE r.job_id=?
		ORDER BY d.updated_at DESC, d.asset_key ASC`, jobID)
	if err != nil {
		return nil, fmt.Errorf("asset download progress: admin list job %s: %w", jobID, err)
	}
	defer rows.Close()
	return scanAssetDownloadProgress(rows, jobID)
}

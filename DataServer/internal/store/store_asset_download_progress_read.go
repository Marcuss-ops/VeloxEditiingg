package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

// AssetDownloadProgressView is the JSON-ready job-scoped latest state.
type AssetDownloadProgressView struct {
	JobID           string   `json:"job_id"`
	WorkerID        string   `json:"worker_id"`
	AssetKey        string   `json:"asset_key"`
	AssetID         string   `json:"asset_id"`
	Role            string   `json:"role"`
	State           string   `json:"state"`
	BytesDownloaded int64    `json:"bytes_downloaded"`
	BytesTotal      int64    `json:"bytes_total"`
	BytesPerSecond  float64  `json:"bytes_per_second"`
	ETASeconds      int64    `json:"eta_seconds"`
	Attempt         int      `json:"attempt"`
	SharedWaiters   int      `json:"shared_waiters"`
	CacheHit        bool     `json:"cache_hit"`
	UpdatedAt       string   `json:"updated_at"`
	ErrorCode       string   `json:"error_code,omitempty"`
	ErrorDetail     string   `json:"error_detail,omitempty"`
	TaskID          string   `json:"task_id,omitempty"`
	SceneIDs        []string `json:"scene_ids,omitempty"`
}

func (s *SQLiteStore) ListAssetDownloadProgressForJob(ctx context.Context, jobID string) ([]AssetDownloadProgressView, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("asset download progress: store not initialized")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT r.job_id, d.worker_id, d.asset_key, d.asset_id, d.role, d.state,
		       d.bytes_downloaded, d.bytes_total, d.bytes_per_second, d.eta_seconds,
		       d.attempt, d.shared_waiters, d.cache_hit, d.updated_at,
		       d.error_code, d.error_detail, r.task_id, r.scene_ids_json
		FROM job_asset_refs r
		JOIN worker_asset_downloads d
		  ON d.worker_id=r.worker_id AND d.asset_key=r.asset_key
		WHERE r.job_id=?
		ORDER BY d.updated_at DESC, d.asset_key ASC`, jobID)
	if err != nil {
		return nil, fmt.Errorf("asset download progress: list job %s: %w", jobID, err)
	}
	defer rows.Close()
	var out []AssetDownloadProgressView
	for rows.Next() {
		var v AssetDownloadProgressView
		var cacheHit int
		var sceneJSON string
		if err := rows.Scan(&v.JobID, &v.WorkerID, &v.AssetKey, &v.AssetID, &v.Role, &v.State,
			&v.BytesDownloaded, &v.BytesTotal, &v.BytesPerSecond, &v.ETASeconds,
			&v.Attempt, &v.SharedWaiters, &cacheHit, &v.UpdatedAt,
			&v.ErrorCode, &v.ErrorDetail, &v.TaskID, &sceneJSON); err != nil {
			return nil, fmt.Errorf("asset download progress: scan job %s: %w", jobID, err)
		}
		v.CacheHit = cacheHit != 0
		if sceneJSON != "" {
			_ = json.Unmarshal([]byte(sceneJSON), &v.SceneIDs)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("asset download progress: rows job %s: %w", jobID, err)
	}
	return out, nil
}

package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// AssetDownloadJobRef preserves per-job ownership metadata for a shared transfer.
type AssetDownloadJobRef struct {
	JobID    string
	TaskID   string
	SceneIDs []string
}

// AssetDownloadProgressRecord is the master-side representation of one worker checkpoint.
type AssetDownloadProgressRecord struct {
	WorkerID           string
	TransferID         string
	AssetKey           string
	AssetID            string
	Role               string
	State              string
	BytesDownloaded    int64
	BytesTotal         int64
	BytesPerSecond     float64
	ETASeconds         int64
	Attempt            int
	SharedWaiters      int
	CacheHit           bool
	QueuedAt           time.Time
	StartedAt          time.Time
	UpdatedAt          time.Time
	CompletedAt        time.Time
	TaskID             string
	JobIDs             []string
	JobRefs            []AssetDownloadJobRef
	SceneIDs           []string
	MIMEType           string
	SHA256             string
	ErrorCode          string
	ErrorDetail        string
	CheckpointSequence int64
	TransferGeneration int64
	ReceivedAt         time.Time
}

// IngestAssetDownloadProgress atomically upserts the latest physical transfer
// state and replaces its job projection. Lower checkpoint sequences are ignored;
// terminal states dominate non-terminal updates even if an old checkpoint arrives.
func (s *SQLiteStore) IngestAssetDownloadProgress(ctx context.Context, p AssetDownloadProgressRecord) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("asset download progress: store not initialized")
	}
	if p.WorkerID == "" || p.AssetKey == "" {
		return fmt.Errorf("asset download progress: worker_id and asset_key are required")
	}
	if p.ReceivedAt.IsZero() {
		p.ReceivedAt = time.Now().UTC()
	}
	if p.BytesDownloaded < 0 || p.BytesTotal < 0 {
		return fmt.Errorf("asset download progress: byte counters must be non-negative")
	}
	if p.BytesTotal > 0 && p.BytesDownloaded > p.BytesTotal {
		p.BytesDownloaded = p.BytesTotal
	}
	jobRefs := normalizeAssetJobRefs(p.JobRefs, p.JobIDs, p.TaskID, p.SceneIDs)
	scenes, err := json.Marshal(uniqueNonEmptyStrings(p.SceneIDs))
	if err != nil {
		return fmt.Errorf("asset download progress: encode scene_ids: %w", err)
	}
	now := p.ReceivedAt.UTC().Format(time.RFC3339Nano)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("asset download progress: begin: %w", err)
	}
	defer tx.Rollback()

	queuedAt := formatProgressTime(p.QueuedAt)
	startedAt := formatProgressTime(p.StartedAt)
	updatedAt := formatProgressTime(p.UpdatedAt)
	completedAt := formatProgressTime(p.CompletedAt)
	result, err := tx.ExecContext(ctx, `
		INSERT INTO worker_asset_downloads (
			worker_id, asset_key, transfer_id, asset_id, role, state,
			bytes_downloaded, bytes_total, bytes_per_second, eta_seconds,
			attempt, shared_waiters, cache_hit, queued_at, started_at,
			updated_at, completed_at, task_id, scene_ids_json, mime_type,
			sha256, error_code, error_detail, checkpoint_sequence, transfer_generation, received_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(worker_id, asset_key) DO UPDATE SET
			transfer_id=excluded.transfer_id, asset_id=excluded.asset_id,
			role=excluded.role, state=excluded.state,
			bytes_downloaded=excluded.bytes_downloaded, bytes_total=excluded.bytes_total,
			bytes_per_second=excluded.bytes_per_second, eta_seconds=excluded.eta_seconds,
			attempt=excluded.attempt, shared_waiters=excluded.shared_waiters,
			cache_hit=excluded.cache_hit, queued_at=excluded.queued_at,
			started_at=excluded.started_at, updated_at=excluded.updated_at,
			completed_at=excluded.completed_at, task_id=excluded.task_id,
			scene_ids_json=excluded.scene_ids_json, mime_type=excluded.mime_type,
			sha256=excluded.sha256, error_code=excluded.error_code,
			error_detail=excluded.error_detail,
			checkpoint_sequence=excluded.checkpoint_sequence,
			transfer_generation=excluded.transfer_generation,
			received_at=excluded.received_at
		WHERE (excluded.transfer_generation > worker_asset_downloads.transfer_generation)
		   OR (excluded.transfer_generation = worker_asset_downloads.transfer_generation
		       AND worker_asset_downloads.state NOT IN ('READY','FAILED','CANCELLED')
		       AND (excluded.checkpoint_sequence > worker_asset_downloads.checkpoint_sequence
		            OR excluded.state IN ('READY','FAILED','CANCELLED')))
		   OR (excluded.transfer_generation = worker_asset_downloads.transfer_generation
		       AND worker_asset_downloads.state IN ('READY','FAILED','CANCELLED')
		       AND excluded.state IN ('READY','FAILED','CANCELLED')
		       AND excluded.checkpoint_sequence > worker_asset_downloads.checkpoint_sequence)`,
		p.WorkerID, p.AssetKey, p.TransferID, p.AssetID, p.Role, p.State,
		p.BytesDownloaded, p.BytesTotal, p.BytesPerSecond, p.ETASeconds,
		p.Attempt, p.SharedWaiters, boolToSQLite(p.CacheHit), queuedAt, startedAt,
		updatedAt, completedAt, p.TaskID, string(scenes), p.MIMEType, p.SHA256,
		p.ErrorCode, p.ErrorDetail, p.CheckpointSequence, p.TransferGeneration, now,
	)
	if err != nil {
		return fmt.Errorf("asset download progress: upsert latest: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("asset download progress: rows affected: %w", err)
	}
	if changed == 0 {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("asset download progress: commit stale checkpoint: %w", err)
		}
		return nil
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM job_asset_refs WHERE worker_id=? AND asset_key=?`, p.WorkerID, p.AssetKey); err != nil {
		return fmt.Errorf("asset download progress: replace refs: %w", err)
	}
	for _, ref := range jobRefs {
		refScenes, err := json.Marshal(uniqueNonEmptyStrings(ref.SceneIDs))
		if err != nil {
			return fmt.Errorf("asset download progress: encode job scenes: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO job_asset_refs (
				job_id, worker_id, asset_key, transfer_id, asset_id, role,
				scene_ids_json, task_id, created_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(job_id, worker_id, asset_key) DO UPDATE SET
				transfer_id=excluded.transfer_id, asset_id=excluded.asset_id,
				role=excluded.role, scene_ids_json=excluded.scene_ids_json,
				task_id=excluded.task_id`,
			ref.JobID, p.WorkerID, p.AssetKey, p.TransferID, p.AssetID, p.Role,
			string(refScenes), ref.TaskID, now,
		); err != nil {
			return fmt.Errorf("asset download progress: upsert job ref %s: %w", ref.JobID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("asset download progress: commit: %w", err)
	}
	return nil
}

func normalizeAssetJobRefs(refs []AssetDownloadJobRef, jobIDs []string, taskID string, sceneIDs []string) []AssetDownloadJobRef {
	if len(refs) == 0 {
		refs = make([]AssetDownloadJobRef, 0, len(jobIDs))
		for _, jobID := range uniqueNonEmptyStrings(jobIDs) {
			refs = append(refs, AssetDownloadJobRef{JobID: jobID, TaskID: taskID, SceneIDs: sceneIDs})
		}
	}
	seen := make(map[string]struct{}, len(refs))
	out := make([]AssetDownloadJobRef, 0, len(refs))
	for _, ref := range refs {
		if ref.JobID == "" {
			continue
		}
		if _, ok := seen[ref.JobID]; ok {
			continue
		}
		seen[ref.JobID] = struct{}{}
		ref.SceneIDs = uniqueNonEmptyStrings(ref.SceneIDs)
		out = append(out, ref)
	}
	return out
}

func formatProgressTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func boolToSQLite(v bool) int {
	if v {
		return 1
	}
	return 0
}

func uniqueNonEmptyStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

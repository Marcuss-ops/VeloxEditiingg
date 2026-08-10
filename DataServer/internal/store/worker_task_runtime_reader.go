package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

// WorkerTaskRuntimeRow is the live, heartbeat-derived view of an active
// Attempt. It is intentionally separate from durable task_attempts history.
type WorkerTaskRuntimeRow struct {
	TaskID            string
	JobID             string
	AttemptID         string
	AttemptNumber     int
	WorkerID          string
	LeaseID           string
	RuntimeStatus     string
	ProgressPercent   int
	ProgressPhase     string
	CurrentScene      int
	TotalScenes       int
	CurrentSegment    int
	TotalSegments     int
	FramesEncoded     int64
	FramesDecoded     int64
	FramesComposited  int64
	FFmpegSpeedX      float64
	ElapsedMS         int64
	CumulativeMetrics map[string]any
	StartedAt         string
	LastProgressAt    string
	UpdatedAt         string
}

// GetWorkerTaskRuntimeByJob returns the current live Attempt projection for
// a job. A job normally has one task; newest update wins for defensive
// compatibility with multi-task jobs.
func (s *SQLiteStore) GetWorkerTaskRuntimeByJob(ctx context.Context, jobID string) (*WorkerTaskRuntimeRow, error) {
	if s == nil || s.db == nil || jobID == "" {
		return nil, nil
	}
	row := s.db.QueryRowContext(ctx, `
		SELECT task_id, job_id, attempt_id, attempt_number, worker_id, lease_id,
		       runtime_status, progress_percent, progress_stage, current_scene,
		       total_scenes, current_segment, total_segments, frames_encoded,
		       frames_decoded, frames_composited, ffmpeg_speed_x, elapsed_ms,
		       cumulative_metrics_json, started_at, last_progress_at, updated_at
		  FROM worker_task_runtime
		 WHERE job_id=?
		 ORDER BY updated_at DESC, task_id DESC
		 LIMIT 1`, jobID)

	var runtime WorkerTaskRuntimeRow
	var metricsJSON sql.NullString
	if err := row.Scan(
		&runtime.TaskID, &runtime.JobID, &runtime.AttemptID, &runtime.AttemptNumber,
		&runtime.WorkerID, &runtime.LeaseID, &runtime.RuntimeStatus,
		&runtime.ProgressPercent, &runtime.ProgressPhase, &runtime.CurrentScene,
		&runtime.TotalScenes, &runtime.CurrentSegment, &runtime.TotalSegments,
		&runtime.FramesEncoded, &runtime.FramesDecoded, &runtime.FramesComposited,
		&runtime.FFmpegSpeedX, &runtime.ElapsedMS, &metricsJSON, &runtime.StartedAt,
		&runtime.LastProgressAt, &runtime.UpdatedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("get worker task runtime: %w", err)
	}
	if metricsJSON.Valid && metricsJSON.String != "" {
		if err := json.Unmarshal([]byte(metricsJSON.String), &runtime.CumulativeMetrics); err != nil {
			return nil, fmt.Errorf("decode worker task runtime metrics: %w", err)
		}
	}
	return &runtime, nil
}

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	sharedtelemetry "velox-shared/telemetry"
)

// WorkerTaskRuntimeRow is the live, heartbeat-derived view of an active
// Attempt. It is intentionally separate from durable task_attempts history.
type WorkerTaskRuntimeRow struct {
	TaskID                 string
	JobID                  string
	AttemptID              string
	AttemptNumber          int
	WorkerID               string
	LeaseID                string
	RuntimeStatus          string
	WorkerConnectionState  string
	ProgressPercent        int
	ProgressPhase          string
	CurrentScene           int
	TotalScenes            int
	CurrentSegment         int
	TotalSegments          int
	FramesEncoded          int64
	FramesDecoded          int64
	FramesComposited       int64
	FFmpegSpeedX           float64
	ElapsedMS              int64
	CumulativeMetrics      map[string]any
	CanonicalAttemptEvents []map[string]any
	// AttemptMilestones is the worker's monotonic milestone timeline folded
	// into the same canonical_events_json column by the heartbeat reconciler
	// (see mergeCanonicalEventsAndMilestones). It is a live-only projection:
	// the durable attempt report carries the same timeline after completion.
	AttemptMilestones []sharedtelemetry.AttemptMilestoneSample
	StartedAt         string
	LastProgressAt    string
	UpdatedAt         string
}

// GetWorkerTaskRuntimeByTask returns the current live Attempt projection
// for one task within a job. Task identity is the canonical selector when
// the job contains more than one task.
func (s *SQLiteStore) GetWorkerTaskRuntimeByTask(ctx context.Context, taskID, jobID string) (*WorkerTaskRuntimeRow, error) {
	if s == nil || s.db == nil || taskID == "" || jobID == "" {
		return nil, nil
	}
	row := s.db.QueryRowContext(ctx, `
		SELECT r.task_id, r.job_id, r.attempt_id, r.attempt_number, r.worker_id, r.lease_id,
		       r.runtime_status, w.connection_state, r.progress_percent, r.progress_stage, r.current_scene,
		       r.total_scenes, r.current_segment, r.total_segments, r.frames_encoded,
		       r.frames_decoded, r.frames_composited, r.ffmpeg_speed_x, r.elapsed_ms,
		       r.cumulative_metrics_json, r.canonical_events_json, r.started_at, r.last_progress_at, r.updated_at
		  FROM worker_task_runtime r
		  INNER JOIN workers w ON w.worker_id = r.worker_id
		 WHERE r.task_id=? AND r.job_id=?
		 ORDER BY r.updated_at DESC, r.task_id DESC
		 LIMIT 1`, taskID, jobID)

	return scanWorkerTaskRuntimeRow(row)
}

// GetWorkerTaskRuntimeByJob preserves the legacy job-scoped reader contract.
// New canonical read paths should use GetWorkerTaskRuntimeByTask so a
// multi-task job cannot project a different task's Attempt.
func (s *SQLiteStore) GetWorkerTaskRuntimeByJob(ctx context.Context, jobID string) (*WorkerTaskRuntimeRow, error) {
	if s == nil || s.db == nil || jobID == "" {
		return nil, nil
	}
	row := s.db.QueryRowContext(ctx, `
		SELECT r.task_id, r.job_id, r.attempt_id, r.attempt_number, r.worker_id, r.lease_id,
		       r.runtime_status, w.connection_state, r.progress_percent, r.progress_stage, r.current_scene,
		       r.total_scenes, r.current_segment, r.total_segments, r.frames_encoded,
		       r.frames_decoded, r.frames_composited, r.ffmpeg_speed_x, r.elapsed_ms,
		       r.cumulative_metrics_json, r.canonical_events_json, r.started_at, r.last_progress_at, r.updated_at
		  FROM worker_task_runtime r
		 INNER JOIN workers w ON w.worker_id = r.worker_id
		 WHERE r.job_id=?
		 ORDER BY r.updated_at DESC, r.task_id DESC
		 LIMIT 1`, jobID)
	return scanWorkerTaskRuntimeRow(row)
}

func scanWorkerTaskRuntimeRow(row *sql.Row) (*WorkerTaskRuntimeRow, error) {
	var runtime WorkerTaskRuntimeRow
	var metricsJSON sql.NullString
	var progressPhase sql.NullString
	var lastProgressAt sql.NullString
	var canonicalEventsJSON sql.NullString
	if err := row.Scan(
		&runtime.TaskID, &runtime.JobID, &runtime.AttemptID, &runtime.AttemptNumber,
		&runtime.WorkerID, &runtime.LeaseID, &runtime.RuntimeStatus, &runtime.WorkerConnectionState,
		&runtime.ProgressPercent, &progressPhase, &runtime.CurrentScene,
		&runtime.TotalScenes, &runtime.CurrentSegment, &runtime.TotalSegments,
		&runtime.FramesEncoded, &runtime.FramesDecoded, &runtime.FramesComposited,
		&runtime.FFmpegSpeedX, &runtime.ElapsedMS, &metricsJSON, &canonicalEventsJSON, &runtime.StartedAt,
		&lastProgressAt, &runtime.UpdatedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("get worker task runtime: %w", err)
	}
	if progressPhase.Valid {
		runtime.ProgressPhase = progressPhase.String
	}
	if lastProgressAt.Valid {
		runtime.LastProgressAt = lastProgressAt.String
	}
	if canonicalEventsJSON.Valid && canonicalEventsJSON.String != "" {
		events, milestones, decodeErr := decodeCanonicalEventsAndMilestones(canonicalEventsJSON.String)
		if decodeErr != nil {
			return nil, fmt.Errorf("decode worker task runtime canonical events: %w", decodeErr)
		}
		runtime.CanonicalAttemptEvents = events
		runtime.AttemptMilestones = milestones
	}
	if metricsJSON.Valid && metricsJSON.String != "" {
		if err := json.Unmarshal([]byte(metricsJSON.String), &runtime.CumulativeMetrics); err != nil {
			return nil, fmt.Errorf("decode worker task runtime metrics: %w", err)
		}
	}
	return &runtime, nil
}

// decodeCanonicalEventsAndMilestones splits the folded canonical_events_json
// array back into canonical lifecycle events (elements carrying event_id) and
// attempt milestone samples (all other elements, which carry name/sequence/
// elapsed_ms). Rows written before milestone support contain only events, so
// the milestone slice is empty for them. A malformed column is a hard read
// error, matching the pre-milestone reader behavior.
func decodeCanonicalEventsAndMilestones(raw string) ([]map[string]any, []sharedtelemetry.AttemptMilestoneSample, error) {
	var elements []map[string]any
	if err := json.Unmarshal([]byte(raw), &elements); err != nil {
		return nil, nil, err
	}
	var events []map[string]any
	var milestones []sharedtelemetry.AttemptMilestoneSample
	for _, element := range elements {
		if eventID, _ := element["event_id"].(string); eventID != "" {
			events = append(events, element)
			continue
		}
		encoded, err := json.Marshal(element)
		if err != nil {
			continue
		}
		var sample sharedtelemetry.AttemptMilestoneSample
		if json.Unmarshal(encoded, &sample) == nil && sample.Name != "" {
			milestones = append(milestones, sample)
		}
	}
	if events == nil {
		events = []map[string]any{}
	}
	return events, milestones, nil
}

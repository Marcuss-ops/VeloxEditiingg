package store

import (
	"context"
	"fmt"
	"strings"

	"velox-server/internal/taskattempts"
)

var _ taskattempts.SuccessfulSegmentTimingReader = (*SQLiteTaskAttemptRepository)(nil)

// ListSuccessfulSegmentTimings returns only evidence attached to the newest
// successful attempt for each requested task. This makes result recovery
// stable across worker retries and Master restarts.
func (r *SQLiteTaskAttemptRepository) ListSuccessfulSegmentTimings(ctx context.Context, taskIDs []string) ([]taskattempts.SegmentTiming, error) {
	if r == nil || r.store == nil || r.store.db == nil {
		return nil, fmt.Errorf("successful segment timings: store is not initialized")
	}
	ids := make([]string, 0, len(taskIDs))
	seen := make(map[string]struct{}, len(taskIDs))
	for _, id := range taskIDs {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	placeholders := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
	args := make([]interface{}, len(ids))
	for i := range ids {
		args[i] = ids[i]
	}
	query := `
		WITH successful AS (
			SELECT a.id AS attempt_id, a.task_id,
			       ROW_NUMBER() OVER (PARTITION BY a.task_id ORDER BY a.attempt_number DESC, a.completed_at DESC, a.id DESC) AS rn
			FROM task_attempts a
			JOIN tasks t ON t.task_id = a.task_id
			WHERE a.status = 'SUCCEEDED'
			  AND t.executor_id = 'video.prepare.window.v1'
			  AND a.task_id IN (` + placeholders + `)
		)
		SELECT s.task_id, st.attempt_id, st.segment_index,
		       COALESCE(st.scene_id,''), COALESCE(st.source_type,''),
		       COALESCE(st.cache_key,''), COALESCE(st.source_url_hash,''),
		       COALESCE(st.codec,''), COALESCE(st.preset,''),
		       COALESCE(st.duration_ms,0), COALESCE(st.asset_download_ms,0),
		       COALESCE(st.ffmpeg_encode_ms,0), COALESCE(st.source_bytes,0),
		       COALESCE(st.output_bytes,0), COALESCE(st.input_duration_ms,0),
		       COALESCE(st.output_duration_ms,0), COALESCE(st.frames_encoded,0),
		       COALESCE(st.frames_decoded,0), COALESCE(st.frames_composited,0),
		       COALESCE(st.ffmpeg_speed_x,0), COALESCE(st.status,''),
		       COALESCE(st.metadata_json,'{}')
		FROM successful s
		JOIN task_attempt_segment_timings st ON st.attempt_id = s.attempt_id
		WHERE s.rn = 1
		ORDER BY s.task_id, st.segment_index`
	rows, err := r.store.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("successful segment timings query: %w", err)
	}
	defer rows.Close()
	var out []taskattempts.SegmentTiming
	for rows.Next() {
		var segment taskattempts.SegmentTiming
		if err := rows.Scan(&segment.TaskID, &segment.AttemptID, &segment.SegmentIndex,
			&segment.SceneID, &segment.SourceType, &segment.CacheKey, &segment.SourceURLHash,
			&segment.Codec, &segment.Preset, &segment.DurationMS, &segment.AssetDownloadMS,
			&segment.FfmpegEncodeMS, &segment.SourceBytes, &segment.OutputBytes,
			&segment.InputDurationMS, &segment.OutputDurationMS, &segment.FramesEncoded,
			&segment.FramesDecoded, &segment.FramesComposited, &segment.FfmpegSpeedX,
			&segment.Status, &segment.MetadataJSON); err != nil {
			return nil, fmt.Errorf("successful segment timings scan: %w", err)
		}
		out = append(out, segment)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("successful segment timings rows: %w", err)
	}
	return out, nil
}

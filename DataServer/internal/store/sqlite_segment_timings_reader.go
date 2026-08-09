package store

import (
	"context"
	"fmt"

	"velox-server/internal/taskattempts"
)

// ListSegmentTimings returns the sidecar segment telemetry for one attempt.
// It is a read-only projection used by the observability service; secrets
// and source URLs are intentionally not included in this operator surface.
func (r *SQLiteTaskAttemptRepository) ListSegmentTimings(ctx context.Context, attemptID string) ([]taskattempts.SegmentTiming, error) {
	if r == nil || r.store == nil || attemptID == "" {
		return nil, nil
	}
	rows, err := r.store.db.QueryContext(ctx, `
		SELECT attempt_id, segment_index, COALESCE(scene_id,''),
		       duration_ms, asset_download_ms, ffmpeg_encode_ms,
		       frames_encoded, COALESCE(frames_decoded,0),
		       COALESCE(frames_composited,0), COALESCE(ffmpeg_speed_x,0),
		       status
		FROM task_attempt_segment_timings
		WHERE attempt_id = ?
		ORDER BY segment_index ASC`, attemptID)
	if err != nil {
		return nil, fmt.Errorf("segment timings list: %w", err)
	}
	defer rows.Close()
	var out []taskattempts.SegmentTiming
	for rows.Next() {
		var segment taskattempts.SegmentTiming
		if err := rows.Scan(&segment.AttemptID, &segment.SegmentIndex, &segment.SceneID,
			&segment.DurationMS, &segment.AssetDownloadMS, &segment.FfmpegEncodeMS,
			&segment.FramesEncoded, &segment.FramesDecoded, &segment.FramesComposited,
			&segment.FfmpegSpeedX, &segment.Status); err != nil {
			return nil, fmt.Errorf("segment timings scan: %w", err)
		}
		out = append(out, segment)
	}
	return out, rows.Err()
}

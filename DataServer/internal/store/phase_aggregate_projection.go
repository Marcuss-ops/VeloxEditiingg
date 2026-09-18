package store

import (
	"context"
	"database/sql"
	"fmt"
)

// projectPhaseAggregates fills the typed attempt scorecard from the detailed
// phase ledger. The worker's typed values win when present; this is a
// compatibility projection for mixed-version workers and must stay inside
// the result-ingest transaction.
func projectPhaseAggregates(ctx context.Context, tx *sql.Tx, attemptID string) error {
	if attemptID == "" {
		return nil
	}
	_, err := tx.ExecContext(ctx, `
		UPDATE task_attempt_metrics
		SET pipeline_compile_ms = CASE WHEN pipeline_compile_ms > 0 THEN pipeline_compile_ms ELSE COALESCE((
			SELECT SUM(duration_ms) FROM task_phase_timings
			WHERE attempt_id = ? AND component = 'worker.plan' AND action = 'compile'
		), 0) END,
		pipeline_render_ms = CASE WHEN pipeline_render_ms > 0 THEN pipeline_render_ms ELSE COALESCE((
			SELECT SUM(duration_ms) FROM task_phase_timings
			WHERE attempt_id = ? AND component = 'engine' AND action = 'render'
		), 0) END,
		pipeline_total_ms = CASE WHEN pipeline_total_ms > 0 THEN pipeline_total_ms ELSE COALESCE((
			SELECT SUM(duration_ms) FROM task_phase_timings
			WHERE attempt_id = ? AND component = 'runner' AND action = 'execute'
		), 0) END,
		native_total_ms = CASE WHEN native_total_ms > 0 THEN native_total_ms ELSE COALESCE((
			SELECT SUM(duration_ms) FROM task_phase_timings
			WHERE attempt_id = ? AND component = 'engine' AND action = 'render'
		), 0) END,
		engine_asset_download_ms = CASE WHEN engine_asset_download_ms > 0 THEN engine_asset_download_ms ELSE COALESCE((
			SELECT SUM(duration_ms) FROM task_phase_timings
			WHERE attempt_id = ? AND component = 'worker.asset' AND action = 'transfer'
		), 0) END,
		-- engine.concat is the frame-pipeline concat; engine.mixed_packet_mux /
		-- engine.packet_mux are the copy-only packet mux that DOES the concat in
		-- the packet-copy path (catalog: engine.concat, engine.mixed_packet_mux,
		-- engine.packet_mux — all duration/exclusive/per_attempt). Without them
		-- the copy-only mux time (the dominant render cost) was invisible here.
		engine_concat_ms = CASE WHEN engine_concat_ms > 0 THEN engine_concat_ms ELSE COALESCE((
			SELECT SUM(duration_ms) FROM task_phase_timings
			WHERE attempt_id = ? AND component = 'engine'
			  AND action IN ('concat', 'mixed_packet_mux', 'packet_mux')
		), 0) END,
		engine_mux_audio_ms = CASE WHEN engine_mux_audio_ms > 0 THEN engine_mux_audio_ms ELSE COALESCE((
			SELECT SUM(duration_ms) FROM task_phase_timings
			WHERE attempt_id = ? AND component = 'engine.mux' AND action = 'audio'
		), 0) END
		WHERE attempt_id = ?`,
		attemptID, attemptID, attemptID, attemptID,
		attemptID, attemptID, attemptID, attemptID,
	)
	if err != nil {
		return fmt.Errorf("project phase aggregates: %w", err)
	}
	return nil
}

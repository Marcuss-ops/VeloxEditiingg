package store

import (
	"context"
	"database/sql"
	"fmt"

	"velox-server/internal/taskattempts"
)

// phaseSummary is the compact component/action aggregate retained in
// task_phase_timings. Repeated segment/track events are kept only in the
// append-only execution event table.
type phaseSummary struct {
	phaseOrder                 int
	component, action, phase   string
	status, errorCode, message string
	startedAt, completedAt     string
	durationMS                 int64
	bytesIn, bytesOut, frames  int64
	metadata                   string
}

func persistPhaseSummary(ctx context.Context, tx *sql.Tx, attemptID string, identity phaseTimingIdentity, timings []taskattempts.PhaseTimingDetailed) error {
	summaries := make(map[string]*phaseSummary)
	order := make([]string, 0, len(timings))
	for _, timing := range timings {
		if timing.Component == "" || timing.Action == "" {
			continue
		}
		key := timing.Component + "\x00" + timing.Action
		summary, ok := summaries[key]
		if !ok {
			summary = &phaseSummary{
				phaseOrder:  timing.PhaseOrder,
				component:   timing.Component,
				action:      timing.Action,
				phase:       phaseName(timing),
				status:      timing.Status,
				errorCode:   timing.ErrorCode,
				message:     timing.ErrorMessage,
				startedAt:   formatTimingTime(timing.StartedAt),
				completedAt: formatTimingTime(timing.CompletedAt),
				metadata:    timing.MetadataJSON,
			}
			summaries[key] = summary
			order = append(order, key)
		}
		summary.durationMS += timing.DurationMS
		summary.bytesIn += timing.BytesIn
		summary.bytesOut += timing.BytesOut
		summary.frames += timing.Frames
		if timing.PhaseOrder < summary.phaseOrder || summary.phaseOrder == 0 {
			summary.phaseOrder = timing.PhaseOrder
		}
		if summary.startedAt == "" || (!timing.StartedAt.IsZero() && formatTimingTime(timing.StartedAt) < summary.startedAt) {
			summary.startedAt = formatTimingTime(timing.StartedAt)
		}
		if timing.CompletedAt.After(parseTimingTime(summary.completedAt)) {
			summary.completedAt = formatTimingTime(timing.CompletedAt)
		}
		if timing.Status == "failed" || (summary.status == "" && timing.Status != "") {
			summary.status = timing.Status
			summary.errorCode = timing.ErrorCode
			summary.message = timing.ErrorMessage
		}
		if summary.metadata == "" {
			summary.metadata = timing.MetadataJSON
		}
	}

	for _, key := range order {
		summary := summaries[key]
		if summary.metadata == "" {
			summary.metadata = "{}"
		}
		if summary.status == "" {
			summary.status = "ok"
		}
		if summary.phase == "" {
			summary.phase = summary.component + "." + summary.action
		}
		_, err := tx.ExecContext(ctx, `
			INSERT INTO task_phase_timings (
				attempt_id, phase, duration_ms, wall_start, wall_end,
				phase_order, component, action, status, error_code, error_message,
				bytes_in, bytes_out, frames, metadata_json,
				job_id, task_id, worker_id, worker_snapshot_id,
				executor_id, executor_version
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(attempt_id, component, action) DO UPDATE SET
				phase = excluded.phase,
				duration_ms = excluded.duration_ms,
				wall_start = excluded.wall_start,
				wall_end = excluded.wall_end,
				phase_order = excluded.phase_order,
				status = excluded.status,
				error_code = excluded.error_code,
				error_message = excluded.error_message,
				bytes_in = excluded.bytes_in,
				bytes_out = excluded.bytes_out,
				frames = excluded.frames,
				metadata_json = excluded.metadata_json,
				job_id = excluded.job_id,
				task_id = excluded.task_id,
				worker_id = excluded.worker_id,
				worker_snapshot_id = excluded.worker_snapshot_id,
				executor_id = excluded.executor_id,
				executor_version = excluded.executor_version`,
			attemptID, summary.phase, summary.durationMS, summary.startedAt, summary.completedAt,
			summary.phaseOrder, summary.component, summary.action, summary.status,
			summary.errorCode, summary.message, summary.bytesIn, summary.bytesOut, summary.frames, summary.metadata, identity.JobID, identity.TaskID, identity.WorkerID,
			identity.WorkerSnapshotID, identity.ExecutorID, identity.ExecutorVersion,
		)
		if err != nil {
			return fmt.Errorf("task ingest atomic phase summary insert %s.%s: %w", summary.component, summary.action, err)
		}
	}
	return nil
}

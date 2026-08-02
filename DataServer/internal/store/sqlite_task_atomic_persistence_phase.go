package store

// sqlite_task_atomic_persistence_phase.go: partial-phase-metrics + raw
// report attempt write helpers used by IngestTaskResultAtomic. Split out
// of sqlite_task_atomic_persistence_attempt.go.

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"time"

	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

// persistPartialPhaseMetrics replaces partial phase metrics for FAILED attempts.
func persistPartialPhaseMetrics(ctx context.Context, tx *sql.Tx, cmd taskgraph.IngestResultCommand) error {
	if len(cmd.PartialPhaseMetrics) == 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM task_phase_timings WHERE attempt_id = ?`, cmd.AttemptID); err != nil {
		return fmt.Errorf("task ingest atomic partial phase timings delete: %w", err)
	}
	nowPhase := time.Now().UTC().Format(time.RFC3339)
	identity, err := resolvePhaseTimingIdentity(ctx, tx, cmd.AttemptID, cmd.TaskID, cmd.WorkerID, cmd.LeaseID)
	if err != nil {
		return fmt.Errorf("task ingest atomic partial phase timing identity: %w", err)
	}
	for _, pt := range cmd.PartialPhaseMetrics {
		startedAt := nowPhase
		completedAt := nowPhase
		if !pt.StartedAt.IsZero() {
			startedAt = pt.StartedAt.UTC().Format(time.RFC3339)
		}
		if !pt.CompletedAt.IsZero() {
			completedAt = pt.CompletedAt.UTC().Format(time.RFC3339)
		}
		phase := pt.Component + "." + pt.Action
		if phase == "." {
			phase = "unknown"
		}
		_, err := tx.ExecContext(ctx,
			`INSERT INTO task_phase_timings (
				attempt_id, phase, duration_ms, wall_start, wall_end,
				phase_order, component, action,
				status, error_code, error_message,
				bytes_in, bytes_out, frames, metadata_json,
				job_id, task_id, worker_id, worker_snapshot_id,
				executor_id, executor_version
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			cmd.AttemptID, phase, pt.DurationMS, startedAt, completedAt,
			pt.PhaseOrder, pt.Component, pt.Action,
			pt.Status, pt.ErrorCode, pt.ErrorMessage,
			pt.BytesIn, pt.BytesOut, pt.Frames, pt.MetadataJSON,
			identity.JobID, identity.TaskID, identity.WorkerID,
			identity.WorkerSnapshotID, identity.ExecutorID, identity.ExecutorVersion,
		)
		if err != nil {
			return fmt.Errorf("task ingest atomic partial phase timing insert %s: %w", phase, err)
		}
	}
	return nil
}

// persistRawReport persists the raw worker report payload for audit/replay,
// enforcing idempotency via report_hash.
func persistRawReport(ctx context.Context, tx *sql.Tx, cmd taskgraph.IngestResultCommand, now string) error {
	if cmd.RawReportJSON == "" {
		return nil
	}
	rawHash := cmd.ReportHash
	if rawHash == "" {
		rawHash = fmt.Sprintf("%x", sha256.Sum256([]byte(cmd.RawReportJSON)))
	}

	var existingHash string
	err := tx.QueryRowContext(ctx,
		`SELECT report_hash FROM task_attempt_reports WHERE attempt_id = ?`,
		cmd.AttemptID,
	).Scan(&existingHash)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("task ingest atomic raw report conflict check: %w", err)
	}
	if existingHash != "" && existingHash != rawHash {
		return fmt.Errorf("task ingest atomic raw report conflict: attempt_id=%s existing_hash=%s new_hash=%s: %w",
			cmd.AttemptID, existingHash, rawHash, taskattempts.ErrReportConflict)
	}

	if existingHash != "" {
		return nil
	}

	receivedAt := now
	if !cmd.RawReportReceivedAt.IsZero() {
		receivedAt = cmd.RawReportReceivedAt.UTC().Format(time.RFC3339)
	}

	reportSchema := cmd.ReportSchemaVersion
	if reportSchema <= 0 {
		reportSchema = 1
	}
	reportVersion := cmd.ReportVersion
	if reportVersion <= 0 {
		reportVersion = 1
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO task_attempt_reports
		 (attempt_id, report_schema, report_version, report_hash, raw_report_json, received_at, persisted_at)
		 VALUES (
			?, ?, ?, ?, ?, ?, ?
		)`,
		cmd.AttemptID, reportSchema, reportVersion, rawHash, cmd.RawReportJSON, receivedAt, now,
	)
	if err != nil {
		return fmt.Errorf("task ingest atomic raw report: %w", err)
	}
	return nil
}

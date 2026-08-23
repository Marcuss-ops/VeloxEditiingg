package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"

	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"

	sharedtelemetry "velox-shared/telemetry"
)

// persistPhaseTimingsAndExecutionEvents is the single atomic writer for the
// worker's detailed phase timeline. PhaseTimings is authoritative; the legacy
// PartialPhaseMetrics field is used only when the new field is absent.
func persistPhaseTimingsAndExecutionEvents(ctx context.Context, tx *sql.Tx, cmd taskgraph.IngestResultCommand) error {
	timings := append([]taskattempts.PhaseTimingDetailed(nil), cmd.PhaseTimings...)
	if len(timings) == 0 {
		timings = append(timings, cmd.PartialPhaseMetrics...)
	}
	timings = deduplicatePhaseTimings(timings)
	timings = normalizePhaseTimingWalls(timings)

	// A report hash is the idempotency boundary for the whole worker event
	// set. An identical terminal replay must not rewrite anything; a
	// different report is rejected before the authoritative replacement can
	// touch the existing rows. The caller's transaction then rolls back all
	// task, attempt, metric, and event writes together.
	replaceWorkerEvents, err := shouldReplaceWorkerExecutionEvents(ctx, tx, cmd)
	if err != nil {
		return err
	}
	if !replaceWorkerEvents {
		return nil
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO task_execution_event_replacement_authorizations
			(attempt_id, authorization, created_at)
		 VALUES (?, 'atomic_ingest', ?)`,
		cmd.AttemptID, nowRFC3339Nano(),
	); err != nil {
		return fmt.Errorf("task ingest atomic worker execution event authorization: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM task_execution_events
		 WHERE attempt_id = ? AND origin <> 'master'`,
		cmd.AttemptID,
	); err != nil {
		return fmt.Errorf("task ingest atomic worker execution event replacement: %w", err)
	}
	// Phase summaries are the compact projection of the same authoritative
	// worker report. Clear them before rebuilding so a replacement that omits
	// a previously reported phase cannot leave stale rows behind. This runs on
	// the coordinator-owned transaction, so a later validation or hash error
	// rolls the deletion back together with the event replacement.
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM task_phase_timings WHERE attempt_id = ?`,
		cmd.AttemptID,
	); err != nil {
		return fmt.Errorf("task ingest atomic phase summary replacement: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM task_attempt_phase_metrics WHERE attempt_id = ?`,
		cmd.AttemptID,
	); err != nil {
		return fmt.Errorf("task ingest atomic canonical phase metrics replacement: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM task_execution_event_replacement_authorizations WHERE attempt_id = ?`,
		cmd.AttemptID,
	); err != nil {
		return fmt.Errorf("task ingest atomic worker execution event authorization cleanup: %w", err)
	}
	if len(timings) == 0 {
		return nil
	}
	identity, err := resolvePhaseTimingIdentity(ctx, tx, cmd.AttemptID, cmd.TaskID, cmd.WorkerID, cmd.LeaseID)
	if err != nil {
		return fmt.Errorf("task ingest atomic phase identity: %w", err)
	}
	if err := persistPhaseSummary(ctx, tx, cmd.AttemptID, identity, timings); err != nil {
		return err
	}
	if err := persistCanonicalPhaseMetrics(ctx, tx, cmd.AttemptID, identity, timings); err != nil {
		return err
	}
	if err := persistExecutionEvents(ctx, tx, cmd, identity, timings); err != nil {
		return err
	}
	return nil
}

// shouldReplaceWorkerExecutionEvents enforces report-hash idempotency before
// the non-master event set is replaced. An empty raw report is a legacy
// caller without a hash boundary and retains the historical write behavior.
func shouldReplaceWorkerExecutionEvents(ctx context.Context, tx *sql.Tx, cmd taskgraph.IngestResultCommand) (bool, error) {
	if cmd.RawReportJSON == "" {
		return true, nil
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
	if err == sql.ErrNoRows {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("task ingest atomic execution event hash check: %w", err)
	}
	if existingHash != "" && existingHash != rawHash {
		return false, fmt.Errorf("task ingest atomic raw report conflict: attempt_id=%s existing_hash=%s new_hash=%s: %w",
			cmd.AttemptID, existingHash, rawHash, taskattempts.ErrReportConflict)
	}
	return existingHash == "", nil
}

func persistExecutionEvents(ctx context.Context, tx *sql.Tx, cmd taskgraph.IngestResultCommand, identity phaseTimingIdentity, timings []taskattempts.PhaseTimingDetailed) error {
	attemptID := cmd.AttemptID
	createdAt := nowRFC3339Nano()
	for _, timing := range timings {
		if phaseTimingIsLegacy(timing) {
			continue
		}
		if timing.Component == "" || timing.Action == "" {
			quarantineTelemetryEvent(ctx, tx, cmd, timing, fmt.Errorf("component and action are required"))
			continue
		}
		if cmd.TelemetrySchemaVersion != 0 && cmd.TelemetrySchemaVersion != sharedtelemetry.SchemaVersion {
			quarantineTelemetryEvent(ctx, tx, cmd, timing, fmt.Errorf("unsupported telemetry schema version %d", cmd.TelemetrySchemaVersion))
			continue
		}
		if err := validateExecutionEventTiming(timing); err != nil {
			// Telemetry is best-effort. Quarantine only the invalid event so
			// one producer taxonomy mistake cannot prevent the canonical
			// TaskResult/artifact transaction from completing.
			quarantineTelemetryEvent(ctx, tx, cmd, timing, err)
			continue
		}
		// SQLite enforces artifact_id for artifact-scoped events. A
		// producer may use a registered generic artifact phase before the
		// artifact declaration exists; keep the terminal ingest reliable
		// and retain the compact phase summary, while omitting only this
		// malformed timeline row.
		if timing.Scope == "artifact" && timing.ArtifactID == "" {
			quarantineTelemetryEvent(ctx, tx, cmd, timing, fmt.Errorf("artifact scope requires artifact_id"))
			continue
		}
		eventID := timing.EventID
		if eventID == "" {
			eventID = deterministicEventID(attemptID, timing)
		}
		var segmentIndex, trackIndex *int
		if timing.Scope == "segment" {
			value := timing.SegmentIndex
			segmentIndex = &value
		}
		if timing.Scope == "audio_track" || timing.Scope == "subtitle_track" {
			value := timing.TrackIndex
			trackIndex = &value
		}
		if err := (taskattempts.ExecutionEvent{
			EventID:      eventID,
			AttemptID:    attemptID,
			EventIndex:   timing.EventIndex,
			Origin:       taskattempts.ExecutionEventOrigin(timing.Origin),
			Scope:        taskattempts.ExecutionEventScope(timing.Scope),
			SegmentIndex: segmentIndex,
			TrackIndex:   trackIndex,
			ArtifactID:   timing.ArtifactID,
		}).Validate(); err != nil {
			quarantineTelemetryEvent(ctx, tx, cmd, timing, err)
			continue
		}
		startedAt := formatTimingTime(timing.StartedAt)
		completedAt := formatTimingTime(timing.CompletedAt)
		metadata := timing.MetadataJSON
		if metadata == "" {
			metadata = "{}"
		}
		_, err := tx.ExecContext(ctx, `
			INSERT INTO task_execution_events (
				event_id, attempt_id, job_id, task_id, worker_id,
				worker_session_id, worker_snapshot_id, lease_id,
				executor_id, executor_version, event_index, origin, scope,
				event_type, event_name, component, action, phase, status,
				error_code, error_message, started_at, completed_at, duration_ms,
				bytes_in, bytes_out, frames, metadata_json, created_at,
				telemetry_schema_version, segment_index, track_kind, track_index, artifact_id,
				started_offset_ms, finished_offset_ms, cpu_ms, queue_wait_ms,
				frames_in, frames_out				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
					?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

			ON CONFLICT(event_id) DO NOTHING`,
			eventID, attemptID, identity.JobID, identity.TaskID, identity.WorkerID,
			identity.WorkerSessionID, identity.WorkerSnapshotID, identity.LeaseID,
			identity.ExecutorID, identity.ExecutorVersion, timing.EventIndex, timing.Origin, timing.Scope,
			timing.EventType, timing.EventName, timing.Component, timing.Action, phaseName(timing),
			normalizeEventStatus(timing.Status), timing.ErrorCode, timing.ErrorMessage,
			startedAt, completedAt, timing.DurationMS, timing.BytesIn, timing.BytesOut, timing.Frames,
			metadata, createdAt, timing.TelemetrySchemaVersion, nullableSegmentIndex(timing), nullableEventString(timing.TrackKind),
			nullableTrackIndex(timing), timing.ArtifactID, timing.StartedOffsetMS, timing.FinishedOffsetMS,
			timing.CPUMS, timing.QueueWaitMS, timing.FramesIn, timing.FramesOut,
		)
		if err != nil {
			return fmt.Errorf("task ingest atomic execution event %s: %w", eventID, err)
		}
	}
	return nil
}

package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"

	sharedtelemetry "velox-shared/telemetry"
)

var telemetryQuarantinedEvents atomic.Uint64

// TelemetryQuarantineCount returns the process-local count of invalid
// telemetry events quarantined (or attempted for quarantine). It is
// deliberately low-cardinality and increments even when the optional
// quarantine table is unavailable, so observability never depends on the
// diagnostic write succeeding.
func TelemetryQuarantineCount() uint64 {
	return telemetryQuarantinedEvents.Load()
}

// persistPhaseTimingsAndExecutionEvents is the single atomic writer for the
// worker's detailed phase timeline. PhaseTimings is authoritative; the legacy
// PartialPhaseMetrics field is used only when the new field is absent.
func persistPhaseTimingsAndExecutionEvents(ctx context.Context, tx *sql.Tx, cmd taskgraph.IngestResultCommand) error {
	timings := append([]taskattempts.PhaseTimingDetailed(nil), cmd.PhaseTimings...)
	if len(timings) == 0 {
		timings = append(timings, cmd.PartialPhaseMetrics...)
	}
	timings = deduplicatePhaseTimings(timings)

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
		cmd.AttemptID, time.Now().UTC().Format(time.RFC3339Nano),
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

// phaseTimingIsLegacy reports whether a phase has no event taxonomy. Older
// workers populated only component/action summaries; those records remain
// valid for task_phase_timings but cannot safely be inserted into the new
// closed event timeline.
func phaseTimingIsLegacy(timing taskattempts.PhaseTimingDetailed) bool {
	return timing.Origin == "" && timing.Scope == ""
}

func deduplicatePhaseTimings(timings []taskattempts.PhaseTimingDetailed) []taskattempts.PhaseTimingDetailed {
	seen := make(map[string]struct{}, len(timings))
	out := make([]taskattempts.PhaseTimingDetailed, 0, len(timings))
	for _, timing := range timings {
		key := timing.EventID
		if key == "" && !phaseTimingIsLegacy(timing) {
			key = deterministicEventID(timing.AttemptID, timing)
		}
		if key == "" {
			// Legacy rows have no stable event identity. The summary table
			// has one row per attempt/component/action, so use that same
			// identity and keep the first legacy observation in a mixed
			// payload instead of duplicating it when only its duration changes.
			key = fmt.Sprintf("legacy|%s|%s", timing.Component, timing.Action)
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, timing)
	}
	return out
}

func validateExecutionEventTiming(timing taskattempts.PhaseTimingDetailed) error {
	if !isExecutionEventOrigin(timing.Origin) || !isExecutionEventScope(timing.Scope) {
		return fmt.Errorf("invalid origin/scope %q/%q", timing.Origin, timing.Scope)
	}
	return sharedtelemetry.Catalog.Validate(sharedtelemetry.TelemetryEventSpec{
		Origin: timing.Origin, Scope: timing.Scope, Component: timing.Component,
		Action: timing.Action, SchemaVersion: timing.TelemetrySchemaVersion,
	})
}

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

func persistExecutionEvents(ctx context.Context, tx *sql.Tx, cmd taskgraph.IngestResultCommand, identity phaseTimingIdentity, timings []taskattempts.PhaseTimingDetailed) error {
	attemptID := cmd.AttemptID
	createdAt := time.Now().UTC().Format(time.RFC3339Nano)
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

func quarantineTelemetryEvent(ctx context.Context, tx *sql.Tx, cmd taskgraph.IngestResultCommand, timing taskattempts.PhaseTimingDetailed, reason error) {
	telemetryQuarantinedEvents.Add(1)
	eventID := timing.EventID
	if eventID == "" {
		eventID = deterministicEventID(cmd.AttemptID, timing)
	}
	eventJSON := fmt.Sprintf(`{"origin":%q,"scope":%q,"component":%q,"action":%q,"event_index":%d,"phase":%q}`, timing.Origin, timing.Scope, timing.Component, timing.Action, timing.EventIndex, timing.Phase)
	_, err := tx.ExecContext(ctx, `
		INSERT INTO telemetry_event_quarantine (
			attempt_id, event_id, origin, scope, component, action,
			schema_version, reason, event_json, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(attempt_id, event_id) DO NOTHING`,
		cmd.AttemptID, eventID, timing.Origin, timing.Scope, timing.Component,
		timing.Action, timing.TelemetrySchemaVersion, reason.Error(), eventJSON,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		// Quarantine is strictly best-effort. In particular, a legacy test
		// schema or a DB that has not applied migration 130 must not turn a
		// telemetry taxonomy error into a failed TaskResult transaction.
		log.Printf("[TELEMETRY_QUARANTINE] attempt=%s event_id=%s component=%s action=%s origin=%s scope=%s err=%v quarantine_write=%v",
			cmd.AttemptID, eventID, timing.Component, timing.Action, timing.Origin, timing.Scope, reason, err)
		return
	}
	log.Printf("[TELEMETRY_QUARANTINE] attempt=%s event_id=%s component=%s action=%s origin=%s scope=%s err=%v",
		cmd.AttemptID, eventID, timing.Component, timing.Action, timing.Origin, timing.Scope, reason)
}

func deterministicEventID(attemptID string, timing taskattempts.PhaseTimingDetailed) string {
	payload := fmt.Sprintf("%s|%s|%s|%d|%s|%s|%d|%s|%d|%s", attemptID, timing.Origin, timing.Scope, timing.EventIndex, timing.Component, timing.Action, timing.SegmentIndex, timing.TrackKind, timing.TrackIndex, timing.ArtifactID)
	sum := sha256.Sum256([]byte(payload))
	return "phase-" + hex.EncodeToString(sum[:])
}

func phaseName(timing taskattempts.PhaseTimingDetailed) string {
	if timing.Phase != "" {
		return timing.Phase
	}
	return timing.Component + "." + timing.Action
}

func formatTimingTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func parseTimingTime(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	parsed, _ := time.Parse(time.RFC3339Nano, value)
	return parsed
}

func normalizeEventStatus(status string) string {
	if status == "" {
		return "ok"
	}
	return status
}

func nullableSegmentIndex(timing taskattempts.PhaseTimingDetailed) any {
	if timing.Scope == "segment" {
		return timing.SegmentIndex
	}
	return nil
}

func nullableTrackIndex(timing taskattempts.PhaseTimingDetailed) any {
	if timing.Scope == "audio_track" || timing.Scope == "subtitle_track" {
		return timing.TrackIndex
	}
	return nil
}

func nullableEventString(value string) any {
	if value == "" {
		return ""
	}
	return value
}

func isExecutionEventOrigin(value string) bool {
	return taskattempts.ExecutionEventOrigin(value).IsValid()
}

func isExecutionEventScope(value string) bool {
	return taskattempts.ExecutionEventScope(value).IsValid()
}

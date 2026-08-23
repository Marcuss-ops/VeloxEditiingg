package store

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync/atomic"

	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
	sharedtelemetry "velox-shared/telemetry"
)

var telemetryQuarantinedEvents atomic.Uint64

// TelemetryQuarantineCount returns the process-local count of invalid
// telemetry events quarantined (or attempted for quarantine).
func TelemetryQuarantineCount() uint64 {
	return telemetryQuarantinedEvents.Load()
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

func isExecutionEventOrigin(value string) bool {
	return taskattempts.ExecutionEventOrigin(value).IsValid()
}

func isExecutionEventScope(value string) bool {
	return taskattempts.ExecutionEventScope(value).IsValid()
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
		nowRFC3339Nano(),
	)
	if err != nil {
		// Quarantine is strictly best-effort. Legacy schemas without the
		// quarantine table must not fail the canonical TaskResult transaction.
		log.Printf("[TELEMETRY_QUARANTINE] attempt=%s event_id=%s component=%s action=%s origin=%s scope=%s err=%v quarantine_write=%v",
			cmd.AttemptID, eventID, timing.Component, timing.Action, timing.Origin, timing.Scope, reason, err)
		return
	}
	log.Printf("[TELEMETRY_QUARANTINE] attempt=%s event_id=%s component=%s action=%s origin=%s scope=%s err=%v",
		cmd.AttemptID, eventID, timing.Component, timing.Action, timing.Origin, timing.Scope, reason)
}

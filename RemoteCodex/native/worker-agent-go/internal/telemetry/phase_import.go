package telemetry

// phase_import.go owns the C++ sidecar import boundary.  It preserves the
// engine's origin/event_index, fills only catalog-authoritative defaults,
// and advances the local per-origin sequence before later Go events are
// recorded.  Re-importing the same (origin,event_index) is idempotent;
// invalid events are retained for master quarantine and returned as an error.

import (
	"fmt"

	sharedtelemetry "velox-shared/telemetry"
)

// ImportCXX is the official C++ sidecar import boundary. It preserves the
// engine's origin/event_index, fills only catalog-authoritative defaults, and
// advances the local per-origin sequence before later Go events are recorded.
// Re-importing the same (origin,event_index) is idempotent; invalid events are
// retained for master quarantine and returned as an error instead of vanishing.
func (r *EventRecorder) ImportCXX(events []RecordedPhase) error {
	if r == nil || len(events) == 0 {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.indexes == nil {
		r.indexes = make(map[string]int64)
	}
	if r.eventRecords == nil {
		r.eventRecords = make(map[eventIdentity]RecordedPhase)
	}
	var firstErr error
	for _, event := range events {
		normalized, err := normalizeImportedPhase(event)
		if err != nil && firstErr == nil {
			firstErr = err
		}
		identity := eventIdentity{origin: normalized.Origin, index: normalized.EventIndex}
		if existing, exists := r.eventRecords[identity]; exists {
			if existing != normalized && firstErr == nil {
				firstErr = fmt.Errorf("telemetry C++ import: conflicting duplicate %s/%d", identity.origin, identity.index)
			}
			continue
		}
		if len(r.events) >= MaxAttemptEvents {
			r.droppedEvents++
			// Preserve imported identity monotonicity even when the bounded
			// journal cannot retain this row.
			if normalized.EventIndex >= r.indexes[normalized.Origin] {
				r.indexes[normalized.Origin] = normalized.EventIndex + 1
			}
			continue
		}
		r.events = append(r.events, normalized)
		r.eventRecords[identity] = normalized
		if normalized.EventIndex >= r.indexes[normalized.Origin] {
			r.indexes[normalized.Origin] = normalized.EventIndex + 1
		}
	}
	return firstErr
}

func normalizeImportedPhase(event RecordedPhase) (RecordedPhase, error) {
	spec, ok := sharedtelemetry.Catalog.Lookup(event.Component, event.Action)
	if !ok {
		return event, fmt.Errorf("telemetry C++ import: unregistered event %q.%q", event.Component, event.Action)
	}
	if event.Origin != spec.Origin || event.Scope != spec.Scope {
		return event, fmt.Errorf("telemetry C++ import: origin/scope mismatch for %q: got %q/%q, want %q/%q", event.Component+"."+event.Action, event.Origin, event.Scope, spec.Origin, spec.Scope)
	}
	if event.SchemaVersion != 0 && event.SchemaVersion != SchemaVersion {
		return event, fmt.Errorf("telemetry C++ import: unsupported schema version %d", event.SchemaVersion)
	}
	if event.Phase != "" && event.Phase != spec.Phase {
		return event, fmt.Errorf("telemetry C++ import: phase mismatch for %q: got %q, want %q", event.Component+"."+event.Action, event.Phase, spec.Phase)
	}
	event.SchemaVersion = SchemaVersion
	event.Phase = spec.Phase
	if event.EventType == "" {
		event.EventType = eventTypeFor("", event.Status)
	}
	return event, nil
}

package telemetry

// status.go — closed status and event_type vocabularies.
//
// These two small vocabularies are shared by the Go worker recorder
// (worker-agent-go/internal/telemetry) and the C++ engine emitter
// (video-engine-cpp/src/telemetry/emitter.cpp):
//
//	status     — the completion outcome of a recorded event (ok | failed).
//	event_type — the lifecycle shape of a recorded event
//	             (started | completed | failed | progress).
//
// Both are declared in the language-neutral schema/catalog.json
// (schema.statuses / schema.event_types) and validated at catalog parse time
// (validateSchemaVocabularies), so a worker/engine drift can never silently
// fork either vocabulary. The literals live here ONCE; consumers reference
// these constants (or the generated C++ kStatus*/kEventType* bindings)
// instead of re-declaring them.

// ── Status: the completion outcome of a recorded event ─────────────────────
const (
	// StatusOK marks a successfully completed event.
	StatusOK = "ok"
	// StatusFailed marks an event that finished in failure.
	StatusFailed = "failed"
)

// IsCanonicalStatus reports whether s is a member of the closed status
// vocabulary. Empty and unknown values return false.
func IsCanonicalStatus(s string) bool {
	return s == StatusOK || s == StatusFailed
}

// ── EventType: the lifecycle shape of a recorded event ─────────────────────
const (
	// EventTypeCompleted marks a point-in-time event recording a successful
	// completion (the default derived from status=ok when unset).
	EventTypeCompleted = "completed"
	// EventTypeFailed marks a point-in-time event recording a failure (the
	// default derived from status=failed when unset).
	EventTypeFailed = "failed"
	// EventTypeStarted marks the beginning of a lifecycle edge.
	EventTypeStarted = "started"
	// EventTypeProgress marks an intermediate progress observation.
	EventTypeProgress = "progress"
)

// IsCanonicalEventType reports whether s is a member of the closed event_type
// vocabulary. Empty (derive-from-status) is handled by callers; unknown
// non-empty values return false.
func IsCanonicalEventType(s string) bool {
	switch s {
	case EventTypeCompleted, EventTypeFailed, EventTypeStarted, EventTypeProgress:
		return true
	}
	return false
}

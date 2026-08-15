package telemetry

// attempt_events.go — closed attempt lifecycle event-name vocabulary.
//
// These are the stable event_name values of the canonical attempt lifecycle
// projection. Both the worker (AttemptEventMachine) and the master
// (deterministic event-id ingestion in task_execution_events) stamp these
// exact wire values, so they are declared ONCE here and consumed by both
// modules — a worker/master drift can never silently fork the lifecycle
// vocabulary.
//
// They are NOT component.action catalog events (schema/catalog.json): each
// lifecycle edge is backed by a registered component.action pair, and the
// event_name is the compact lifecycle label carried alongside it. Keeping the
// labels in this closed Go vocabulary (rather than the language-neutral
// catalog) reflects that only the two Go modules consume them — the C++ engine
// never emits an attempt lifecycle event.
const (
	AttemptEventStarted               = "ATTEMPT_STARTED"
	AttemptEventPhaseChanged          = "PHASE_CHANGED"
	AttemptEventSegmentStarted        = "SEGMENT_STARTED"
	AttemptEventSegmentCompleted      = "SEGMENT_COMPLETED"
	AttemptEventProgressUpdated       = "PROGRESS_UPDATED"
	AttemptEventArtifactVerifyStarted = "ARTIFACT_VERIFY_STARTED"
	AttemptEventArtifactVerified      = "ARTIFACT_VERIFIED"
	AttemptEventDeliveryStarted       = "DELIVERY_STARTED"
	AttemptEventCompleted             = "ATTEMPT_COMPLETED"
)

// canonicalAttemptEventNames is the closed set backing IsCanonicalAttemptEvent.
var canonicalAttemptEventNames = map[string]struct{}{
	AttemptEventStarted: {}, AttemptEventPhaseChanged: {},
	AttemptEventSegmentStarted: {}, AttemptEventSegmentCompleted: {},
	AttemptEventProgressUpdated: {}, AttemptEventArtifactVerifyStarted: {},
	AttemptEventArtifactVerified: {}, AttemptEventDeliveryStarted: {},
	AttemptEventCompleted: {},
}

// IsCanonicalAttemptEvent reports whether name is one of the closed attempt
// lifecycle event names. Empty and unknown names return false.
func IsCanonicalAttemptEvent(name string) bool {
	_, ok := canonicalAttemptEventNames[name]
	return ok
}

// CanonicalAttemptEventNames returns a defensive copy of the closed event-name
// vocabulary in declaration order.
func CanonicalAttemptEventNames() []string {
	return []string{
		AttemptEventStarted, AttemptEventPhaseChanged,
		AttemptEventSegmentStarted, AttemptEventSegmentCompleted,
		AttemptEventProgressUpdated, AttemptEventArtifactVerifyStarted,
		AttemptEventArtifactVerified, AttemptEventDeliveryStarted,
		AttemptEventCompleted,
	}
}

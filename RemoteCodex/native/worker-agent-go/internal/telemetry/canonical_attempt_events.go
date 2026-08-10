package telemetry

import (
	"encoding/json"
	"fmt"
	"time"
)

// Canonical Attempt lifecycle events. They are emitted through the existing
// EventRecorder and therefore use the same ordered TaskResult/heartbeat path.
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

var canonicalAttemptEventNames = map[string]struct{}{
	AttemptEventStarted: {}, AttemptEventPhaseChanged: {},
	AttemptEventSegmentStarted: {}, AttemptEventSegmentCompleted: {},
	AttemptEventProgressUpdated: {}, AttemptEventArtifactVerifyStarted: {},
	AttemptEventArtifactVerified: {}, AttemptEventDeliveryStarted: {},
	AttemptEventCompleted: {},
}

// CanonicalAttemptEvent is the stable live heartbeat representation. The
// detailed counters remain in the existing progress projection; this compact
// list only exposes lifecycle edges and their monotonic recorder index.
type CanonicalAttemptEvent struct {
	EventID    string         `json:"event_id"`
	EventName  string         `json:"event_name"`
	EventIndex int64          `json:"event_index"`
	Phase      string         `json:"phase,omitempty"`
	Status     string         `json:"status,omitempty"`
	OccurredAt string         `json:"occurred_at,omitempty"`
	Metadata   map[string]any `json:"metadata,omitempty"`
}

func IsCanonicalAttemptEvent(name string) bool {
	_, ok := canonicalAttemptEventNames[name]
	return ok
}

// CanonicalAttemptEvents projects only lifecycle events from a recorder
// snapshot. It is intentionally pure so the heartbeat and tests share the
// same wire mapping. Event IDs are deterministic across TaskResult retries.
func CanonicalAttemptEvents(attemptID string, phases []RecordedPhase) []CanonicalAttemptEvent {
	if len(phases) == 0 || attemptID == "" {
		return nil
	}
	out := make([]CanonicalAttemptEvent, 0, len(phases))
	for _, phase := range phases {
		if !IsCanonicalAttemptEvent(phase.EventName) {
			continue
		}
		metadata := map[string]any{}
		if phase.MetadataJSON != "" {
			_ = json.Unmarshal([]byte(phase.MetadataJSON), &metadata)
		}
		out = append(out, CanonicalAttemptEvent{
			EventID:    deterministicAttemptEventID(attemptID, phase),
			EventName:  phase.EventName,
			EventIndex: phase.EventIndex,
			Phase:      phase.Phase,
			Status:     phase.Status,
			OccurredAt: phase.CompletedAt.UTC().Format(time.RFC3339Nano),
			Metadata:   metadata,
		})
	}
	return out
}

func deterministicAttemptEventID(attemptID string, phase RecordedPhase) string {
	return fmt.Sprintf("attempt-event-%s-%s-%d", attemptID, phase.Origin, phase.EventIndex)
}

package performance

// classify.go — shared catalog classification helpers used by BOTH the
// RunMetrics assembler path (classifyPhaseTiming / phaseName) and the
// AttemptSnapshot projection (classifyRecordedPhase / recordedPhaseName).
//
// One definition, two consumers: a divergence here (a lookup or a
// taxonomy fallback changing in only one path) would silently break
// accounted_ratio semantics between the two receipt projections. Never
// reimplement the lookup in a new assembly path.

import (
	"strings"

	sharedtelemetry "velox-shared/telemetry"
)

// classifyTimingRole resolves the canonical catalog timing role for an
// event identified by (component, action, scope, phase):
//
//  1. shared-catalog event lookup by component.action — authoritative
//     (e.g. engine.render → exclusive; engine.video.decode → span_child);
//  2. phase-taxonomy fallback for attempt-scoped events that predate a
//     catalog key: the event inherits the canonical phase's accounted role;
//  3. anything else returns "" — the row is quarantined from
//     accounted_ratio (fail-closed: an unclassifiable event is never
//     treated as exclusive).
func classifyTimingRole(component, action, scope, phase string) sharedtelemetry.TimingMode {
	if spec, ok := sharedtelemetry.Catalog.Lookup(component, action); ok {
		return spec.TimingMode
	}
	if scope == string(sharedtelemetry.ScopeAttempt) && phase != "" {
		if role, ok := sharedtelemetry.PhaseRoleOf(phase); ok {
			return role
		}
	}
	return ""
}

// descriptiveName picks the most descriptive canonical name for an event:
// the event name when present, else component.action, else the phase
// label, else the event type. Rows with no identifiable name are labeled
// "unknown" so accounting never sees an empty name.
func descriptiveName(eventName, component, action, phase, eventType string) string {
	if strings.TrimSpace(eventName) != "" {
		return eventName
	}
	if component != "" && action != "" {
		return component + "." + action
	}
	if phase != "" {
		return phase
	}
	if eventType != "" {
		return eventType
	}
	return "unknown"
}

package store

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"velox-server/internal/taskattempts"
	sharedtelemetry "velox-shared/telemetry"
)

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

// normalizePhaseTimingWalls prevents one malformed producer timestamp from
// inflating the attempt/task wall clock. Detailed phase durations remain
// useful, so only impossible wall bounds are discarded; the event itself is
// still persisted with its declared duration and taxonomy.
func normalizePhaseTimingWalls(timings []taskattempts.PhaseTimingDetailed) []taskattempts.PhaseTimingDetailed {
	for i := range timings {
		timing := &timings[i]
		if timing.StartedAt.IsZero() || timing.CompletedAt.IsZero() {
			continue
		}
		span := timing.CompletedAt.Sub(timing.StartedAt)
		declared := time.Duration(timing.DurationMS) * time.Millisecond
		// A phase can include a small scheduling/clock discrepancy. A span
		// greater than 30s and more than 10x its declared duration is not a
		// credible phase measurement (for example 1ms reported over 5m).
		if span < 0 || timing.DurationMS < 0 || (span > 30*time.Second && span > declared*10) {
			timing.StartedAt = time.Time{}
			timing.CompletedAt = time.Time{}
		}
	}
	return timings
}

func deterministicEventID(attemptID string, timing taskattempts.PhaseTimingDetailed) string {
	// Canonical Attempt lifecycle events are projected live from the same
	// recorder origin/index tuple. Reuse that identity at final ingest so a
	// heartbeat event and its TaskResult event converge under one idempotency
	// key across retries. Other detailed phases retain the historical hash
	// identity because their component/action/segment tuple is richer than
	// the compact lifecycle projection.
	if timing.EventName != "" && isCanonicalAttemptEventName(timing.EventName) {
		return fmt.Sprintf("attempt-event-%s-%s-%d", attemptID, timing.Origin, timing.EventIndex)
	}
	payload := fmt.Sprintf("%s|%s|%s|%d|%s|%s|%d|%s|%d|%s", attemptID, timing.Origin, timing.Scope, timing.EventIndex, timing.Component, timing.Action, timing.SegmentIndex, timing.TrackKind, timing.TrackIndex, timing.ArtifactID)
	sum := sha256.Sum256([]byte(payload))
	return "phase-" + hex.EncodeToString(sum[:])
}

func isCanonicalAttemptEventName(name string) bool {
	return sharedtelemetry.IsCanonicalAttemptEvent(name)
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

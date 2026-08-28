// phase_recorder.go — per-attempt canonical execution event recorder.
//
// EventRecorder is the single truth source for event recording.  Supporting
// vocabulary lives in:
//   - phase_types.go      — RecordedPhase, EventSpec, eventIdentity, constants
//   - event_handle.go     — EventHandle lifecycle wrapper
//   - phase_import.go     — C++ sidecar import boundary (ImportCXX)
package telemetry

import (
	"sync"
	"sync/atomic"
	"time"
)

// EventRecorder is the canonical append-only observation journal for one
// Attempt. Events are never removed by snapshots; the recorder is scoped to
// the attempt and becomes unreachable when that attempt completes. Event
// indexes remain monotonic per origin and imported C++ indexes are preserved.
type EventRecorder struct {
	mu               sync.Mutex
	startedAt        time.Time
	events           []RecordedPhase
	indexes          map[string]int64
	eventRecords     map[eventIdentity]RecordedPhase
	attemptTelemetry *AttemptTelemetrySession
	droppedEvents    int64
	invalidEvents    int64
}

func NewEventRecorder() *EventRecorder {
	return &EventRecorder{
		startedAt:    time.Now(),
		indexes:      make(map[string]int64),
		eventRecords: make(map[eventIdentity]RecordedPhase),
	}
}

func (r *EventRecorder) BindAttemptTelemetry(session *AttemptTelemetrySession) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.attemptTelemetry = session
	r.mu.Unlock()
}

func (r *EventRecorder) AttemptTelemetry() *AttemptTelemetrySession {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.attemptTelemetry
}

// Start begins one event using the canonical recorder API. The returned
// handle owns a monotonic start timestamp and records exactly once when
// completed. Begin remains as a compatibility alias for existing callers.
func (r *EventRecorder) Start(spec EventSpec) *EventHandle {
	if r == nil {
		return nil
	}
	if !normalizeEventSpec(&spec) {
		// Invalid events remain on the wire so the master can quarantine
		// them. They must never disappear silently at the producer boundary.
		r.markInvalidEvent()
	}
	now := time.Now()
	return &EventHandle{rec: r, spec: spec, startWall: now.UTC(), startMono: now}
}

// Begin is retained for compatibility with existing worker call sites.
func (r *EventRecorder) Begin(spec EventSpec) *EventHandle {
	return r.Start(spec)
}

func (r *EventRecorder) Emit(spec EventSpec, status, errCode, errMsg string) {
	if r == nil {
		return
	}
	if !normalizeEventSpec(&spec) {
		r.markInvalidEvent()
	}
	now := time.Now()
	eventType := eventTypeFor(spec.EventType, status)
	r.record(RecordedPhase{
		Origin: spec.Origin, Scope: spec.Scope, Component: spec.Component,
		Action: spec.Action, Phase: spec.Phase, EventType: eventType,
		EventName: spec.EventName, SchemaVersion: spec.SchemaVersion,
		ArtifactID: spec.ArtifactID, StartedAt: now.UTC(), CompletedAt: now.UTC(),
		Status: status, ErrorCode: errCode, ErrorMessage: errMsg,
		MetadataJSON: spec.MetadataJSON, SegmentIndex: spec.SegmentIndex,
		TrackKind: spec.TrackKind, TrackIndex: spec.TrackIndex,
		StartedOffsetMS: r.offsetMS(now), FinishedOffsetMS: r.offsetMS(now),
		CPUMS: spec.CPUMS, QueueWaitMS: spec.QueueWaitMS,
		FramesIn: spec.FramesIn, FramesOut: spec.FramesOut,
	})
}

func (r *EventRecorder) Record(spec EventSpec, startedAt, completedAt time.Time, durationMS int64, status, errCode, errMsg string) {
	if r == nil {
		return
	}
	if !normalizeEventSpec(&spec) {
		r.markInvalidEvent()
	}
	r.record(RecordedPhase{
		Origin: spec.Origin, Scope: spec.Scope, Component: spec.Component,
		Action: spec.Action, Phase: spec.Phase,
		EventType: eventTypeFor(spec.EventType, status), EventName: spec.EventName,
		SchemaVersion: spec.SchemaVersion, ArtifactID: spec.ArtifactID,
		StartedAt: startedAt.UTC(), CompletedAt: completedAt.UTC(), DurationMS: durationMS,
		Status: status, ErrorCode: errCode, ErrorMessage: errMsg,
		MetadataJSON: spec.MetadataJSON, SegmentIndex: spec.SegmentIndex,
		TrackKind: spec.TrackKind, TrackIndex: spec.TrackIndex,
		StartedOffsetMS: spec.StartedOffsetMS, FinishedOffsetMS: spec.FinishedOffsetMS,
		CPUMS: spec.CPUMS, QueueWaitMS: spec.QueueWaitMS,
		FramesIn: spec.FramesIn, FramesOut: spec.FramesOut,
	})
}

// Flush is a compatibility name for a non-destructive snapshot. The
// canonical Attempt journal is never drained by a projection.
func (r *EventRecorder) Flush() []RecordedPhase {
	return r.Snapshot()
}

// SnapshotFrom returns an independent copy of events recorded at or after
// offset. It is the official incremental projection API: callers retain the
// offset and the journal remains intact for later projections/retries.
func (r *EventRecorder) SnapshotFrom(offset int) []RecordedPhase {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if offset < 0 {
		offset = 0
	}
	if offset > len(r.events) {
		offset = len(r.events)
	}
	out := make([]RecordedPhase, len(r.events)-offset)
	copy(out, r.events[offset:])
	return out
}

// DrainFrom is retained as a source-compatible alias for older callers. It
// is deliberately non-destructive; new code must use SnapshotFrom to make
// the append-only contract explicit.
func (r *EventRecorder) DrainFrom(offset int) []RecordedPhase {
	return r.SnapshotFrom(offset)
}

// Snapshot returns an independent, non-destructive copy of all events
// recorded so far. Recording remains append-only; mutating the returned
// slice or its elements cannot mutate recorder state.
func (r *EventRecorder) Snapshot() []RecordedPhase {
	return r.SnapshotFrom(0)
}

// DroppedEventCount reports observations rejected after the bounded journal
// filled. It is monotonic for the lifetime of the attempt.
func (r *EventRecorder) DroppedEventCount() int64 {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.droppedEvents
}

// InvalidEventCount reports taxonomy/schema observations retained for
// quarantine. The recorder owns this raw fact; Prometheus receives it only
// through PrometheusSink when the AttemptSnapshot is published.
func (r *EventRecorder) InvalidEventCount() int64 {
	if r == nil {
		return 0
	}
	return atomic.LoadInt64(&r.invalidEvents)
}

func (r *EventRecorder) markInvalidEvent() {
	if r != nil {
		atomic.AddInt64(&r.invalidEvents, 1)
	}
}

func (r *EventRecorder) record(event RecordedPhase) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.events) >= MaxAttemptEvents {
		r.droppedEvents++
		return
	}
	if r.indexes == nil {
		r.indexes = make(map[string]int64)
	}
	if r.eventRecords == nil {
		r.eventRecords = make(map[eventIdentity]RecordedPhase)
	}
	event.EventIndex = r.indexes[event.Origin]
	r.indexes[event.Origin]++
	r.events = append(r.events, event)
	r.eventRecords[eventIdentity{origin: event.Origin, index: event.EventIndex}] = event
}

func (r *EventRecorder) offsetMS(stamp time.Time) float64 {
	if r == nil {
		return 0
	}
	return float64(stamp.Sub(r.startedAt).Microseconds()) / 1000
}

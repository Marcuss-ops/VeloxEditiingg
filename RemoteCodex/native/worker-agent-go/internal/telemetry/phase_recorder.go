// phase_recorder.go — observability chain / block 1: per-attempt event
// recorder.
//
// The recorder accumulates execution events for ONE attempt. The
// TaskRunner creates it at Run time, records every canonical phase
// through EventHandle.Begin/Complete, and drains it via Flush when the
// attempt finishes. The taskrunner boundary maps the drained
// telemetry.RecordedPhase values onto DetailedPhaseTiming (its report
// type), which the transport layer serializes into
// TaskResult.phase_timings for the master's task_execution_events table.
//
// Clock contract (mirrors canonical_phases.go):
//   - DurationMS is measured on a MONOTONIC clock (time.Since against
//     the recorder's start stamp) so wall-clock jumps never distort
//     phase durations.
//   - StartedAt / CompletedAt are UTC wall stamps, for cross-host
//     correlation only (the master does not compute durations from
//     them).
//
// Event index: each recorded event carries a per-origin event_index,
// incrementing from 0. The master's task_execution_events table guards
// UNIQUE(attempt_id, origin, event_index) so a replayed report is
// idempotent per origin.
//
// Thread-safety: EventRecorder is safe for concurrent use. The runner
// itself is single-goroutine per Run, but the C++ engine bridge may
// record from other goroutines in a later block.
package telemetry

import (
	"sync"
	"time"
)

// Status values recorded on events. These mirror the status vocabulary
// used by task_phase_timings so that summary + detail rows stay
// consistent for the same phase.
const (
	StatusOK     = "ok"
	StatusFailed = "failed"
)

// RecordedPhase is one immutable execution event drained from an
// EventRecorder. It is the telemetry-owned shape; the taskrunner maps
// it onto its DetailedPhaseTiming report type at the Run boundary.
type RecordedPhase struct {
	Origin           string
	Scope            string
	Component        string
	Action           string
	Phase            string
	EventType        string
	EventName        string
	EventIndex       int64
	StartedAt        time.Time
	CompletedAt      time.Time
	DurationMS       int64
	Status           string
	ErrorCode        string
	ErrorMessage     string
	BytesIn          int64
	BytesOut         int64
	Frames           int64
	MetadataJSON     string
	SegmentIndex     int32
	TrackKind        string
	TrackIndex       int32
	StartedOffsetMS  float64
	FinishedOffsetMS float64
	CPUMS            float64
	QueueWaitMS      float64
	FramesIn         int64
	FramesOut        int64
}

// EventSpec describes the event the caller is about to record. Origin,
// Scope, and Component/Action MUST be registered in phase_registry.go;
// non-canonical values are rejected before they enter the event stream.
type EventSpec struct {
	Origin           string
	Scope            string
	Component        string
	Action           string
	Phase            string
	EventType        string
	EventName        string
	MetadataJSON     string
	SegmentIndex     int32
	TrackKind        string
	TrackIndex       int32
	StartedOffsetMS  float64
	FinishedOffsetMS float64
	CPUMS            float64
	QueueWaitMS      float64
	FramesIn         int64
	FramesOut        int64
}

// EventRecorder accumulates RecordedPhase entries for one attempt.
type EventRecorder struct {
	mu        sync.Mutex
	startedAt time.Time // monotonic base for DurationMS
	events    []RecordedPhase
	indexes   map[string]int64 // origin → next event_index
}

// NewEventRecorder returns a recorder whose DurationMS values are
// measured against a fresh monotonic start stamp.
func NewEventRecorder() *EventRecorder {
	return &EventRecorder{
		startedAt: time.Now(),
		indexes:   make(map[string]int64),
	}
}

// Begin opens a new event for the given spec and returns a handle the
// caller completes (or aborts). A nil recorder returns a nil handle so
// callers can guard with `if h != nil`; Begin itself is safe to call on
// a nil receiver.
func (r *EventRecorder) Begin(spec EventSpec) *EventHandle {
	if r == nil {
		return nil
	}
	if !normalizeEventSpec(&spec) {
		return nil
	}
	return &EventHandle{
		rec:       r,
		spec:      spec,
		startWall: time.Now().UTC(),
		startMono: time.Now(),
	}
}

// Emit records a point-in-time event with no duration (started and
// completed stamps identical). Safe on a nil receiver.
func (r *EventRecorder) Emit(spec EventSpec, status, errCode, errMsg string) {
	if r == nil {
		return
	}
	if !normalizeEventSpec(&spec) {
		return
	}
	now := time.Now().UTC()
	r.record(RecordedPhase{
		Origin:           spec.Origin,
		Scope:            spec.Scope,
		Component:        spec.Component,
		Action:           spec.Action,
		Phase:            spec.Phase,
		EventType:        spec.EventType,
		EventName:        spec.EventName,
		StartedAt:        now,
		CompletedAt:      now,
		DurationMS:       0,
		Status:           status,
		ErrorCode:        errCode,
		ErrorMessage:     errMsg,
		MetadataJSON:     spec.MetadataJSON,
		SegmentIndex:     spec.SegmentIndex,
		TrackKind:        spec.TrackKind,
		TrackIndex:       spec.TrackIndex,
		StartedOffsetMS:  spec.StartedOffsetMS,
		FinishedOffsetMS: spec.FinishedOffsetMS,
		CPUMS:            spec.CPUMS,
		QueueWaitMS:      spec.QueueWaitMS,
		FramesIn:         spec.FramesIn,
		FramesOut:        spec.FramesOut,
	})
}

// Record appends a fully-formed event with explicit stamps. Use it when
// the caller already owns the timing (e.g. the runner reusing
// PhaseMarker times so summary + detail rows correlate exactly);
// Begin/Complete remains the API for long-running work. Safe on a nil
// receiver.
func (r *EventRecorder) Record(spec EventSpec, startedAt, completedAt time.Time, durationMS int64, status, errCode, errMsg string) {
	if r == nil {
		return
	}
	if !normalizeEventSpec(&spec) {
		return
	}
	eventType := spec.EventType
	if eventType == "" {
		if status == StatusFailed {
			eventType = "failed"
		} else {
			eventType = "completed"
		}
	}
	r.record(RecordedPhase{
		Origin:           spec.Origin,
		Scope:            spec.Scope,
		Component:        spec.Component,
		Action:           spec.Action,
		Phase:            spec.Phase,
		EventType:        eventType,
		EventName:        spec.EventName,
		StartedAt:        startedAt.UTC(),
		CompletedAt:      completedAt.UTC(),
		DurationMS:       durationMS,
		Status:           status,
		ErrorCode:        errCode,
		ErrorMessage:     errMsg,
		MetadataJSON:     spec.MetadataJSON,
		SegmentIndex:     spec.SegmentIndex,
		TrackKind:        spec.TrackKind,
		TrackIndex:       spec.TrackIndex,
		StartedOffsetMS:  spec.StartedOffsetMS,
		FinishedOffsetMS: spec.FinishedOffsetMS,
		CPUMS:            spec.CPUMS,
		QueueWaitMS:      spec.QueueWaitMS,
		FramesIn:         spec.FramesIn,
		FramesOut:        spec.FramesOut,
	})
}

// Flush drains a defensive copy of all recorded events in insertion
// order, and clears the accumulator so a subsequent drain returns only
// events recorded after the flush. Safe on a nil receiver.
func (r *EventRecorder) Flush() []RecordedPhase {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]RecordedPhase, len(r.events))
	copy(out, r.events)
	r.events = r.events[:0]
	for k := range r.indexes {
		delete(r.indexes, k)
	}
	return out
}

func (r *EventRecorder) record(p RecordedPhase) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p.EventIndex = r.indexes[p.Origin]
	r.indexes[p.Origin]++
	r.events = append(r.events, p)
}

// normalizeEventSpec applies the closed canonical taxonomy. Invalid
// origin/scope/component/action combinations are rejected, so the
// recorder never emits rows the master's CHECK constraints can reject.
func normalizeEventSpec(spec *EventSpec) bool {
	if spec == nil {
		return false
	}
	return CanonicalizeEventSpec(spec)
}

// EventHandle is an in-flight event returned by EventRecorder.Begin.
// It must be completed exactly once; a nil handle makes every method a
// no-op so caller-side guards are optional.
type EventHandle struct {
	rec       *EventRecorder
	spec      EventSpec
	startWall time.Time
	startMono time.Time
	done      bool
}

// Complete finalizes the event as success (status "ok"). Safe on a nil
// handle; a second Complete/Abort on the same handle is a no-op.
func (h *EventHandle) Complete() {
	h.complete(0, 0, 0, StatusOK, "", "")
}

// CompleteWith finalizes the event with counters and an explicit
// status. Safe on a nil handle.
func (h *EventHandle) CompleteWith(bytesIn, bytesOut, frames int64, status, errCode, errMsg string) {
	h.complete(bytesIn, bytesOut, frames, status, errCode, errMsg)
}

// Abort finalizes the event as failed. Safe on a nil handle.
func (h *EventHandle) Abort(errCode, errMsg string) {
	h.complete(0, 0, 0, StatusFailed, errCode, errMsg)
}

func (h *EventHandle) complete(bytesIn, bytesOut, frames int64, status, errCode, errMsg string) {
	if h == nil || h.rec == nil || h.done {
		return
	}
	h.done = true
	endMono := time.Now()
	eventType := h.spec.EventType
	if eventType == "" {
		if status == StatusFailed {
			eventType = "failed"
		} else {
			eventType = "completed"
		}
	}
	h.rec.record(RecordedPhase{
		Origin:           h.spec.Origin,
		Scope:            h.spec.Scope,
		Component:        h.spec.Component,
		Action:           h.spec.Action,
		Phase:            h.spec.Phase,
		EventType:        eventType,
		EventName:        h.spec.EventName,
		StartedAt:        h.startWall,
		CompletedAt:      endMono.UTC(),
		DurationMS:       endMono.Sub(h.startMono).Milliseconds(),
		Status:           status,
		ErrorCode:        errCode,
		ErrorMessage:     errMsg,
		BytesIn:          bytesIn,
		BytesOut:         bytesOut,
		Frames:           frames,
		MetadataJSON:     h.spec.MetadataJSON,
		SegmentIndex:     h.spec.SegmentIndex,
		TrackKind:        h.spec.TrackKind,
		TrackIndex:       h.spec.TrackIndex,
		StartedOffsetMS:  h.spec.StartedOffsetMS,
		FinishedOffsetMS: h.spec.FinishedOffsetMS,
		CPUMS:            h.spec.CPUMS,
		QueueWaitMS:      h.spec.QueueWaitMS,
		FramesIn:         h.spec.FramesIn,
		FramesOut:        h.spec.FramesOut,
	})
}

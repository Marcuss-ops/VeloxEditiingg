// phase_recorder.go — per-attempt canonical execution event recorder.
package telemetry

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	sharedtelemetry "velox-shared/telemetry"
)

const (
	StatusOK     = "ok"
	StatusFailed = "failed"

	// MaxAttemptEvents bounds the in-memory attempt journal. Once full,
	// new observations are dropped and counted on the final snapshot. This
	// explicit best-effort policy prevents pathological attempts from growing
	// memory without limit.
	MaxAttemptEvents = 4096
)

// RecordedPhase is one immutable execution event stored in an EventRecorder.
type RecordedPhase struct {
	Origin           string
	Scope            string
	Component        string
	Action           string
	Phase            string
	EventType        string
	EventName        string
	SchemaVersion    int32
	EventIndex       int64
	ArtifactID       string
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

// EventSpec describes an event. Origin, scope, and component/action should be
// registered in phase_registry.go. Invalid specs are retained and sent to the
// master for quarantine rather than being silently dropped at the worker.
type EventSpec struct {
	Origin           string
	Scope            string
	Component        string
	Action           string
	Phase            string
	EventType        string
	EventName        string
	SchemaVersion    int32
	ArtifactID       string
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

type eventIdentity struct {
	origin string
	index  int64
}

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

func eventTypeFor(explicit, status string) string {
	if explicit != "" {
		return explicit
	}
	if status == StatusFailed {
		return StatusFailed
	}
	return "completed"
}

func normalizeEventSpec(spec *EventSpec) bool {
	return spec != nil && CanonicalizeEventSpec(spec)
}

// EventHandle is safe to complete from multiple goroutines; exactly one
// completion is recorded. Counter and metadata updates are safe before the
// completion wins the lifecycle race. Handles created from invalid specs
// preserve the raw taxonomy so the master can quarantine the event.
type EventHandle struct {
	rec       *EventRecorder
	spec      EventSpec
	startWall time.Time
	startMono time.Time
	done      atomic.Bool

	mu        sync.Mutex
	bytesIn   int64
	bytesOut  int64
	frames    int64
	framesIn  int64
	framesOut int64
	metadata  string
}

func (h *EventHandle) Complete() {
	h.complete(0, 0, 0, StatusOK, "", "")
}

func (h *EventHandle) CompleteWith(bytesIn, bytesOut, frames int64, status, errCode, errMsg string) {
	h.complete(bytesIn, bytesOut, frames, status, errCode, errMsg)
}

func (h *EventHandle) Abort(errCode, errMsg string) {
	h.complete(0, 0, 0, StatusFailed, errCode, errMsg)
}

func (h *EventHandle) AddInputBytes(n int64) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.bytesIn += n
	h.mu.Unlock()
}

func (h *EventHandle) AddOutputBytes(n int64) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.bytesOut += n
	h.mu.Unlock()
}

func (h *EventHandle) AddFrames(n int64) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.frames += n
	h.mu.Unlock()
}

func (h *EventHandle) AddFramesIn(n int64) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.framesIn += n
	h.mu.Unlock()
}

func (h *EventHandle) AddFramesOut(n int64) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.framesOut += n
	h.mu.Unlock()
}

func (h *EventHandle) SetMetadataJSON(value string) {
	if h == nil {
		return
	}
	var object map[string]any
	if err := json.Unmarshal([]byte(value), &object); err != nil {
		return
	}
	encoded, err := json.Marshal(object)
	if err != nil {
		return
	}
	h.mu.Lock()
	h.metadata = string(encoded)
	h.mu.Unlock()
}

func (h *EventHandle) SetMetadata(key string, value any) {
	if h == nil || key == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	object := map[string]any{}
	if h.metadata != "" {
		_ = json.Unmarshal([]byte(h.metadata), &object)
	}
	object[key] = value
	if encoded, err := json.Marshal(object); err == nil {
		h.metadata = string(encoded)
	}
}

func (h *EventHandle) complete(bytesIn, bytesOut, frames int64, status, errCode, errMsg string) {
	if h == nil || h.rec == nil || !h.done.CompareAndSwap(false, true) {
		return
	}
	h.mu.Lock()
	bytesIn += h.bytesIn
	bytesOut += h.bytesOut
	frames += h.frames
	framesIn := h.framesIn
	framesOut := h.framesOut
	metadata := h.metadata
	h.mu.Unlock()

	endMono := time.Now()
	startedOffset := h.spec.StartedOffsetMS
	if startedOffset == 0 {
		startedOffset = h.rec.offsetMS(h.startMono)
	}
	finishedOffset := h.spec.FinishedOffsetMS
	if finishedOffset == 0 {
		finishedOffset = h.rec.offsetMS(endMono)
	}
	h.rec.record(RecordedPhase{
		Origin: h.spec.Origin, Scope: h.spec.Scope, Component: h.spec.Component,
		Action: h.spec.Action, Phase: h.spec.Phase,
		EventType: eventTypeFor(h.spec.EventType, status), EventName: h.spec.EventName,
		SchemaVersion: h.spec.SchemaVersion, ArtifactID: h.spec.ArtifactID,
		StartedAt: h.startWall, CompletedAt: endMono.UTC(),
		DurationMS: endMono.Sub(h.startMono).Milliseconds(), Status: status,
		ErrorCode: errCode, ErrorMessage: errMsg,
		BytesIn: bytesIn, BytesOut: bytesOut, Frames: frames,
		MetadataJSON: firstNonEmpty(metadata, h.spec.MetadataJSON),
		SegmentIndex: h.spec.SegmentIndex, TrackKind: h.spec.TrackKind,
		TrackIndex: h.spec.TrackIndex, StartedOffsetMS: startedOffset,
		FinishedOffsetMS: finishedOffset, CPUMS: h.spec.CPUMS,
		QueueWaitMS: h.spec.QueueWaitMS, FramesIn: framesIn + h.spec.FramesIn,
		FramesOut: framesOut + h.spec.FramesOut,
	})
}

func firstNonEmpty(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

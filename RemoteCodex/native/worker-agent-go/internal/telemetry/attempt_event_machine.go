package telemetry

import (
	"encoding/json"
	"sync"
	"time"
)

// AttemptEventMachine publishes lifecycle edges through the attempt's existing
// EventRecorder. It owns only event ordering/deduplication policy; progress
// values remain in the canonical JobProgress/active_jobs projection.
type AttemptEventMachine struct {
	recorder  *EventRecorder
	attemptID string

	mu                  sync.Mutex
	lastPhase           string
	lastSegment         int32
	hasSegment          bool
	lastSegmentComplete bool
	started             bool
	verifyStarted       bool
	verified            bool
	deliveryStarted     bool
	completed           bool
	lastProgressAt      time.Time
	lastProgressKey     string
}

func NewAttemptEventMachine(recorder *EventRecorder, attemptID string) *AttemptEventMachine {
	if recorder == nil || attemptID == "" {
		return nil
	}
	return &AttemptEventMachine{recorder: recorder, attemptID: attemptID}
}

func (m *AttemptEventMachine) AttemptStarted() {
	if m == nil {
		return
	}
	m.mu.Lock()
	if m.started {
		m.mu.Unlock()
		return
	}
	m.started = true
	m.mu.Unlock()
	m.emit(AttemptEventStarted, EventSpec{
		Origin: OriginWorker, Scope: ScopeAttempt,
		Component: "runner", Action: "execute", Phase: PhaseRender,
	}, StatusOK, nil)
}

func (m *AttemptEventMachine) PhaseChanged(phase string) {
	if m == nil || phase == "" {
		return
	}
	m.mu.Lock()
	if m.lastPhase == phase {
		m.mu.Unlock()
		return
	}
	m.lastPhase = phase
	m.mu.Unlock()
	m.emit(AttemptEventPhaseChanged, EventSpec{
		Origin: OriginWorker, Scope: ScopeAttempt,
		Component: "runner", Action: "execute", Phase: phase,
	}, StatusOK, map[string]any{"phase": phase})
}

func (m *AttemptEventMachine) SegmentStarted(segment int32, phase string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	if m.hasSegment && m.lastSegment == segment && !m.lastSegmentComplete {
		m.mu.Unlock()
		return
	}
	m.lastSegment = segment
	m.hasSegment = true
	m.lastSegmentComplete = false
	m.mu.Unlock()
	m.emit(AttemptEventSegmentStarted, EventSpec{
		Origin: OriginWorker, Scope: ScopeSegment,
		Component: "worker.parallel", Action: "segment_start", Phase: phase,
		SegmentIndex: segment,
	}, StatusOK, map[string]any{"segment": segment})
}

func (m *AttemptEventMachine) SegmentCompleted(segment int32, phase string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	if !m.hasSegment || m.lastSegment != segment || m.lastSegmentComplete {
		m.mu.Unlock()
		return
	}
	m.lastSegmentComplete = true
	m.mu.Unlock()
	m.emit(AttemptEventSegmentCompleted, EventSpec{
		Origin: OriginWorker, Scope: ScopeSegment,
		Component: "worker.parallel", Action: "segment_finish", Phase: phase,
		SegmentIndex: segment,
	}, StatusOK, map[string]any{"segment": segment})
}

// ProgressUpdated is sampled at most every two seconds, with phase and segment
// transitions bypassing the interval. Identical snapshots are always dropped.
func (m *AttemptEventMachine) ProgressUpdated(phase string, segment int32, percent int32, elapsedMS int64, framesEncoded int64, now time.Time) {
	if m == nil {
		return
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	key := progressKey(phase, segment, percent, elapsedMS, framesEncoded)
	m.mu.Lock()
	phaseChanged := phase != m.lastPhase
	segmentChanged := segment != m.lastSegment
	if key == m.lastProgressKey || (!phaseChanged && !segmentChanged && !m.lastProgressAt.IsZero() && now.Sub(m.lastProgressAt) < 2*time.Second) {
		m.mu.Unlock()
		return
	}
	m.lastProgressKey = key
	m.lastProgressAt = now
	m.lastPhase = phase
	m.lastSegment = segment
	m.hasSegment = true
	m.lastSegmentComplete = false
	m.mu.Unlock()

	// Progress is the engine-facing transition point. Emit lifecycle edges
	// here as well as from the worker callback so direct renderer users cannot
	// omit PHASE_CHANGED or SEGMENT_STARTED. The state is updated above, so
	// explicit callback-side edge calls become idempotent no-ops.
	if phaseChanged {
		m.emit(AttemptEventPhaseChanged, EventSpec{
			Origin: OriginWorker, Scope: ScopeAttempt,
			Component: "runner", Action: "execute", Phase: phase,
		}, StatusOK, map[string]any{"phase": phase})
	}
	if segmentChanged {
		m.emit(AttemptEventSegmentStarted, EventSpec{
			Origin: OriginWorker, Scope: ScopeSegment,
			Component: "worker.parallel", Action: "segment_start", Phase: phase,
			SegmentIndex: segment,
		}, StatusOK, map[string]any{"segment": segment})
	}
	m.emit(AttemptEventProgressUpdated, EventSpec{
		Origin: OriginFFmpeg, Scope: ScopeSegment,
		Component: "ffmpeg", Action: "progress", Phase: phase,
		SegmentIndex: segment,
	}, StatusOK, map[string]any{
		"percent": percent, "segment": segment, "elapsed_ms": elapsedMS,
		"frames_encoded": framesEncoded,
	})
}

func (m *AttemptEventMachine) ArtifactVerifyStarted() {
	if m == nil {
		return
	}
	m.mu.Lock()
	if m.verifyStarted {
		m.mu.Unlock()
		return
	}
	m.verifyStarted = true
	m.mu.Unlock()
	m.emit(AttemptEventArtifactVerifyStarted, EventSpec{
		Origin: OriginValidation, Scope: ScopeAttempt,
		Component: "quality", Action: "ffprobe", Phase: PhaseFinalize,
	}, StatusOK, nil)
}

func (m *AttemptEventMachine) ArtifactVerified(status string, err error) {
	if m == nil {
		return
	}
	m.mu.Lock()
	if m.verified {
		m.mu.Unlock()
		return
	}
	m.verified = true
	m.mu.Unlock()
	if status == "" {
		status = StatusOK
	}
	code, message := "", ""
	if err != nil {
		code, message = "artifact_verification_failed", err.Error()
		status = StatusFailed
	}
	m.emit(AttemptEventArtifactVerified, EventSpec{
		Origin: OriginValidation, Scope: ScopeAttempt,
		Component: "quality", Action: "sha256", Phase: PhaseFinalize,
	}, status, map[string]any{"verified": err == nil, "error": message, "error_code": code})
}

func (m *AttemptEventMachine) DeliveryStarted() {
	if m == nil {
		return
	}
	m.mu.Lock()
	if m.deliveryStarted {
		m.mu.Unlock()
		return
	}
	m.deliveryStarted = true
	m.mu.Unlock()
	m.emit(AttemptEventDeliveryStarted, EventSpec{
		Origin: OriginUpload, Scope: ScopeAttempt,
		Component: "worker", Action: "commit_ack_wait", Phase: PhaseUpload,
	}, StatusOK, nil)
}

func (m *AttemptEventMachine) AttemptCompleted(status string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	if m.completed {
		m.mu.Unlock()
		return
	}
	m.completed = true
	m.mu.Unlock()
	if status == "" {
		status = StatusOK
	}
	m.emit(AttemptEventCompleted, EventSpec{
		Origin: OriginWorker, Scope: ScopeAttempt,
		Component: "runner", Action: "report", Phase: PhaseFinalize,
	}, status, nil)
}

func (m *AttemptEventMachine) Snapshot() []CanonicalAttemptEvent {
	if m == nil || m.recorder == nil {
		return nil
	}
	return CanonicalAttemptEvents(m.attemptID, m.recorder.Snapshot())
}

func (m *AttemptEventMachine) emit(name string, spec EventSpec, status string, metadata map[string]any) {
	if m == nil || m.recorder == nil || !IsCanonicalAttemptEvent(name) {
		return
	}
	if metadata != nil {
		if encoded, err := json.Marshal(metadata); err == nil {
			spec.MetadataJSON = string(encoded)
		}
	}
	spec.EventName = name
	m.recorder.Emit(spec, status, "", "")
}

func progressKey(phase string, segment int32, percent int32, elapsedMS, framesEncoded int64) string {
	return string(rune(len(phase))) + phase + ":" + itoa32(segment) + ":" + itoa32(percent) + ":" + itoa64(elapsedMS) + ":" + itoa64(framesEncoded)
}

func itoa32(value int32) string { return itoa64(int64(value)) }
func itoa64(value int64) string {
	if value == 0 {
		return "0"
	}
	negative := value < 0
	if negative {
		value = -value
	}
	buf := [20]byte{}
	i := len(buf)
	for value > 0 {
		i--
		buf[i] = byte('0' + value%10)
		value /= 10
	}
	if negative {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

package telemetry

// event_handle.go owns EventHandle, the lifecycle wrapper for a single
// in-flight event.  Handles are created by EventRecorder.Start and complete
// exactly once, recording into the canonical append-only journal.

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"
)

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

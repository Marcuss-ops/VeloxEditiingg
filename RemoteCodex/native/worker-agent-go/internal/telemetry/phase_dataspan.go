package telemetry

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type DataSpan struct {
	timer   *JobPhaseTimer
	spanID  string
	phase   string
	sceneID string

	mu        sync.Mutex
	bytesIn   int64
	bytesOut  int64
	framesIn  int64
	framesOut int64
	cpuMs     float64
	queueMs   float64
}

func (t *JobPhaseTimer) BeginDataSpan(phase string) *DataSpan {
	if t == nil || !IsFineGrainedPhase(phase) {
		return nil
	}
	seq := atomic.AddUint64(&t.spanSeq, 1)
	spanID := fmt.Sprintf("span_%d_%d", time.Now().UnixNano(), seq)
	t.mu.Lock()
	t.activeSpans[spanID] = activeSpan{Phase: phase, Start: time.Now()}
	t.mu.Unlock()
	return &DataSpan{timer: t, spanID: spanID, phase: phase}
}

func (t *JobPhaseTimer) BeginSceneDataSpan(sceneID, phase string) *DataSpan {
	if t == nil || !IsFineGrainedPhase(phase) {
		return nil
	}
	t.mu.Lock()
	if _, exists := t.scenes[sceneID]; !exists {
		t.scenes[sceneID] = &ScenePhaseTiming{
			SceneID: sceneID,
			Phases:  make(map[string]PhaseTiming),
		}
	}
	t.mu.Unlock()
	seq := atomic.AddUint64(&t.spanSeq, 1)
	spanID := fmt.Sprintf("span_%d_%d", time.Now().UnixNano(), seq)
	t.mu.Lock()
	t.activeSpans[spanID] = activeSpan{Phase: phase, SceneID: sceneID, Start: time.Now()}
	t.mu.Unlock()
	return &DataSpan{timer: t, spanID: spanID, phase: phase, sceneID: sceneID}
}

// AddBytesIn adds input bytes to the span.
func (d *DataSpan) AddBytesIn(n int64) {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.bytesIn += n
	d.mu.Unlock()
}

// AddBytesOut adds output bytes to the span.
func (d *DataSpan) AddBytesOut(n int64) {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.bytesOut += n
	d.mu.Unlock()
}

// AddFramesIn adds input frames to the span.
func (d *DataSpan) AddFramesIn(n int64) {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.framesIn += n
	d.mu.Unlock()
}

// AddFramesOut adds output frames to the span.
func (d *DataSpan) AddFramesOut(n int64) {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.framesOut += n
	d.mu.Unlock()
}

// AddCPUMs adds CPU milliseconds to the span.
func (d *DataSpan) AddCPUMs(ms float64) {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.cpuMs += ms
	d.mu.Unlock()
}

// AddQueueWaitMs adds queue wait milliseconds to the span.
func (d *DataSpan) AddQueueWaitMs(ms float64) {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.queueMs += ms
	d.mu.Unlock()
}

// Complete ends the span and records all accumulated data.
func (d *DataSpan) Complete() {
	if d == nil || d.timer == nil || d.spanID == "" {
		return
	}
	d.timer.End(d.spanID)
	d.mu.Lock()
	bytesIn := d.bytesIn
	bytesOut := d.bytesOut
	framesIn := d.framesIn
	framesOut := d.framesOut
	cpuMs := d.cpuMs
	queueMs := d.queueMs
	d.mu.Unlock()

	if d.sceneID != "" {
		d.timer.AddScenePhaseData(d.sceneID, d.phase, bytesIn, bytesOut, framesIn, framesOut, cpuMs)
	} else {
		d.timer.AddPhaseData(d.phase, bytesIn, bytesOut, framesIn, framesOut, cpuMs, queueMs)
	}
}

package telemetry

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)
type activeSpan struct {
	Phase   string
	SceneID string
	Start   time.Time
}

type JobPhaseTimer struct {
	mu          sync.Mutex
	startedAt   time.Time
	phases      map[string]*PhaseTiming
	scenes      map[string]*ScenePhaseTiming
	activeSpans map[string]activeSpan
	spanSeq     uint64

	cacheMut      sync.Mutex
	cacheHitBytes int64
	cacheMissBytes int64
}

// NewJobPhaseTimer returns a ready-to-use timer with the default wall clock.
func NewJobPhaseTimer() *JobPhaseTimer {
	return &JobPhaseTimer{
		startedAt:   time.Now(),
		phases:      make(map[string]*PhaseTiming, len(FineGrainedPhaseOrder)),
		scenes:      make(map[string]*ScenePhaseTiming),
		activeSpans: make(map[string]activeSpan),
	}
}

// NewJobPhaseTimerWithClock allows injecting a fixed clock for tests.
func NewJobPhaseTimerWithClock(clock func() time.Time) *JobPhaseTimer {
	t := NewJobPhaseTimer()
	t.startedAt = clock()
	return t
}

// Begin starts timing a fine-grained phase. It returns a unique span key that
// must be passed to End. Unknown phases are silently ignored (noop span).
// Begin is thread-safe. Span IDs are opaque and never parsed for metadata.
func (t *JobPhaseTimer) Begin(phase string) string {
	if t == nil || !IsFineGrainedPhase(phase) {
		return ""
	}
	seq := atomic.AddUint64(&t.spanSeq, 1)
	spanID := fmt.Sprintf("span_%d_%d", time.Now().UnixNano(), seq)
	t.mu.Lock()
	t.activeSpans[spanID] = activeSpan{Phase: phase, Start: time.Now()}
	t.mu.Unlock()
	return spanID
}

// BeginScene starts a phase within a specific scene. The sceneID is used to
// attribute timing to the per-scene breakdown.
func (t *JobPhaseTimer) BeginScene(sceneID, phase string) string {
	if t == nil || !IsFineGrainedPhase(phase) {
		return ""
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
	return spanID
}

func (t *JobPhaseTimer) End(spanID string) {
	if t == nil || spanID == "" {
		return
	}
	t.mu.Lock()
	span, ok := t.activeSpans[spanID]
	if !ok {
		t.mu.Unlock()
		return
	}
	delete(t.activeSpans, spanID)
	t.mu.Unlock()

	duration := time.Since(span.Start)
	phase := span.Phase
	sceneID := span.SceneID
	if !IsFineGrainedPhase(phase) {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.phases[phase] == nil {
		t.phases[phase] = &PhaseTiming{}
	}
	t.phases[phase].Duration += duration
	t.phases[phase].Count++

	if sceneID != "" {
		if t.scenes[sceneID] == nil {
			t.scenes[sceneID] = &ScenePhaseTiming{
				SceneID: sceneID,
				Phases:  make(map[string]PhaseTiming),
			}
		}
		s := t.scenes[sceneID]
		pt := s.Phases[phase]
		pt.Duration += duration
		pt.Count++
		s.Phases[phase] = pt
	}
}

// AddPhaseData records data volumes, frames, and CPU time for a phase.
// Can be called independently of Begin/End or combined via DataSpan.
func (t *JobPhaseTimer) AddPhaseData(phase string, bytesIn, bytesOut, framesIn, framesOut int64, cpuMs, queueWaitMs float64) {
	if t == nil || !IsFineGrainedPhase(phase) {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.phases[phase] == nil {
		t.phases[phase] = &PhaseTiming{}
	}
	p := t.phases[phase]
	p.BytesIn += bytesIn
	p.BytesOut += bytesOut
	p.FramesIn += framesIn
	p.FramesOut += framesOut
	p.CPUMs += cpuMs
	p.QueueWaitMs += queueWaitMs
}

// AddSceneData records per-scene frame and byte metrics.
func (t *JobPhaseTimer) AddSceneData(sceneID string, sourceDurationMs, outputDurationMs int64, inputBytes, outputBytes int64, framesDecoded, framesEncoded int64, fps float64) {
	if t == nil || sceneID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.scenes[sceneID] == nil {
		t.scenes[sceneID] = &ScenePhaseTiming{
			SceneID: sceneID,
			Phases:  make(map[string]PhaseTiming),
		}
	}
	s := t.scenes[sceneID]
	s.SourceDurationMs = sourceDurationMs
	s.OutputDurationMs = outputDurationMs
	s.InputBytes = inputBytes
	s.OutputBytes = outputBytes
	s.FramesDecoded = framesDecoded
	s.FramesEncoded = framesEncoded
	s.FPS = fps
}

// AddScenePhaseData records per-scene phase data.
func (t *JobPhaseTimer) AddScenePhaseData(sceneID, phase string, bytesIn, bytesOut, framesIn, framesOut int64, cpuMs float64) {
	if t == nil || sceneID == "" || !IsFineGrainedPhase(phase) {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.scenes[sceneID] == nil {
		t.scenes[sceneID] = &ScenePhaseTiming{
			SceneID: sceneID,
			Phases:  make(map[string]PhaseTiming),
		}
	}
	s := t.scenes[sceneID]
	pt := s.Phases[phase]
	pt.BytesIn += bytesIn
	pt.BytesOut += bytesOut
	pt.FramesIn += framesIn
	pt.FramesOut += framesOut
	pt.CPUMs += cpuMs
	s.Phases[phase] = pt
}

// AddCacheHitBytes records bytes served from the local cache (cache hit).
// Thread-safe; may be called from download workers.
func (t *JobPhaseTimer) AddCacheHitBytes(n int64) {
	if t == nil {
		return
	}
	t.cacheMut.Lock()
	t.cacheHitBytes += n
	t.cacheMut.Unlock()
}

// AddCacheMissBytes records bytes downloaded from remote (cache miss).
// Thread-safe; may be called from download workers.
func (t *JobPhaseTimer) AddCacheMissBytes(n int64) {
	if t == nil {
		return
	}
	t.cacheMut.Lock()
	t.cacheMissBytes += n
	t.cacheMut.Unlock()
}

// CacheBytes returns the accumulated cache hit and miss bytes.
func (t *JobPhaseTimer) CacheBytes() (hitBytes, missBytes int64) {
	if t == nil {
		return 0, 0
	}
	t.cacheMut.Lock()
	hitBytes = t.cacheHitBytes
	missBytes = t.cacheMissBytes
	t.cacheMut.Unlock()
	return
}

// PhaseTimings returns a defensive copy of all phase timings, ordered by
// FineGrainedPhaseOrder. Phases with no data are included with zero values.
func (t *JobPhaseTimer) PhaseTimings() []PhaseTimingWithName {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.phaseTimingsLocked()
}

func (t *JobPhaseTimer) phaseTimingsLocked() []PhaseTimingWithName {
	out := make([]PhaseTimingWithName, 0, len(FineGrainedPhaseOrder))
	for _, name := range FineGrainedPhaseOrder {
		pt := PhaseTiming{}
		if t != nil && t.phases[name] != nil {
			pt = *t.phases[name]
		}
		out = append(out, PhaseTimingWithName{Name: name, Timing: pt})
	}
	return out
}

// SceneTimings returns scene timings sorted by descending total duration
// (slowest first). Useful for TOP SLOWEST SCENES reporting.
func (t *JobPhaseTimer) SceneTimings() []SceneTimingWithName {
	t.mu.Lock()
	defer t.mu.Unlock()

	out := make([]SceneTimingWithName, 0, len(t.scenes))
	for id, s := range t.scenes {
		out = append(out, SceneTimingWithName{SceneID: id, Timing: *s})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Timing.TotalMs() > out[j].Timing.TotalMs()
	})
	return out
}

// TotalDuration returns the sum of all phase durations. Note that this may
// exceed wall clock when phases overlap (e.g. parallel execution).
func (t *JobPhaseTimer) TotalDuration() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	var total time.Duration
	for _, p := range t.phases {
		total += p.Duration
	}
	return total
}

// StartedAt returns when the timer was created (approximate job start).
func (t *JobPhaseTimer) StartedAt() time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.startedAt
}

// DataSpan provides a context-like interface for recording data alongside
// timing. Call Begin to start, then Add* methods accumulate counters, and
// Complete records the end time and flushes all data.

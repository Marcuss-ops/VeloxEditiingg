package jobperf

import (
	"sync"
	"time"
)

// Canonical counter keys for the GPU↔CPU transfer accounting. The
// optimization target for the ideal CUDA pipeline is
// FramesDownloadedFromGPU == 0.
const (
	CounterFramesDownloadedFromGPU = "frames_downloaded_from_gpu"
	CounterFramesUploadedToGPU     = "frames_uploaded_to_gpu"
	CounterGpuToCpuBytes           = "gpu_to_cpu_bytes"
	CounterCpuToGpuBytes           = "cpu_to_gpu_bytes"
)

// Tracker accumulates wall-clock phase durations and raw counters for a
// single job. Begin/End may be called repeatedly for the same key —
// durations accumulate (sub-phases that run once per scene). All
// methods are safe for concurrent use; the engine runs synchronously
// today but the sampler and future parallel segment workers justify the
// mutex.
type Tracker struct {
	mu        sync.Mutex
	starts    map[string]time.Time
	durations map[string]float64 // accumulated milliseconds
	counters  map[string]int64
	scenes    []SceneMetrics
	startedAt time.Time
	stoppedAt time.Time
	clock     func() time.Time
}

// NewTracker returns a zero-value-usable Tracker with the wall clock.
func NewTracker() *Tracker {
	now := time.Now()
	return &Tracker{
		starts:    make(map[string]time.Time),
		durations: make(map[string]float64),
		counters:  make(map[string]int64),
		startedAt: now,
		clock:     time.Now,
	}
}

// NewTrackerWithClock injects a clock for tests.
func NewTrackerWithClock(clock func() time.Time) *Tracker {
	t := NewTracker()
	t.clock = clock
	t.startedAt = clock()
	return t
}

// Begin stamps the start of one phase invocation. Re-Begin without End
// re-stamps (last write wins), matching telemetry.PhaseTimer semantics.
func (t *Tracker) Begin(phase string) {
	if t == nil || t.clock == nil {
		return
	}
	t.mu.Lock()
	t.starts[phase] = t.clock()
	t.mu.Unlock()
}

// End closes the most recent Begin(phase) and ACCUMULATES the elapsed
// duration. End without a pending Begin records nothing.
func (t *Tracker) End(phase string) {
	if t == nil || t.clock == nil {
		return
	}
	end := t.clock()
	t.mu.Lock()
	if start, ok := t.starts[phase]; ok {
		t.durations[phase] += float64(end.Sub(start).Microseconds()) / 1000
		delete(t.starts, phase)
	}
	t.mu.Unlock()
}

// Measure times fn under the given phase key. Panic-safe: the phase is
// closed before the panic propagates.
func (t *Tracker) Measure(phase string, fn func()) {
	t.Begin(phase)
	defer t.End(phase)
	fn()
}

// AddCounter increments a named raw counter (frames, bytes, execs).
func (t *Tracker) AddCounter(name string, delta int64) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.counters[name] += delta
	t.mu.Unlock()
}

// SetCounter overwrites a named counter with an observed fact (e.g. a
// sidecar total).
func (t *Tracker) SetCounter(name string, value int64) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.counters[name] = value
	t.mu.Unlock()
}

// Counter returns the current value of a counter (0 when absent).
func (t *Tracker) Counter(name string) int64 {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.counters[name]
}

// AddScene appends one per-scene breakdown row.
func (t *Tracker) AddScene(scene SceneMetrics) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.scenes = append(t.scenes, scene)
	t.mu.Unlock()
}

// Stop seals the tracker's job_total window and returns it in ms.
func (t *Tracker) Stop() float64 {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stoppedAt = t.clock()
	total := float64(t.stoppedAt.Sub(t.startedAt).Microseconds()) / 1000
	t.durations[PhaseJobTotal] = total
	return total
}

// PhaseMS returns a defensive copy of accumulated durations keyed by
// phase. Zero-duration phases are omitted.
func (t *Tracker) PhaseMS() map[string]float64 {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string]float64, len(t.durations))
	for k, v := range t.durations {
		out[k] = v
	}
	return out
}

// Scenes returns a defensive copy of the recorded per-scene rows.
func (t *Tracker) Scenes() []SceneMetrics {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]SceneMetrics(nil), t.scenes...)
}

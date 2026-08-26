package telemetry

import (
	"sync"
	"time"

	sharedtelemetry "velox-shared/telemetry"
)

type AttemptMilestoneRecorder struct {
	mu        sync.RWMutex
	startedAt time.Time
	sequence  uint64
	samples   []sharedtelemetry.AttemptMilestoneSample
	index     map[sharedtelemetry.AttemptMilestone]int
}

func NewAttemptMilestoneRecorder() *AttemptMilestoneRecorder {
	return &AttemptMilestoneRecorder{
		startedAt: time.Now(),
		index:     make(map[sharedtelemetry.AttemptMilestone]int),
	}
}

func NewAttemptMilestoneRecorderAt(start time.Time) *AttemptMilestoneRecorder {
	if start.IsZero() {
		start = time.Now()
	}
	return &AttemptMilestoneRecorder{
		startedAt: start,
		index:     make(map[sharedtelemetry.AttemptMilestone]int),
	}
}

func (r *AttemptMilestoneRecorder) Mark(name sharedtelemetry.AttemptMilestone) {
	if r == nil || name == "" {
		return
	}
	if !sharedtelemetry.IsCanonicalAttemptMilestone(name) {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.index[name]; exists {
		return
	}
	now := time.Now()
	elapsed := now.Sub(r.startedAt).Milliseconds()
	if elapsed < 0 {
		elapsed = 0
	}
	r.sequence++
	sample := sharedtelemetry.AttemptMilestoneSample{
		Name:       name,
		Sequence:   r.sequence,
		ElapsedMS:  elapsed,
		OccurredAt: now.UTC().Format(time.RFC3339Nano),
	}
	r.index[name] = len(r.samples)
	r.samples = append(r.samples, sample)
}

func (r *AttemptMilestoneRecorder) Snapshot() []sharedtelemetry.AttemptMilestoneSample {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]sharedtelemetry.AttemptMilestoneSample, len(r.samples))
	copy(out, r.samples)
	return out
}

func (r *AttemptMilestoneRecorder) Has(name sharedtelemetry.AttemptMilestone) bool {
	if r == nil {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.index[name]
	return ok
}

func (r *AttemptMilestoneRecorder) ElapsedMS(name sharedtelemetry.AttemptMilestone) (int64, bool) {
	if r == nil {
		return 0, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	idx, ok := r.index[name]
	if !ok {
		return 0, false
	}
	return r.samples[idx].ElapsedMS, true
}

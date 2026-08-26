package taskrunner

import (
	"context"
	"sync"
	"time"
)

// WaterfallStage is a serial, non-overlapping interval in the worker attempt.
// It is deliberately separate from phase timings, whose spans may overlap.
type WaterfallStage struct {
	Name        string    `json:"name"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
	DurationMS  int64     `json:"duration_ms"`
	Status      string    `json:"status,omitempty"`
}

type waterfallKey struct{}

type WaterfallRecorder struct {
	mu      sync.Mutex
	current *WaterfallStage
	stages  []WaterfallStage
}

func NewWaterfallRecorder(start time.Time) *WaterfallRecorder {
	return &WaterfallRecorder{}
}

func WithWaterfallRecorder(ctx context.Context, recorder *WaterfallRecorder) context.Context {
	return context.WithValue(ctx, waterfallKey{}, recorder)
}

func WaterfallRecorderFromContext(ctx context.Context) *WaterfallRecorder {
	if ctx == nil {
		return nil
	}
	recorder, _ := ctx.Value(waterfallKey{}).(*WaterfallRecorder)
	return recorder
}

// Transition closes the previous stage and starts the next one. Gaps are
// intentional: they are measured as unclassified time by the Master.
func (r *WaterfallRecorder) Transition(name string, at time.Time) {
	if r == nil || name == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.current != nil {
		r.current.CompletedAt = at
		r.current.DurationMS = nonNegativeMillis(r.current.StartedAt, at)
		r.current.Status = "ok"
		r.stages = append(r.stages, *r.current)
	}
	r.current = &WaterfallStage{Name: name, StartedAt: at}
}

func (r *WaterfallRecorder) Finish(at time.Time, status string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.current == nil {
		return
	}
	r.current.CompletedAt = at
	r.current.DurationMS = nonNegativeMillis(r.current.StartedAt, at)
	if status != "" {
		r.current.Status = status
	} else {
		r.current.Status = "ok"
	}
	r.stages = append(r.stages, *r.current)
	r.current = nil
}

func (r *WaterfallRecorder) Snapshot() []WaterfallStage {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]WaterfallStage(nil), r.stages...)
}

func nonNegativeMillis(start, end time.Time) int64 {
	if end.Before(start) {
		return 0
	}
	return end.Sub(start).Milliseconds()
}

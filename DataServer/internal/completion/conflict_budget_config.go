package completion

import "time"

// WithMetricsSink installs or replaces the optional metrics sink.
func (b *ConflictBudget) WithMetricsSink(sink ConflictBudgetSink) *ConflictBudget {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sink = sink
	return b
}

// NewConflictBudget constructs a budget with canonical defaults for omitted values.
func NewConflictBudget(p ConflictBudgetPolicy) *ConflictBudget {
	if p.ConsecutiveConflictThreshold <= 0 {
		p.ConsecutiveConflictThreshold = 3
	}
	if p.ResetWindow <= 0 {
		p.ResetWindow = 5 * time.Minute
	}
	return &ConflictBudget{
		Policy:  p,
		nowFn:   time.Now,
		streaks: make(map[string]*streakState),
	}
}

// WithClock replaces the wall-clock source, primarily for deterministic tests.
func (b *ConflictBudget) WithClock(nowFn func() time.Time) *ConflictBudget {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nowFn = nowFn
	return b
}

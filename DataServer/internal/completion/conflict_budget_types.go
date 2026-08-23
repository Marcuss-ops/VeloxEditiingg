package completion

import (
	"errors"
	"sync"
	"time"
)

// ErrConflictBudgetExhausted signals that a conflict streak crossed its threshold.
var ErrConflictBudgetExhausted = errors.New("completion: conflict budget exhausted")

// ConflictBudgetPolicy governs ConflictBudget escalation.
type ConflictBudgetPolicy struct {
	ConsecutiveConflictThreshold int
	ResetWindow                  time.Duration
}

func DefaultConflictBudgetPolicy() ConflictBudgetPolicy {
	return ConflictBudgetPolicy{
		ConsecutiveConflictThreshold: 3,
		ResetWindow:                  5 * time.Minute,
	}
}

// ConflictBudget counts per-key consecutive ErrTransitionConflict results.
type ConflictBudget struct {
	Policy ConflictBudgetPolicy

	mu      sync.Mutex
	streaks map[string]*streakState
	nowFn   func() time.Time
	sink    ConflictBudgetSink
}

type streakState struct {
	consecutive int
	firstErrAt  time.Time
	lastErrAt   time.Time
}

// ConflictBudgetSink is the optional metrics contract for budget transitions.
type ConflictBudgetSink interface {
	ResetConflictBudget()
	ObserveConflictStreakUnderThreshold(streak int)
	EscalateConflictBudget(streak int)
}

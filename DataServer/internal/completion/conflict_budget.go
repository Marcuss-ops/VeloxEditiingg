package completion

import (
	"errors"
	"fmt"
	"time"
)

// Record registers a Coordinator-method CAS outcome for a specific key.
// Nil resets the key, non-conflict errors pass through, and transition
// conflicts increment the key's streak until the configured threshold.
func (b *ConflictBudget) Record(key string, err error) error {
	if err == nil {
		b.resetKey(key)
		return nil
	}
	if !errors.Is(err, ErrTransitionConflict) {
		return err
	}

	b.mu.Lock()
	now := b.nowFn()
	state := b.streaks[key]
	if state == nil || (b.Policy.ResetWindow > 0 && now.Sub(state.firstErrAt) > b.Policy.ResetWindow) {
		state = &streakState{consecutive: 1, firstErrAt: now, lastErrAt: now}
		b.streaks[key] = state
	} else {
		state.consecutive++
		state.lastErrAt = now
	}
	escalated := state.consecutive >= b.Policy.ConsecutiveConflictThreshold
	streakSnapshot := state.consecutive
	firstErrAt := state.firstErrAt
	lastErrAt := state.lastErrAt
	sink := b.sink
	if escalated {
		delete(b.streaks, key)
	}
	b.mu.Unlock()

	wrapErr := func() error {
		return fmt.Errorf("%w: consecutive=%d (since=%s last=%s) key=%s original=%v",
			ErrConflictBudgetExhausted, streakSnapshot,
			firstErrAt.Format(time.RFC3339Nano),
			lastErrAt.Format(time.RFC3339Nano),
			key,
			err)
	}
	if sink == nil {
		if escalated {
			return wrapErr()
		}
		return nil
	}
	if escalated {
		sink.EscalateConflictBudget(streakSnapshot)
		return wrapErr()
	}
	sink.ObserveConflictStreakUnderThreshold(streakSnapshot)
	return nil
}

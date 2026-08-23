package completion

// resetKey clears the streak for one key and emits a real reset notification.
func (b *ConflictBudget) resetKey(key string) {
	b.mu.Lock()
	state, ok := b.streaks[key]
	if !ok {
		b.mu.Unlock()
		return
	}
	wasStreak := state.consecutive > 0
	delete(b.streaks, key)
	sink := b.sink
	b.mu.Unlock()

	if wasStreak && sink != nil {
		sink.ResetConflictBudget()
	}
}

// Reset clears every key's streak and emits at most one reset notification.
func (b *ConflictBudget) Reset() {
	b.mu.Lock()
	wasStreak := false
	for _, state := range b.streaks {
		if state.consecutive > 0 {
			wasStreak = true
			break
		}
	}
	b.streaks = make(map[string]*streakState)
	sink := b.sink
	b.mu.Unlock()

	if wasStreak && sink != nil {
		sink.ResetConflictBudget()
	}
}

// Consecutive returns the maximum active streak across keys.
func (b *ConflictBudget) Consecutive() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	max := 0
	for _, state := range b.streaks {
		if state.consecutive > max {
			max = state.consecutive
		}
	}
	return max
}

// consecutiveForKey returns the active streak for one key, or zero when absent.
func (b *ConflictBudget) consecutiveForKey(key string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if state, ok := b.streaks[key]; ok {
		return state.consecutive
	}
	return 0
}

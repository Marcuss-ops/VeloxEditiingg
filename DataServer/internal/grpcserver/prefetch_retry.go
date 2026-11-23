package grpcserver

// prefetch_retry.go closes the one-way door left by the submission hook.
// PrefetchSubmittedJob fires exactly once, when the task becomes READY or
// right after enqueue. If no worker is eligible in that instant it logs
// "submission hook deferred" and returns. The READY task keeps
// runtime_assets_pending=1, the claim queries exclude it, and the job sits
// PENDING with no lease forever. This sweeper re-drives the same hook for
// tasks that are still READY, still runtime-pending and own no live
// reservation, which is the durable retry the hook comment promises.

import (
	"context"
	"sync"
	"time"

	"velox-server/internal/logging"
	"velox-server/internal/taskgraph"
)

const (
	prefetchRetrySweepInterval = time.Minute
	prefetchRetryMinAge        = 45 * time.Second
	prefetchRetryPerSweep      = 25
	prefetchRetryScanLimit     = 500
)

// prefetchRetryBackoff grows with each failed re-drive so a plan whose assets
// can never materialise stops hammering the worker every minute while the job
// stays visible.
var prefetchRetryBackoff = []time.Duration{
	2 * time.Minute, 5 * time.Minute, 15 * time.Minute, 30 * time.Minute,
}

type prefetchRetryState struct {
	attempts int
	nextAt   time.Time
}

type prefetchRetryTracker struct {
	mu    sync.Mutex
	state map[string]prefetchRetryState
}

func (t *prefetchRetryTracker) due(taskID string, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	st, ok := t.state[taskID]
	return !ok || !now.Before(st.nextAt)
}

func (t *prefetchRetryTracker) success(taskID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.state, taskID)
}

func (t *prefetchRetryTracker) failure(taskID string, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state == nil {
		t.state = map[string]prefetchRetryState{}
	}
	st := t.state[taskID]
	st.attempts++
	delay := prefetchRetryBackoff[len(prefetchRetryBackoff)-1]
	if st.attempts <= len(prefetchRetryBackoff) {
		delay = prefetchRetryBackoff[st.attempts-1]
	}
	st.nextAt = now.Add(delay)
	t.state[taskID] = st
}

// RunPrefetchRetrySweeper re-drives deferred prefetch hooks until ctx ends.
// A non-positive interval selects the production default.
func (h *Handler) RunPrefetchRetrySweeper(ctx context.Context, interval time.Duration) {
	if h == nil || h.taskRepo == nil {
		return
	}
	reader, ok := h.taskRepo.(taskgraph.Reader)
	if !ok {
		return
	}
	specs, ok := h.taskRepo.(taskgraph.TaskSpecStore)
	if !ok {
		return
	}
	reservations, ok := h.taskRepo.(taskgraph.FutureReservationStore)
	if !ok {
		return
	}
	if interval <= 0 {
		interval = prefetchRetrySweepInterval
	}
	// The first sweep waits one interval: a fresh submission gets the
	// low-latency hook first and only a missed one is re-driven here.
	tracker := &prefetchRetryTracker{}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.retryDeferredPrefetch(ctx, reader, specs, reservations, tracker)
		}
	}
}

func (h *Handler) retryDeferredPrefetch(ctx context.Context, reader taskgraph.Reader, specs taskgraph.TaskSpecStore, reservations taskgraph.FutureReservationStore, tracker *prefetchRetryTracker) {
	tasks, err := reader.List(ctx, taskgraph.Filter{
		Statuses: []taskgraph.Status{taskgraph.StatusReady},
		Limit:    prefetchRetryScanLimit,
	})
	if err != nil {
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCPrefetchFailed, "[PREFETCH] retry sweep list: %v", err)
		return
	}
	if len(tasks) == 0 {
		return
	}
	live := make(map[string]taskgraph.ReservationState)
	existing, err := reservations.ListFutureReservations(ctx, "")
	if err != nil {
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCPrefetchFailed, "[PREFETCH] retry sweep reservations: %v", err)
		return
	}
	for _, reservation := range existing {
		if reservation.TaskID != "" && !reservation.State.IsTerminal() {
			live[reservation.TaskID] = reservation.State
		}
	}
	now := time.Now().UTC()
	cutoff := now.Add(-prefetchRetryMinAge)
	redriven := 0
	redrivenTasks := make([]string, 0, prefetchRetryPerSweep)
	for _, task := range tasks {
		if redriven >= prefetchRetryPerSweep {
			break
		}
		if task.JobID == "" || !task.CreatedAt.Before(cutoff) {
			continue
		}
		if state, ok := live[task.ID]; ok {
			// Only verified evidence resets the backoff: a PLANNING reservation
			// is created even when the plan's assets never materialise.
			if state == taskgraph.ReservationPrepared {
				tracker.success(task.ID)
			}
			continue
		}
		if !tracker.due(task.ID, now) {
			continue
		}
		payload, err := specs.GetTaskSpecPayload(ctx, task.ID)
		if err != nil {
			continue
		}
		pending, _ := payload["runtime_assets_pending"].(bool)
		if !pending {
			continue
		}
		logGRPCf(ctx, logging.LevelInfo, logging.CodeGRPCPrefetch, "[PREFETCH] retry sweep re-driving deferred job=%s task=%s", task.JobID, task.ID)
		h.PrefetchSubmittedJob(ctx, task.JobID)
		redriven++
		redrivenTasks = append(redrivenTasks, task.ID)
	}
	if redriven == 0 {
		return
	}
	// Every re-drive that did not already own a verified reservation counts as
	// a failed attempt; only PREPARED evidence resets the counter above.
	for _, taskID := range redrivenTasks {
		tracker.failure(taskID, now)
	}
	logGRPCf(ctx, logging.LevelInfo, logging.CodeGRPCPrefetch, "[PREFETCH] retry sweep re-drove %d deferred job(s)", redriven)
}

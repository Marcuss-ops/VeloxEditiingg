package darkeditor

import (
	"sort"
	"sync"
	"time"
)

const (
	defaultBackgroundTaskTTL             = 24 * time.Hour
	defaultBackgroundTaskMaxSize         = 1000
	defaultBackgroundTaskCleanupInterval = 5 * time.Minute
)

// BackgroundTaskRepository stores asynchronous background-removal status.
// It owns all synchronization and never exposes pointers into its internal
// map. Expiration is enforced on reads/writes and by the optional cleanup
// goroutine, while maxSize bounds memory even when tasks are never queried.
type BackgroundTaskRepository struct {
	mu      sync.RWMutex
	tasks   map[string]BackgroundRemovalStatus
	ttl     time.Duration
	maxSize int
	now     func() time.Time

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// NewBackgroundTaskRepository creates a repository using production defaults
// for TTL, capacity, and periodic cleanup.
func NewBackgroundTaskRepository(ttl time.Duration, maxSize int) *BackgroundTaskRepository {
	if ttl <= 0 {
		ttl = defaultBackgroundTaskTTL
	}
	if maxSize <= 0 {
		maxSize = defaultBackgroundTaskMaxSize
	}
	return newBackgroundTaskRepository(ttl, maxSize, defaultBackgroundTaskCleanupInterval, time.Now)
}

func newBackgroundTaskRepository(ttl time.Duration, maxSize int, cleanupInterval time.Duration, now func() time.Time) *BackgroundTaskRepository {
	if ttl <= 0 {
		ttl = defaultBackgroundTaskTTL
	}
	if maxSize <= 0 {
		maxSize = defaultBackgroundTaskMaxSize
	}
	if now == nil {
		now = time.Now
	}

	r := &BackgroundTaskRepository{
		tasks:   make(map[string]BackgroundRemovalStatus),
		ttl:     ttl,
		maxSize: maxSize,
		now:     now,
	}
	if cleanupInterval > 0 {
		r.stopCh = make(chan struct{})
		r.doneCh = make(chan struct{})
		go r.cleanupLoop(cleanupInterval)
	}
	return r
}

// Set inserts or replaces a task status. The input is copied before storage.
func (r *BackgroundTaskRepository) Set(status BackgroundRemovalStatus) {
	if r == nil || status.TaskID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cleanupExpiredLocked(r.now())
	r.tasks[status.TaskID] = status
	r.evictToLimitLocked()
}

// Get returns a value copy of a task status. Expired tasks are treated as not
// found and removed before the result is returned.
func (r *BackgroundTaskRepository) Get(taskID string) (BackgroundRemovalStatus, bool) {
	if r == nil || taskID == "" {
		return BackgroundRemovalStatus{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cleanupExpiredLocked(r.now())
	status, ok := r.tasks[taskID]
	return status, ok
}

// Update applies fn to a private copy of the stored status. The callback runs
// under the repository write lock and cannot retain an internal pointer; it
// should only mutate the supplied status and must not call back into r.
func (r *BackgroundTaskRepository) Update(taskID string, fn func(*BackgroundRemovalStatus)) bool {
	if r == nil || taskID == "" || fn == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cleanupExpiredLocked(r.now())
	status, ok := r.tasks[taskID]
	if !ok {
		return false
	}
	fn(&status)
	status.TaskID = taskID
	r.tasks[taskID] = status
	return true
}

// Cleanup removes all statuses older than the configured TTL and then applies
// the capacity bound. It returns the number of removed entries.
func (r *BackgroundTaskRepository) Cleanup() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	removed := r.cleanupExpiredLocked(r.now())
	beforeEviction := len(r.tasks)
	r.evictToLimitLocked()
	return removed + beforeEviction - len(r.tasks)
}

// Len returns the current number of retained statuses after lazy expiration.
func (r *BackgroundTaskRepository) Len() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cleanupExpiredLocked(r.now())
	return len(r.tasks)
}

// Close stops the periodic cleanup goroutine. It is safe to call more than
// once and is useful for tests and controlled handler shutdown.
func (r *BackgroundTaskRepository) Close() {
	if r == nil || r.stopCh == nil {
		return
	}
	r.stopOnce.Do(func() {
		close(r.stopCh)
		<-r.doneCh
	})
}

func (r *BackgroundTaskRepository) cleanupLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer close(r.doneCh)
	for {
		select {
		case <-ticker.C:
			r.Cleanup()
		case <-r.stopCh:
			return
		}
	}
}

func (r *BackgroundTaskRepository) cleanupExpiredLocked(now time.Time) int {
	if r.ttl <= 0 {
		return 0
	}
	removed := 0
	for taskID, status := range r.tasks {
		if !status.StartedAt.IsZero() && !now.Before(status.StartedAt.Add(r.ttl)) {
			delete(r.tasks, taskID)
			removed++
		}
	}
	return removed
}

func (r *BackgroundTaskRepository) evictToLimitLocked() {
	if r.maxSize <= 0 || len(r.tasks) <= r.maxSize {
		return
	}
	ids := make([]string, 0, len(r.tasks))
	for taskID := range r.tasks {
		ids = append(ids, taskID)
	}
	sort.Slice(ids, func(i, j int) bool {
		left := r.tasks[ids[i]]
		right := r.tasks[ids[j]]
		leftTerminal := backgroundTaskTerminal(left)
		rightTerminal := backgroundTaskTerminal(right)
		if leftTerminal != rightTerminal {
			// Prefer evicting completed/failed statuses before active work.
			return leftTerminal
		}
		if left.StartedAt.Equal(right.StartedAt) {
			return ids[i] < ids[j]
		}
		return left.StartedAt.Before(right.StartedAt)
	})
	for _, taskID := range ids[:len(ids)-r.maxSize] {
		delete(r.tasks, taskID)
	}
}

func backgroundTaskTerminal(status BackgroundRemovalStatus) bool {
	return status.Status == "completed" || status.Status == "failed"
}

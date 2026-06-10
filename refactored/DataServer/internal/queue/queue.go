package queue

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

// Priority levels for jobs
const (
	PriorityLow    int = 1
	PriorityNormal int = 5
	PriorityHigh   int = 10
	PriorityUrgent int = 100
)

// ensureProjectionRenderPlanVersion keeps queue projections aligned with the
// canonical render-plan contract version used by the worker and master APIs.
func ensureProjectionRenderPlanVersion(payload map[string]any) map[string]any {
	if payload == nil {
		payload = make(map[string]any)
	}
	if v, _ := payload["render_plan_version"].(string); v != "" {
		return payload
	}
	payload["render_plan_version"] = "v1"
	return payload
}

// PriorityJob wraps a Job with priority information
type PriorityJob struct {
	*Job
	Priority   int       `json:"priority"`
	EnqueuedAt time.Time `json:"enqueued_at"`
	Index      int       `json:"-"` // Index in the heap (managed by heap.Interface)
}

// PriorityQueue implements a thread-safe priority queue using container/heap
type PriorityQueue struct {
	mu    sync.RWMutex
	items []*PriorityJob
}

// Len implements heap.Interface
func (pq *PriorityQueue) Len() int {
	return len(pq.items)
}

// Less implements heap.Interface - higher priority comes first
// For equal priority, earlier enqueued jobs come first (FIFO within priority)
func (pq *PriorityQueue) Less(i, j int) bool {
	if pq.items[i].Priority != pq.items[j].Priority {
		return pq.items[i].Priority > pq.items[j].Priority
	}
	return pq.items[i].EnqueuedAt.Before(pq.items[j].EnqueuedAt)
}

// Swap implements heap.Interface
func (pq *PriorityQueue) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].Index = i
	pq.items[j].Index = j
}

// Push implements heap.Interface
func (pq *PriorityQueue) Push(x interface{}) {
	n := len(pq.items)
	item := x.(*PriorityJob)
	item.Index = n
	pq.items = append(pq.items, item)
}

// Pop implements heap.Interface
func (pq *PriorityQueue) Pop() interface{} {
	old := pq.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.Index = -1 // for safety
	pq.items = old[0 : n-1]
	return item
}

// NewPriorityQueue creates a new empty priority queue
func NewPriorityQueue() *PriorityQueue {
	pq := &PriorityQueue{
		items: make([]*PriorityJob, 0),
	}
	heap.Init(pq)
	return pq
}

// Enqueue adds a job to the priority queue
func (pq *PriorityQueue) Enqueue(job *Job, priority int) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	priorityJob := &PriorityJob{
		Job:        job,
		Priority:   priority,
		EnqueuedAt: time.Now().UTC(),
	}
	heap.Push(pq, priorityJob)
}

// EnqueueWithTime adds a job with a specific enqueue time (for restoration)
func (pq *PriorityQueue) EnqueueWithTime(job *Job, priority int, enqueuedAt time.Time) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	priorityJob := &PriorityJob{
		Job:        job,
		Priority:   priority,
		EnqueuedAt: enqueuedAt,
	}
	heap.Push(pq, priorityJob)
}

// Dequeue removes and returns the highest priority job
func (pq *PriorityQueue) Dequeue() *PriorityJob {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.Len() == 0 {
		return nil
	}
	return heap.Pop(pq).(*PriorityJob)
}

// Peek returns the highest priority job without removing it
func (pq *PriorityQueue) Peek() *PriorityJob {
	pq.mu.RLock()
	defer pq.mu.RUnlock()

	if pq.Len() == 0 {
		return nil
	}
	return pq.items[0]
}

// Size returns the number of items in the queue
func (pq *PriorityQueue) Size() int {
	pq.mu.RLock()
	defer pq.mu.RUnlock()
	return pq.Len()
}

// Clear removes all items from the queue
func (pq *PriorityQueue) Clear() {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	pq.items = make([]*PriorityJob, 0)
}

// GetAllItems returns a copy of all items in the queue (for inspection)
func (pq *PriorityQueue) GetAllItems() []*PriorityJob {
	pq.mu.RLock()
	defer pq.mu.RUnlock()

	items := make([]*PriorityJob, len(pq.items))
	copy(items, pq.items)
	return items
}

// PriorityAwareQueue combines FileQueue with priority scheduling
type PriorityAwareQueue struct {
	*FileQueue
	pq      *PriorityQueue
	pqMu    sync.RWMutex
	enabled bool
}

// NewPriorityAwareQueue creates a queue that combines file persistence with priority scheduling
func NewPriorityAwareQueue(cfg *FileQueueConfig) (*PriorityAwareQueue, error) {
	fq, err := NewFileQueue(cfg)
	if err != nil {
		return nil, err
	}

	return &PriorityAwareQueue{
		FileQueue: fq,
		pq:        NewPriorityQueue(),
		enabled:   true,
	}, nil
}

// SubmitJobWithPriority adds a job with a specific priority
func (paq *PriorityAwareQueue) SubmitJobWithPriority(ctx context.Context, jobID string, payload map[string]interface{}, priority int) error {
	// First persist to file
	if err := paq.SubmitJob(ctx, jobID, payload); err != nil {
		return err
	}

	// Get the created job
	job, err := paq.GetJob(ctx, jobID)
	if err != nil {
		return err
	}

	// Add to priority queue
	paq.pqMu.Lock()
	paq.pq.Enqueue(job, priority)
	paq.pqMu.Unlock()

	return nil
}

// GetNextPriorityJobID returns the next job ID considering priority
func (paq *PriorityAwareQueue) GetNextPriorityJobID(ctx context.Context) (string, error) {
	// Try priority queue first
	paq.pqMu.RLock()
	if paq.pq.Size() > 0 {
		item := paq.pq.Peek()
		if item != nil && item.Status == StatusPending {
			paq.pqMu.RUnlock()
			return item.JobID, nil
		}
		// Pop stale items from priority queue
		paq.pqMu.RUnlock()
		paq.pqMu.Lock()
		paq.pq.Dequeue()
		paq.pqMu.Unlock()
		return paq.GetNextPriorityJobID(ctx)
	}
	paq.pqMu.RUnlock()

	// Fall back to regular queue
	return paq.GetNextJobID(ctx)
}

// RebuildPriorityQueue reconstructs the priority queue from file-based jobs
func (paq *PriorityAwareQueue) RebuildPriorityQueue(ctx context.Context) error {
	jobs, err := paq.GetAllJobs(ctx)
	if err != nil {
		return err
	}

	paq.pqMu.Lock()
	defer paq.pqMu.Unlock()

	paq.pq.Clear()

	now := time.Now().UTC()
	for _, job := range jobs {
		if job.Status == StatusPending {
			priority := PriorityNormal
			if p, ok := job.Payload["priority"].(int); ok {
				priority = p
			} else if p, ok := job.Payload["priority"].(float64); ok {
				priority = int(p)
			}

			enqueuedAt := now
			if t, ok := job.Payload["enqueued_at"].(string); ok {
				if parsed, err := time.Parse(time.RFC3339, t); err == nil {
					enqueuedAt = parsed
				}
			}

			paq.pq.EnqueueWithTime(job, priority, enqueuedAt)
		}
	}

	return nil
}

// GetPriorityStats returns statistics about the priority queue
func (paq *PriorityAwareQueue) GetPriorityStats() map[string]int64 {
	paq.pqMu.RLock()
	defer paq.pqMu.RUnlock()

	stats := map[string]int64{
		"total":  int64(paq.pq.Size()),
		"high":   0,
		"normal": 0,
		"low":    0,
		"urgent": 0,
	}

	for _, item := range paq.pq.GetAllItems() {
		switch {
		case item.Priority >= PriorityUrgent:
			stats["urgent"]++
		case item.Priority >= PriorityHigh:
			stats["high"]++
		case item.Priority >= PriorityNormal:
			stats["normal"]++
		default:
			stats["low"]++
		}
	}

	return stats
}

// GetPriorityForJob determines priority based on job metadata
func GetPriorityForJob(payload map[string]interface{}) int {
	// Check explicit priority
	if p, ok := payload["priority"].(int); ok {
		return p
	}
	if p, ok := payload["priority"].(float64); ok {
		return int(p)
	}

	// Check job type for implicit priority
	jobType, _ := payload["job_type"].(string)
	switch jobType {
	case "urgent", "priority", "manual_intervention":
		return PriorityUrgent
	case "video_generation", "render":
		return PriorityHigh
	case "cleanup", "maintenance":
		return PriorityLow
	default:
		return PriorityNormal
	}
}

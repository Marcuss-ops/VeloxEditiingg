// Package admission provides bounded resource admission and a fair in-memory
// dispatch queue. Durable task state remains in SQLite; this layer decides
// whether work may start and explains why it is waiting.
package admission

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrQuotaExceeded = errors.New("admission: quota exceeded")
	ErrNotAdmitted   = errors.New("admission: request not admissible")
	ErrQueueFull     = errors.New("admission: queue full")
)

type Limits struct {
	MaxJobsPerBatch        int
	MaxScenesPerJob        int
	MaxInputBytes          int64
	MaxDurationSeconds     float64
	MaxTempBytes           int64
	MaxRenderConcurrent    int
	MaxUploadConcurrent    int
	MaxRetries             int
	MaxQueueItems          int
	MaxConsecutivePerBatch int
	UrgentReservedSlots    int
}

type Request struct {
	ID, BatchID, ProjectID, ChannelID string
	Scenes                            int
	InputBytes, TempBytes             int64
	DurationSeconds                   float64
	RenderSlots, UploadSlots          int
	Retries                           int
	WorkerCompatible                  bool
	ProviderRateLimited               bool
	CredentialUsable                  bool
	Deadline                          time.Time
}

type Usage struct {
	JobsByBatch                  map[string]int
	RenderRunning, UploadRunning int
	ReservedTempBytes            int64
}

type Controller struct {
	mu        sync.Mutex
	limits    Limits
	usage     Usage
	active    map[string]Request
	paused    map[string]bool
	cancelled map[string]bool
}

func NewController(limits Limits) *Controller {
	return &Controller{limits: limits, usage: Usage{JobsByBatch: map[string]int{}}, active: map[string]Request{}, paused: map[string]bool{}, cancelled: map[string]bool{}}
}

func (c *Controller) Reserve(req Request) (*Reservation, error) {
	if c == nil {
		return nil, ErrNotAdmitted
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if reason := c.reasonLocked(req); reason != "" {
		return nil, fmt.Errorf("%w: %s", ErrNotAdmitted, reason)
	}
	c.active[req.ID] = req
	c.usage.JobsByBatch[req.BatchID]++
	c.usage.RenderRunning += req.RenderSlots
	c.usage.UploadRunning += req.UploadSlots
	c.usage.ReservedTempBytes += req.TempBytes
	return &Reservation{controller: c, id: req.ID}, nil
}

func (c *Controller) reasonLocked(req Request) string {
	l := c.limits
	if c.cancelled[req.BatchID] {
		return "batch_cancelled"
	}
	if c.paused[req.BatchID] {
		return "batch_paused"
	}
	if l.MaxJobsPerBatch > 0 && c.usage.JobsByBatch[req.BatchID] >= l.MaxJobsPerBatch {
		return "batch_job_quota"
	}
	if l.MaxScenesPerJob > 0 && req.Scenes > l.MaxScenesPerJob {
		return "scenes_limit"
	}
	if l.MaxInputBytes > 0 && req.InputBytes > l.MaxInputBytes {
		return "input_bytes_limit"
	}
	if l.MaxDurationSeconds > 0 && req.DurationSeconds > l.MaxDurationSeconds {
		return "duration_limit"
	}
	if l.MaxTempBytes > 0 && req.TempBytes > l.MaxTempBytes {
		return "temp_bytes_limit"
	}
	if l.MaxRenderConcurrent > 0 && c.usage.RenderRunning+req.RenderSlots > l.MaxRenderConcurrent {
		return "render_concurrency"
	}
	if l.MaxUploadConcurrent > 0 && c.usage.UploadRunning+req.UploadSlots > l.MaxUploadConcurrent {
		return "upload_concurrency"
	}
	if l.MaxRetries > 0 && req.Retries > l.MaxRetries {
		return "retry_limit"
	}
	if !req.WorkerCompatible {
		return "worker_incompatible"
	}
	if req.ProviderRateLimited {
		return "provider_rate_limited"
	}
	if !req.CredentialUsable {
		return "credential_unusable"
	}
	if !req.Deadline.IsZero() && time.Now().After(req.Deadline) {
		return "deadline_unrealistic"
	}
	return ""
}

func (c *Controller) Release(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	req, ok := c.active[id]
	if !ok {
		return
	}
	delete(c.active, id)
	c.usage.JobsByBatch[req.BatchID]--
	c.usage.RenderRunning -= req.RenderSlots
	c.usage.UploadRunning -= req.UploadSlots
	c.usage.ReservedTempBytes -= req.TempBytes
}
func (c *Controller) PauseBatch(id string)  { c.mu.Lock(); defer c.mu.Unlock(); c.paused[id] = true }
func (c *Controller) ResumeBatch(id string) { c.mu.Lock(); defer c.mu.Unlock(); delete(c.paused, id) }
func (c *Controller) CancelBatch(id string) { c.mu.Lock(); defer c.mu.Unlock(); c.cancelled[id] = true }
func (c *Controller) Usage() Usage {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.usage
	out.JobsByBatch = map[string]int{}
	for k, v := range c.usage.JobsByBatch {
		out.JobsByBatch[k] = v
	}
	return out
}

type Reservation struct {
	controller *Controller
	id         string
	once       sync.Once
}

func (r *Reservation) Release() {
	if r != nil {
		r.once.Do(func() { r.controller.Release(r.id) })
	}
}

type QueueItem struct {
	ID, BatchID, ProjectID string
	Priority               int
	Urgent                 bool
	SubmittedAt            time.Time
}

type FairQueue struct {
	mu                sync.Mutex
	limits            Limits
	items             []QueueItem
	paused, cancelled map[string]bool
	weights           map[string]int
	served            map[string]int
	consecutiveBatch  string
	consecutive       int
}

func NewFairQueue(limits Limits) *FairQueue {
	return &FairQueue{limits: limits, paused: map[string]bool{}, cancelled: map[string]bool{}, weights: map[string]int{}, served: map[string]int{}}
}
func (q *FairQueue) SetProjectWeight(project string, weight int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if weight < 1 {
		weight = 1
	}
	q.weights[project] = weight
}
func (q *FairQueue) Enqueue(item QueueItem) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.limits.MaxQueueItems > 0 && len(q.items) >= q.limits.MaxQueueItems {
		return ErrQueueFull
	}
	if item.SubmittedAt.IsZero() {
		item.SubmittedAt = time.Now()
	}
	q.items = append(q.items, item)
	return nil
}
func (q *FairQueue) PauseBatch(id string)  { q.mu.Lock(); defer q.mu.Unlock(); q.paused[id] = true }
func (q *FairQueue) ResumeBatch(id string) { q.mu.Lock(); defer q.mu.Unlock(); delete(q.paused, id) }
func (q *FairQueue) CancelBatch(id string) { q.mu.Lock(); defer q.mu.Unlock(); q.cancelled[id] = true }
func (q *FairQueue) Len() int              { q.mu.Lock(); defer q.mu.Unlock(); return len(q.items) }

func (q *FairQueue) Next(now time.Time) (QueueItem, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	best := -1
	bestScore := -1e18
	for i, item := range q.items {
		if q.paused[item.BatchID] || q.cancelled[item.BatchID] {
			continue
		}
		if q.limits.MaxConsecutivePerBatch > 0 && q.consecutive >= q.limits.MaxConsecutivePerBatch && q.consecutiveBatch == item.BatchID {
			continue
		}
		age := now.Sub(item.SubmittedAt).Seconds()
		if age < 0 {
			age = 0
		}
		weight := q.weights[item.ProjectID]
		if weight < 1 {
			weight = 1
		}
		score := float64(item.Priority)*100000 + age*10 - float64(q.served[item.ProjectID])/float64(weight)
		if item.Urgent {
			score += 1e9
		}
		if score > bestScore {
			best, bestScore = i, score
		}
	}
	if best < 0 {
		return QueueItem{}, false
	}
	item := q.items[best]
	q.items = append(q.items[:best], q.items[best+1:]...)
	q.served[item.ProjectID]++
	if q.consecutiveBatch == item.BatchID {
		q.consecutive++
	} else {
		q.consecutiveBatch = item.BatchID
		q.consecutive = 1
	}
	return item, true
}

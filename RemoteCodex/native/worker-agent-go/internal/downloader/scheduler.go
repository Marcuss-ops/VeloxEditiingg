package downloader

// scheduler.go — the concurrency-bounded, priority-stable transfer pool.
//
// A fixed number of dispatcher goroutines (Config.Concurrency) drain one
// priority queue. Dispatch order is stable:
//
//	priority  (higher first)
//	→ queued_at (older first)
//	→ asset_key (lexicographic tie-break)
//
// Enqueuing is O(log n); dispatchers block on a condition variable when the
// queue is empty, so an idle pool costs no CPU. Close() wakes all dispatchers
// and joins them.

import (
	"container/heap"
	"sync"
	"time"

	"velox-shared/assetref"
)

// schedItem is one queued transfer. run must be non-blocking-safe to call
// outside the scheduler lock.
type schedItem struct {
	key      assetref.AssetKey
	priority int
	queuedAt time.Time
	run      func()
	index    int // maintained by heap.Interface
}

// schedQueue implements heap.Interface with the stable ordering above.
type schedQueue []*schedItem

func (q schedQueue) Len() int { return len(q) }

// Less returns true when item i must be dispatched before item j.
func (q schedQueue) Less(i, j int) bool {
	a, b := q[i], q[j]
	if a.priority != b.priority {
		return a.priority > b.priority
	}
	if !a.queuedAt.Equal(b.queuedAt) {
		return a.queuedAt.Before(b.queuedAt)
	}
	return a.key < b.key
}

func (q schedQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].index = i
	q[j].index = j
}

func (q *schedQueue) Push(x interface{}) {
	item := x.(*schedItem)
	item.index = len(*q)
	*q = append(*q, item)
}

func (q *schedQueue) Pop() interface{} {
	old := *q
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid retaining a dead pointer
	item.index = -1
	*q = old[:n-1]
	return item
}

// scheduler is a pool of `concurrency` dispatchers draining one stable queue.
type scheduler struct {
	concurrency int
	now         func() time.Time

	mu     sync.Mutex
	queue  schedQueue
	items  map[assetref.AssetKey]*schedItem
	cond   *sync.Cond
	closed bool

	wg sync.WaitGroup
}

func newScheduler(concurrency int, now func() time.Time) *scheduler {
	if concurrency < 1 {
		concurrency = 1
	}
	s := &scheduler{
		concurrency: concurrency,
		now:         now,
		items:       make(map[assetref.AssetKey]*schedItem),
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// Start launches the dispatcher goroutines. Must be called before Enqueue.
func (s *scheduler) Start() {
	s.wg.Add(s.concurrency)
	for i := 0; i < s.concurrency; i++ {
		go s.dispatcher()
	}
}

// Enqueue adds a transfer to the stable queue. run is invoked on a
// dispatcher goroutine once the item reaches the head and a slot is free.
// Returns false — discarding the item — when the pool is already closed;
// the caller must then settle the transfer itself (e.g. as cancelled) so no
// waiter hangs on a transfer that will never run.
func (s *scheduler) Enqueue(key assetref.AssetKey, priority int, queuedAt time.Time, run func()) bool {
	if run == nil {
		return false
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return false
	}
	item := &schedItem{
		key:      key,
		priority: priority,
		queuedAt: queuedAt,
		run:      run,
	}
	heap.Push(&s.queue, item)
	s.items[key] = item
	s.cond.Signal()
	s.mu.Unlock()
	return true
}

// Promote updates a queued transfer in place. Running transfers retain the
// promoted priority on Transfer for observability and future QoS hooks.
func (s *scheduler) Promote(key assetref.AssetKey, priority int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.items[key]
	if item == nil || item.index < 0 {
		return false
	}
	if priority <= item.priority {
		return true
	}
	item.priority = priority
	heap.Fix(&s.queue, item.index)
	s.cond.Signal()
	return true
}

// Size returns the number of transfers currently queued (not running).
func (s *scheduler) Size() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queue)
}

func (s *scheduler) dispatcher() {
	defer s.wg.Done()
	for {
		s.mu.Lock()
		for len(s.queue) == 0 && !s.closed {
			s.cond.Wait()
		}
		if len(s.queue) == 0 && s.closed {
			s.mu.Unlock()
			return
		}
		item := heap.Pop(&s.queue).(*schedItem)
		delete(s.items, item.key)
		s.mu.Unlock()

		item.run()
	}
}

// Close stops the pool. Pending queued items are drained through their run
// callbacks after the queue is closed; each callback observes the cancelled
// transfer context and settles its Transfer as CANCELLED. Running callbacks
// are allowed to observe the same cancellation. Close is idempotent and safe
// to call from any goroutine.
func (s *scheduler) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.wg.Wait()
		return
	}
	s.closed = true
	pending := make([]func(), 0, len(s.queue))
	for len(s.queue) > 0 {
		item := heap.Pop(&s.queue).(*schedItem)
		pending = append(pending, item.run)
	}
	s.cond.Broadcast()
	s.mu.Unlock()

	for _, run := range pending {
		run()
	}
	s.wg.Wait()
}

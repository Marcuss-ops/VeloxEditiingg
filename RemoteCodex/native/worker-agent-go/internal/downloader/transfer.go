package downloader

// transfer.go — one shared transfer per asset key, plus the registry that
// owns them.
//
// The central rule of the plan: the transfer must NEVER be bound to the
// context of the job that first requested it. Cancelling the first job would
// otherwise tear down the download for the other nine. The Transfer therefore
// owns a context derived from the MANAGER's lifetime:
//
//	transferCtx, cancel := context.WithCancel(workerCtx)
//
// Every job is just a waiter: jobs register on the transfer, the transfer
// publishes progress to all of them, and cancellation happens only when the
// LAST waiter leaves (waiters == 0) while the transfer is still running.
//
// Two jobs on the same stock asset therefore observe exactly the plan's
// invariant:
//
//	1 upstream request, 1 temp file, 1 SHA verification,
//	2 jobs receiving the same progress.

import (
	"context"
	"errors"
	"sync"
	"time"
)

// waiterKey identifies one logical waiter: a (job, task) pair. The same task
// resolving the same asset twice registers once (map semantics).
type waiterKey struct{ jobID, taskID string }

// errTransferCancelled is the sentinel surfaced to waiters when the transfer
// was cancelled (last waiter left or the manager closed).
var errTransferCancelled = errors.New("downloader: transfer cancelled")

// Transfer is one asset download shared by N waiters.
type Transfer struct {
	Key string
	req DownloadRequest

	transferCtx context.Context
	cancel      context.CancelFunc
	// reportCtx carries the first waiter's caller-scoped telemetry context.
	// Value-reads only — see Transferer's context contract in source.go.
	reportCtx context.Context
	// now is the manager's clock, used for all transfer timestamps.
	now func() time.Time

	mu sync.Mutex

	state           DownloadState
	cacheHit        bool
	attempt         int
	queueSeq        int64
	bytesTotal      int64
	bytesDownloaded int64
	queuedAt        time.Time
	startedAt       time.Time
	updatedAt       time.Time
	completedAt     time.Time

	waiters     map[waiterKey]struct{}
	subscribers map[uint64]chan DownloadSnapshot
	subSeq      uint64

	res        TransferResult
	err        error
	done       chan struct{}
	once       sync.Once
	transferID string
}

// newTransfer builds a transfer with its OWN context (manager-derived), plus
// the first waiter's report context. A nil reportCtx degrades to
// context.Background() (caller-scoped telemetry simply missing).
func newTransfer(managerCtx context.Context, key string, req DownloadRequest, reportCtx context.Context, now func() time.Time, transferID string) *Transfer {
	if reportCtx == nil {
		reportCtx = context.Background()
	}
	ctx, cancel := context.WithCancel(managerCtx)
	t := &Transfer{
		Key:         key,
		req:         req,
		transferCtx: ctx,
		cancel:      cancel,
		reportCtx:   reportCtx,
		now:         now,
		state:       DownloadQueued,
		bytesTotal:  req.SizeBytes,
		queuedAt:    now(),
		updatedAt:   now(),
		waiters:     make(map[waiterKey]struct{}),
		subscribers: make(map[uint64]chan DownloadSnapshot),
		done:        make(chan struct{}),
		transferID:  transferID,
	}
	return t
}

// doneCh returns the completion channel, closed once when the transfer
// reaches a terminal state.
func (t *Transfer) doneCh() <-chan struct{} { return t.done }

// transferContext exposes the transfer-owned context (cancellation for the
// byte pipeline). Used by the manager's run loop.
func (t *Transfer) transferContext() context.Context { return t.transferCtx }

// reportContext exposes the first waiter's telemetry context.
func (t *Transfer) reportContext() context.Context { return t.reportCtx }

// isTerminal reports whether the transfer already reached a terminal state.
func (t *Transfer) isTerminal() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state.Terminal()
}

// addWaiter registers one logical waiter. SharedWaiters in snapshots equals
// the current waiter count.
func (t *Transfer) addWaiter(jobID, taskID string) {
	t.mu.Lock()
	t.waiters[waiterKey{jobID, taskID}] = struct{}{}
	t.updatedAt = t.now()
	t.mu.Unlock()
}

// removeWaiter unregisters one waiter and reports whether it was the last one.
func (t *Transfer) removeWaiter(jobID, taskID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.waiters, waiterKey{jobID, taskID})
	return len(t.waiters) == 0
}

// waiterCount returns the number of registered waiters.
func (t *Transfer) waiterCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.waiters)
}

// subscribe registers a snapshot subscriber. The returned channel delivers
// every published snapshot (non-blocking: a slow consumer drops intermediate
// snapshots, never stalls the transfer). The unsubscribe func removes the
// subscription and is idempotent.
//
// When the transfer is already terminal (settled before the subscriber
// arrived), the channel delivers the terminal snapshot exactly once and is
// closed, so a late subscriber never blocks on an empty channel.
func (t *Transfer) subscribe() (<-chan DownloadSnapshot, func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state.Terminal() {
		ch := make(chan DownloadSnapshot, 1)
		ch <- t.snapshotLocked(t.now())
		close(ch)
		return ch, func() {}
	}
	t.subSeq++
	id := t.subSeq
	ch := make(chan DownloadSnapshot, 32)
	t.subscribers[id] = ch
	var once sync.Once
	unsub := func() {
		once.Do(func() {
			t.mu.Lock()
			delete(t.subscribers, id)
			t.mu.Unlock()
		})
	}
	return ch, unsub
}

// setState transitions the state machine and publishes a snapshot. Terminal
// states are settled only via finish/finishCancelled, never here.
func (t *Transfer) setState(s DownloadState) {
	now := t.now()
	t.mu.Lock()
	t.state = s
	t.updatedAt = now
	t.mu.Unlock()
	t.publish(now)
}

// setCacheHit marks a cache-hit outcome before finishing READY.
func (t *Transfer) setCacheHit(hit bool) {
	t.mu.Lock()
	t.cacheHit = hit
	t.mu.Unlock()
}

// setDownloading records DOWNLOADING start (attempt bump + startedAt).
func (t *Transfer) setDownloading() {
	now := t.now()
	t.mu.Lock()
	t.state = DownloadRunning
	t.attempt++
	if t.startedAt.IsZero() {
		t.startedAt = now
	}
	t.updatedAt = now
	t.mu.Unlock()
	t.publish(now)
}

// setQueuePos records the stable-queue sequence assigned by the scheduler.
func (t *Transfer) setQueuePos(seq int64) {
	now := t.now()
	t.mu.Lock()
	t.queueSeq = seq
	t.state = DownloadQueued
	t.updatedAt = now
	t.mu.Unlock()
	t.publish(now)
}

// finish settles the transfer into READY (nil err) or FAILED, publishes the
// terminal snapshot and closes done exactly once. Only the run loop calls it.
func (t *Transfer) finish(result TransferResult, err error) {
	now := t.now()
	t.mu.Lock()
	t.res = result
	t.err = err
	t.completedAt = now
	t.updatedAt = now
	if err != nil {
		t.state = DownloadFailed
	} else {
		t.state = DownloadReady
		t.bytesDownloaded = result.Bytes
	}
	t.mu.Unlock()
	t.publish(now)
	t.once.Do(func() { close(t.done) })
}

// finishCancelled settles the transfer into CANCELLED. Used when the transfer
// context dies (last waiter left or manager closed) before completion. No-op
// once the transfer already reached a terminal state.
func (t *Transfer) finishCancelled() {
	now := t.now()
	t.mu.Lock()
	if t.state.Terminal() {
		t.mu.Unlock()
		return
	}
	t.state = DownloadCancelled
	t.err = errTransferCancelled
	t.completedAt = now
	t.updatedAt = now
	t.mu.Unlock()
	t.publish(now)
	t.once.Do(func() { close(t.done) })
}

// result returns the settled (result, err) pair. Valid only after done is
// closed.
func (t *Transfer) result() (TransferResult, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.res, t.err
}

// publish sends the current snapshot to every subscriber. The snapshot is
// built UNDER the transfer mutex (snapshotLocked's contract) so concurrent
// writers (setState/addWaiter/finish from other goroutines) never race the
// read. Sends are non-blocking: a subscriber with a full buffer misses this
// snapshot.
func (t *Transfer) publish(now time.Time) {
	t.mu.Lock()
	snap := t.snapshotLocked(now)
	for _, ch := range t.subscribers {
		select {
		case ch <- snap:
		default:
		}
	}
	t.mu.Unlock()
}

// snapshot builds the observable snapshot. Caller must not hold t.mu.
func (t *Transfer) snapshot(now time.Time) DownloadSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.snapshotLocked(now)
}

// snapshotLocked builds the snapshot under the transfer mutex.
func (t *Transfer) snapshotLocked(now time.Time) DownloadSnapshot {
	state := t.state
	queuePos := 0
	if state == DownloadQueued {
		queuePos = int(t.queueSeq)
	}
	s := DownloadSnapshot{
		TransferID:      t.transferID,
		AssetKey:        t.Key,
		AssetID:         t.req.AssetID,
		Role:            t.req.Role,
		State:           state,
		BytesDownloaded: t.bytesDownloaded,
		BytesTotal:      t.bytesTotal,
		Attempt:         t.attempt,
		QueuePosition:   queuePos,
		SharedWaiters:   len(t.waiters),
		QueuedAt:        t.queuedAt,
		StartedAt:       t.startedAt,
		UpdatedAt:       t.updatedAt,
		CompletedAt:     t.completedAt,
		CacheHit:        t.cacheHit,
		ErrorCode:       errorCodeOf(t.err),
		ErrorDetail:     errorDetailOf(t.err),
	}
	if t.bytesTotal > 0 {
		s.ProgressPercent = float64(t.bytesDownloaded) / float64(t.bytesTotal) * 100
	}
	return s
}

// errorCodeOf derives a stable, low-cardinality error code from a transfer
// error ("" for nil).
func errorCodeOf(err error) string {
	if err == nil {
		return ""
	}
	switch {
	case errors.Is(err, errTransferCancelled):
		return "transfer_cancelled"
	case errors.Is(err, ErrVerify):
		return "verify_failed"
	case errors.Is(err, ErrPermanent):
		return "permanent"
	case errors.Is(err, ErrRetryable):
		return "retryable"
	default:
		return "failed"
	}
}

// errorDetailOf returns the error text ("" for nil).
func errorDetailOf(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// TransferRegistry owns the set of transfers, keyed by AssetKey.
type TransferRegistry struct {
	mu        sync.Mutex
	transfers map[string]*Transfer
}

func newTransferRegistry() *TransferRegistry {
	return &TransferRegistry{transfers: make(map[string]*Transfer)}
}

// Get returns the transfer for key (nil when absent).
func (r *TransferRegistry) Get(key string) *Transfer {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.transfers[key]
}

// GetOrCreate returns the transfer for key when it exists, otherwise creates
// one via mk and stores it. `created` reports whether mk ran.
func (r *TransferRegistry) GetOrCreate(key string, mk func() *Transfer) (t *Transfer, created bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing := r.transfers[key]; existing != nil {
		return existing, false
	}
	t = mk()
	r.transfers[key] = t
	return t, true
}

// Each visits every registered transfer (deterministic key order).
func (r *TransferRegistry) Each(f func(key string, t *Transfer)) {
	r.mu.Lock()
	keys := make([]string, 0, len(r.transfers))
	for k := range r.transfers {
		keys = append(keys, k)
	}
	r.mu.Unlock()
	for _, k := range keys {
		if t := r.Get(k); t != nil {
			f(k, t)
		}
	}
}

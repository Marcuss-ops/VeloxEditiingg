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
	"sync"
	"time"

	"velox-shared/assetref"
)

// Transfer is one asset download shared by N waiters.
type Transfer struct {
	Key assetref.AssetKey
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

	// Progress tracking (refreshed by reportProgress as bytes land):
	// throughputBPS is the bytes/sec measured across consecutive progress
	// samples; the publish/checkpoint baselines drive the throttles.
	throughputBPS       float64
	lastSampleAt        time.Time
	lastSampleBytes     int64
	lastPublishAt       time.Time
	lastPublishBytes    int64
	lastCheckpointAt    time.Time
	lastCheckpointBytes int64

	// Throttle configuration + durable hook (wired from manager.Config).
	publishInterval       time.Duration
	publishBytes          int64
	checkpointInterval    time.Duration
	checkpointBytes       int64
	onCheckpoint          func(DownloadSnapshot, context.Context)
	onOperationalSnapshot func()

	waiters map[waiterKey]struct{}
	// jobRefs survives waiter removal so JobSnapshot remains a durable
	// per-job read model after an asset reaches READY/FAILED.
	jobRefs            map[string]DownloadJobReference
	sceneIDs           map[string]struct{}
	subscribers        map[uint64]chan DownloadSnapshot
	checkpointSequence int64
	subSeq             uint64

	res                TransferResult
	err                error
	done               chan struct{}
	once               sync.Once
	transferID         string
	transferGeneration int64
}

// newTransfer builds a transfer with its OWN context (manager-derived), plus
// the first waiter's report context. A nil reportCtx degrades to
// context.Background() (caller-scoped telemetry simply missing).
func newTransfer(managerCtx context.Context, key assetref.AssetKey, req DownloadRequest, reportCtx context.Context, now func() time.Time, transferID string, transferGeneration int64) *Transfer {
	if reportCtx == nil {
		reportCtx = context.Background()
	}
	ctx, cancel := context.WithCancel(managerCtx)
	t := &Transfer{
		Key:                key,
		req:                req,
		transferCtx:        ctx,
		cancel:             cancel,
		reportCtx:          reportCtx,
		now:                now,
		state:              DownloadQueued,
		bytesTotal:         req.SizeBytes,
		queuedAt:           now(),
		updatedAt:          now(),
		waiters:            make(map[waiterKey]struct{}),
		jobRefs:            make(map[string]DownloadJobReference),
		sceneIDs:           make(map[string]struct{}),
		subscribers:        make(map[uint64]chan DownloadSnapshot),
		done:               make(chan struct{}),
		transferID:         transferID,
		transferGeneration: transferGeneration,
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

// setState transitions the state machine and publishes a snapshot. Terminal
// states are settled only via finish/finishCancelled, never here.
func (t *Transfer) setState(s DownloadState) {
	now := t.now()
	t.mu.Lock()
	t.state = s
	t.updatedAt = now
	t.mu.Unlock()
	t.publish(now)
	t.notifyOperational()
}

// setCacheHit marks a cache-hit outcome before finishing READY.
func (t *Transfer) setCacheHit(hit bool) {
	t.mu.Lock()
	t.cacheHit = hit
	t.mu.Unlock()
}

// setDownloading records DOWNLOADING start (attempt bump + startedAt) and
// resets the progress baselines so each attempt re-baselines its rate sample
// and publishes its first byte report immediately.
func (t *Transfer) setDownloading() {
	now := t.now()
	t.mu.Lock()
	t.state = DownloadRunning
	t.attempt++
	if t.startedAt.IsZero() {
		t.startedAt = now
	}
	t.updatedAt = now
	t.throughputBPS = 0
	t.lastSampleAt = time.Time{}
	t.lastSampleBytes = 0
	t.lastPublishAt = time.Time{}
	t.lastPublishBytes = 0
	t.lastCheckpointAt = time.Time{}
	t.lastCheckpointBytes = 0
	t.mu.Unlock()
	t.publish(now)
	t.notifyOperational()
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
	t.notifyOperational()
}

// finish settles the transfer into READY (nil err) or FAILED, publishes the
// terminal snapshot, emits the terminal checkpoint and closes done exactly
// once. Only the run loop calls it. Terminal transitions always publish and
// checkpoint immediately, bypassing the throttles.
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
	t.emitCheckpoint(now)
	t.once.Do(func() { close(t.done) })
	t.notifyOperational()
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
	t.emitCheckpoint(now)
	t.once.Do(func() { close(t.done) })
	t.notifyOperational()
}

// result returns the settled (result, err) pair. Valid only after done is
// closed.
func (t *Transfer) result() (TransferResult, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.res, t.err
}

package downloader

// manager.go — the canonical AssetDownloadManager implementation.
//
// Resolve is the single entry point. It de-duplicates concurrent requests for
// the same asset key onto one shared Transfer (the byte pipeline runs at most
// once per key), registers the caller as a waiter, and blocks until the
// transfer settles or the caller's own context is cancelled. The transfer
// context is manager-owned, so a cancelled caller only removes its waiter —
// it never kills the shared download while other jobs still need it.
//
// State machine driven here:
//
//	QUEUED → CACHE_CHECK → CACHE_HIT → READY        (verified cache hit)
//	QUEUED → CACHE_CHECK → QUEUED → DOWNLOADING → VERIFYING → READY
//	... → CANCELLED                                  (last waiter left / close)
//	... → FAILED                                     (transferer error)
//
// READY is reachable only from CACHE_HIT or VERIFYING — never directly from
// a byte stream. Progress percent is weighted on bytes (bytes_downloaded /
// bytes_total), never on file counts.

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"velox-shared/assetref"
)

// ErrEmptyKey is returned by Resolve when a request carries neither an
// AssetKey nor an AssetID to derive one from.
var ErrEmptyKey = errors.New("downloader: DownloadRequest has no asset key")

// Config tunes the manager. Zero values are replaced by defaults.
type Config struct {
	// Concurrency is the number of simultaneous byte transfers per worker
	// (VELOX_ASSET_DOWNLOAD_CONCURRENCY). Default 4.
	Concurrency int
	// Now is the clock used for all snapshots/timestamps; nil → time.Now.
	// Tests inject a deterministic clock.
	Now func() time.Time

	// PublishInterval / PublishBytes throttle subscriber snapshots: at least
	// one publish per interval or per newly-downloaded bytes, whichever comes
	// first. State changes and terminal transitions always publish
	// immediately. Defaults: 500ms / 4 MiB.
	PublishInterval time.Duration
	PublishBytes    int64

	// CheckpointInterval / CheckpointBytes throttle the durable OnCheckpoint
	// hook (coarser than publishes). Terminal transitions always checkpoint.
	// Defaults: 2s / 16 MiB.
	CheckpointInterval time.Duration
	CheckpointBytes    int64

	// OnCheckpoint is invoked with the current transfer snapshot for durable
	// progress records (telemetry events, local persistence). Called on the
	// transferer goroutine at most once per throttle interval; MUST be
	// non-blocking.
	OnCheckpoint func(snapshot DownloadSnapshot, reportCtx context.Context)

	// OnOperationalSnapshot receives an aggregate low-cardinality view after
	// lifecycle or throttled progress updates. It must be non-blocking.
	OnOperationalSnapshot func(snapshot OperationalSnapshot)

	// OnCoalescedRequest observes a caller that joined an already-running
	// transfer. It must be non-blocking; the manager invokes it once per
	// coalesced Resolve call with the requested asset size for duplicate-
	// download accounting.
	OnCoalescedRequest func(sizeBytes int64)

	// MaxRetainedTransfers bounds terminal transfers kept for late Snapshot /
	// JobSnapshot reads. Zero uses the bounded default.
	MaxRetainedTransfers int
}

func (c *Config) withDefaults() {
	if c.Concurrency <= 0 {
		c.Concurrency = 4
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.PublishInterval <= 0 {
		c.PublishInterval = ProgressPublishInterval
	}
	if c.PublishBytes <= 0 {
		c.PublishBytes = ProgressPublishBytes
	}
	if c.CheckpointInterval <= 0 {
		c.CheckpointInterval = ProgressCheckpointInterval
	}
	if c.CheckpointBytes <= 0 {
		c.CheckpointBytes = ProgressCheckpointBytes
	}
	if c.MaxRetainedTransfers <= 0 {
		c.MaxRetainedTransfers = 1024
	}
}

// AssetDownloadManager is the canonical public surface every consumer (the
// worker adapter today, renderers tomorrow) uses to obtain a verified local
// asset path.
type AssetDownloadManager interface {
	Resolve(ctx context.Context, request DownloadRequest) (DownloadedAsset, error)
	Snapshot(assetKey assetref.AssetKey) (DownloadSnapshot, bool)
	Subscribe(assetKey assetref.AssetKey) (<-chan DownloadSnapshot, func())
	JobSnapshot(jobID string) JobDownloadSnapshot
	LatestOperational() OperationalSnapshot
}

// Manager implements AssetDownloadManager.
type Manager struct {
	cfg        Config
	transferer Transferer
	registry   *TransferRegistry
	sched      *scheduler

	ctx    context.Context
	cancel context.CancelFunc

	qseq       atomic.Int64
	generation atomic.Int64
	waiterSeq  atomic.Uint64
	coalesced  atomic.Uint64

	// lastOperational caches the most recent refreshOperational projection so
	// heartbeat/telemetry consumers can read the current queue depth without
	// registering as a snapshot subscriber (telemetry_snapshot.go in the
	// worker agent reads it for WorkerTelemetrySnapshot.DownloadQueue).
	lastOperational atomic.Pointer[OperationalSnapshot]
}

// NewManager wires the manager with its transferer and starts the bounded
// pool. Callers must Close() the manager when the worker shuts down.
func NewManager(cfg Config, transferer Transferer) *Manager {
	cfg.withDefaults()
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		cfg:        cfg,
		transferer: transferer,
		registry:   newTransferRegistry(),
		sched:      newScheduler(cfg.Concurrency, cfg.Now),
		ctx:        ctx,
		cancel:     cancel,
	}
	m.sched.Start()
	return m
}

// Close cancels every transfer, stops the pool and joins its dispatchers.
// Idempotent. After Close, new transfers settle as cancelled (a cache hit
// still present on disk may still resolve READY via the cache probe).
func (m *Manager) Close() {
	m.cancel()
	m.sched.Close()
}

// Resolve returns the verified local path for the requested asset. Concurrent
// requests for the same AssetKey share one transfer; each request is a
// waiter.
func (m *Manager) Resolve(ctx context.Context, req DownloadRequest) (DownloadedAsset, error) {
	key := req.AssetKey
	if key == "" {
		key = assetref.AssetKey(req.AssetID)
	}
	if key == "" {
		return DownloadedAsset{}, ErrEmptyKey
	}
	req.AssetKey = key

	// A transfer cancelled by a racing "last waiter left" must not poison a
	// caller that just arrived: retry on a fresh transfer.
	for attempt := 0; attempt < 3; attempt++ {
		t, shared := m.acquireTransfer(key, req, ctx)
		if shared {
			m.coalesced.Add(1)
			if m.cfg.OnCoalescedRequest != nil {
				m.cfg.OnCoalescedRequest(req.SizeBytes)
			}
		}

		waiterID := m.waiterSeq.Add(1)
		t.addWaiter(waiterID, req)
		m.refreshOperational()
		select {
		case <-t.doneCh():
			result, err := t.result()
			t.removeWaiter(waiterID)
			if err != nil {
				if errors.Is(err, errTransferCancelled) && ctx.Err() == nil {
					continue
				}
				return DownloadedAsset{}, err
			}
			hit, readyAt := t.outcome()
			return DownloadedAsset{
				AssetKey:  assetref.AssetKey(key),
				AssetID:   req.AssetID,
				LocalPath: result.LocalPath,
				SHA256:    result.SHA256,
				SizeBytes: result.Bytes,
				CacheHit:  hit,
				ReadyAt:   readyAt,
			}, nil

		case <-ctx.Done():
			// The caller (job/task) went away. Remove the waiter; only when it
			// was the LAST waiter may the shared transfer be cancelled.
			if last := t.removeWaiter(waiterID); last {
				t.requestCancel()
			}
			return DownloadedAsset{}, ctx.Err()
		}
	}
	return DownloadedAsset{}, errTransferCancelled
}

// acquireTransfer returns the live transfer for key, creating one (and
// starting its run loop) when absent or when the previous transfer reached a
// terminal state. Terminal transfers stay in the registry for snapshot
// visibility; new Resolve calls re-check the cache cheaply.
func (m *Manager) acquireTransfer(key assetref.AssetKey, req DownloadRequest, reportCtx context.Context) (*Transfer, bool) {
	m.registry.PruneTerminal(m.cfg.MaxRetainedTransfers)
	m.registry.mu.Lock()
	defer m.registry.mu.Unlock()
	if existing := m.registry.transfers[key]; existing != nil && !existing.isTerminal() {
		return existing, true
	}
	generation := m.generation.Add(1)
	t := newTransfer(m.ctx, key, req, reportCtx, m.cfg.Now, nextTransferID(), generation)
	// Wire the progress throttles and the durable checkpoint hook.
	t.publishInterval = m.cfg.PublishInterval
	t.publishBytes = m.cfg.PublishBytes
	t.checkpointInterval = m.cfg.CheckpointInterval
	t.checkpointBytes = m.cfg.CheckpointBytes
	t.onCheckpoint = m.cfg.OnCheckpoint
	t.onOperationalSnapshot = m.refreshOperational
	m.registry.transfers[key] = t
	go t.run(m)
	return t, false
}

// refreshOperational projects the registry into one low-cardinality worker
// snapshot for Prometheus/Grafana. Terminal transfers remain in the registry
// for API visibility; ready/failed/cache-hit gauges therefore describe the
// manager's retained transfer read model, not a short-lived active-only count.
func (m *Manager) refreshOperational() {
	out := m.computeOperational()
	m.lastOperational.Store(&out)
	if m.cfg.OnOperationalSnapshot != nil {
		m.cfg.OnOperationalSnapshot(out)
	}
	m.registry.PruneTerminal(m.cfg.MaxRetainedTransfers)
}

// computeOperational projects the transfer registry into one low-cardinality
// worker snapshot WITHOUT invoking any callback or mutating manager state.
// Shared by refreshOperational (which forwards it to the callback) and
// LatestOperational (which serves a cached copy to telemetry consumers).
func (m *Manager) computeOperational() OperationalSnapshot {
	out := OperationalSnapshot{CoalescedRequestsTotal: m.coalesced.Load()}
	m.registry.Each(func(_ assetref.AssetKey, t *Transfer) {
		snap := t.snapshot(m.cfg.Now())
		out.BytesDownloaded += snap.BytesDownloaded
		out.BytesTotal += snap.BytesTotal
		switch snap.State {
		case DownloadRunning, DownloadVerifying:
			out.ActiveTransfers++
			out.ThroughputBPS += snap.ThroughputBytesPerSecond
			if snap.ETASeconds > out.ETASeconds {
				out.ETASeconds = snap.ETASeconds
			}
		case DownloadQueued:
			out.QueuedTransfers++
		case DownloadReady:
			out.ReadyTransfers++
			if snap.CacheHit {
				out.CacheHitTransfers++
			}
		case DownloadFailed:
			out.FailedTransfers++
		}
	})
	return out
}

// LatestOperational returns the most recent low-cardinality operational
// snapshot computed by refreshOperational, or the zero value when no
// projection has run yet (manager idle since construction). The cached
// snapshot is refreshed on every lifecycle/progress edge, so telemetry
// consumers read a live queue depth without subscribing to updates.
func (m *Manager) LatestOperational() OperationalSnapshot {
	if m == nil {
		return OperationalSnapshot{}
	}
	if p := m.lastOperational.Load(); p != nil {
		return *p
	}
	return OperationalSnapshot{}
}

// Snapshot returns the current snapshot for an asset key.
func (m *Manager) Snapshot(key assetref.AssetKey) (DownloadSnapshot, bool) {
	t := m.registry.Get(key)
	if t == nil {
		return DownloadSnapshot{}, false
	}
	return t.snapshot(m.cfg.Now()), true
}

// Subscribe registers a snapshot subscriber for an asset key. The channel
// delivers every published snapshot (non-blocking, buffered). Returns a nil
// channel when no transfer exists for the key — call Resolve first. The
// returned func unsubscribes.
func (m *Manager) Subscribe(key assetref.AssetKey) (<-chan DownloadSnapshot, func()) {
	t := m.registry.Get(key)
	if t == nil {
		return nil, func() {}
	}
	return t.subscribe()
}

// JobSnapshot aggregates every transfer the job is waiting on, weighted on
// bytes. A job is "waiting" on a transfer when it has a registered waiter.
func (m *Manager) JobSnapshot(jobID string) JobDownloadSnapshot {
	out := JobDownloadSnapshot{JobID: jobID}
	now := m.cfg.Now()
	m.registry.Each(func(_ assetref.AssetKey, t *Transfer) {
		if !t.hasJobReference(jobID) {
			return
		}
		snap := t.snapshot(now)
		out.AssetsTotal++
		out.BytesTotal += snap.BytesTotal
		// In-flight bytes count toward the job: sumDownloadedBytes grows with
		// every transfer, READY assets add their full size (a cache hit
		// downloaded zero but the file is fully available).
		out.BytesDownloaded += snap.BytesDownloaded
		switch snap.State {
		case DownloadReady:
			out.AssetsReady++
			// READY assets are fully available: weight them on their total
			// size regardless of what the transfer happened to download.
			out.BytesDownloaded += snap.BytesTotal - snap.BytesDownloaded
			if snap.CacheHit {
				out.CacheHits++
			} else {
				out.CacheMisses++
			}
		case DownloadQueued:
			out.AssetsQueued++
			out.QueuedTransfers++
		case DownloadRunning:
			out.AssetsDownloading++
			out.ActiveTransfers++
			// Job ETA is the longest pole among active transfers.
			if snap.ETASeconds > out.EstimatedRemainingSeconds {
				out.EstimatedRemainingSeconds = snap.ETASeconds
			}
		case DownloadVerifying:
			out.AssetsVerifying++
			out.ActiveTransfers++
		case DownloadFailed, DownloadCancelled:
			out.AssetsFailed++
		}
	})
	if out.BytesTotal > 0 {
		out.ProgressPercent = float64(out.BytesDownloaded) / float64(out.BytesTotal) * 100
	}
	return out
}

// run drives one transfer: cache check first (CACHE_HIT fast path), then the
// bounded, priority-stable byte transfer.
func (t *Transfer) run(m *Manager) {
	t.setState(DownloadCacheCheck)

	hit, err := m.transferer.Check(t.transferContext(), t.reportContext(), t.req)
	if err != nil {
		if t.transferContext().Err() != nil {
			t.finishCancelled()
			return
		}
		t.finish(TransferResult{}, err)
		return
	}
	if hit.CacheHit {
		// A cache hit downloaded zero bytes: the verified file was already
		// on disk. The size is known from the request (BytesTotal), but
		// BytesDownloaded must stay 0 per the plan contract.
		t.setCacheHit(true)
		t.setState(DownloadCacheHit)
		// CACHE_HIT is itself an immediate terminal cache event; persist it
		// before the subsequent READY transition.
		t.emitCheckpoint(t.now())
		t.finish(TransferResult{LocalPath: hit.LocalPath, SHA256: hit.SHA256}, nil)
		return
	}

	// Miss: the byte transfer goes through the bounded, stable queue.
	t.setQueuePos(m.qseq.Add(1))
	if !m.sched.Enqueue(t.Key, t.req.Priority, t.queuedAtLocked(), func() { t.scheduled(m) }) {
		// The pool closed between the cache probe and the enqueue: settle the
		// transfer as cancelled so no waiter hangs on a never-run transfer.
		t.finishCancelled()
		return
	}
}

// scheduled runs on a scheduler dispatcher goroutine once a slot is free.
func (t *Transfer) scheduled(m *Manager) {
	if t.transferContext().Err() != nil {
		t.finishCancelled()
		return
	}
	t.setDownloading()
	result, err := m.transferer.Transfer(t.transferContext(), t.reportContext(), t.req, t.reportProgress)
	if err != nil {
		if t.transferContext().Err() != nil {
			t.finishCancelled()
			return
		}
		t.finish(TransferResult{}, err)
		return
	}
	// Verification completed inside the transferer; VERIFYING → READY keeps
	// the observable transition faithful before the terminal snapshot.
	t.setState(DownloadVerifying)
	t.finish(result, nil)
}

// requestCancel cancels the transfer context if the transfer is still live.
// A finished transfer ignores the request.
func (t *Transfer) requestCancel() {
	t.mu.Lock()
	terminal := t.state.Terminal()
	t.mu.Unlock()
	if !terminal {
		t.cancel()
	}
}

// outcome reads the cache-hit flag and completion time after done is closed.
func (t *Transfer) outcome() (cacheHit bool, completedAt time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.cacheHit, t.completedAt
}

// hasWaiter reports whether the transfer currently has a waiter for jobID.
func (t *Transfer) hasWaiter(jobID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	// Active waiter identity is intentionally opaque; job membership is
	// represented by the durable jobRefs map below.
	for _, ref := range t.jobRefs {
		if ref.JobID == jobID {
			return true
		}
	}
	return false
}

// queuedAtLocked returns the queued timestamp (for the stable queue ordering).
func (t *Transfer) queuedAtLocked() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.queuedAt
}

var transferSeq atomic.Uint64

func nextTransferID() string {
	return fmt.Sprintf("xfer-%d", transferSeq.Add(1))
}

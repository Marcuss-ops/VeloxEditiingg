package prefetch

// Scheduler is the execution layer for FutureAssetPlan. It deliberately
// depends on the canonical CacheResolver: prefetch has no cache, transfer,
// hash verifier, or eviction implementation of its own.

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"velox-shared/futureasset"
	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/internal/workercache"
)

const (
	PriorityForeground = 1000
	PriorityPrefetchD1 = 300
	PriorityPrefetchD2 = 200
	PriorityPrefetchD3 = 100
)

// NetworkPacer is the interface for shared bandwidth admission. The prefetch
// scheduler delegates byte pacing to this interface when available, replacing
// the local MaxBandwidthBytesPerSecond per-request cap. The implementation
// (worker.NetworkAdmissionController) handles work-conserving priority across
// publish (P0), runtime (P1), and prefetch (P2) consumers.
type NetworkPacer interface {
	// AcquireBytes blocks until the controller permits n bytes to transfer
	// in the given direction and priority. Returns ctx.Err() on cancellation.
	AcquireBytes(ctx context.Context, dir int, priority int, n int64) error
	// BeginTransfer increments the active-transfer counter.
	BeginTransfer(priority int)
	// ReleaseBytes decrements the active-transfer counter and records consumed bytes.
	ReleaseBytes(priority int)
	// RecordBytes records consumed bytes for metrics.
	RecordBytes(priority int, dir int, n int64)
	// IsPrefetchThrottled returns true when NIC saturation exceeds the
	// throttle threshold and prefetch admission should be rejected.
	IsPrefetchThrottled() bool
}

// AdmissionCategory identifies the resource category for admission control.
type AdmissionCategory int

const (
	AdmissionPrefetch AdmissionCategory = iota
	AdmissionPublish
	AdmissionRender
)

// AdmissionDecision is the result of an admission check.
type AdmissionDecision int

const (
	AdmissionAdmit AdmissionDecision = iota
	AdmissionRejectMemory
	AdmissionRejectStopped
)

// ResourceAdmissionController is the interface for RSS-based admission control.
// The prefetch scheduler checks this before each download to prevent OOM
// under memory pressure. The implementation lives in the worker package
// (ResourceAdmissionController) to avoid circular imports.
type ResourceAdmissionController interface {
	// CanAdmit checks whether a resource claim can be admitted given current
	// RSS pressure. Returns Admit or RejectMemory/RejectStopped.
	CanAdmit(category AdmissionCategory) AdmissionDecision
	// RecordAdmissionResult updates hysteresis state after an operation
	// completes (success or failure).
	RecordAdmissionResult(category AdmissionCategory, admitted bool)
}

// Network direction and priority constants matching worker.NetDir* and
// worker.NetPriority* values. Defined here to avoid a circular import
// between the prefetch and worker packages.
const (
	NetDirIngress       = 0 // download
	NetDirEgress        = 1 // upload
	NetPriorityPublish  = 0
	NetPriorityRuntime  = 1
	NetPriorityPrefetch = 2
)

type Config struct {
	WorkerID                   string
	MaxConcurrent              int
	ByteBudget                 int64
	MaxBandwidthBytesPerSecond int64
	DiskRestrictedPercent      int
	DiskCriticalPercent        int
	DiskRecoveryPercent        int
	DiskUsagePercent           func() int
	Now                        func() time.Time
	OnState                    func(string, futureasset.Job, futureasset.AssetManifest, error)
	// MetadataResolver runs after a verified cache hit/download and before
	// an asset is considered prepared. It must not mutate the cache path.
	MetadataResolver MetadataResolver
	// OnPrepared receives the aggregate PREPARED transition for a job.
	OnPrepared            func(PreparedJob)
	RAM                   *RAMCache
	RAMMinFutureRefs      int
	RAMMaxNextUseDistance int
	OnEvent               func(Event)

	// NetworkPacer is the optional shared bandwidth admission controller.
	// When non-nil, the scheduler delegates byte pacing to the shared
	// controller instead of using the local MaxBandwidthBytesPerSecond
	// per-request cap. The controller handles work-conserving priority
	// across publish (P0), runtime (P1), and prefetch (P2) consumers.
	NetworkPacer NetworkPacer

	// AdmissionController is the optional RSS-based admission controller.
	// When non-nil, the scheduler checks CanAdmit(AdmissionPrefetch)
	// before each download and calls RecordAdmissionResult after.
	// This prevents OOM when RSS exceeds 80% of total RAM.
	AdmissionController ResourceAdmissionController
}

type jobRuntime struct {
	job        futureasset.Job
	ctx        context.Context
	cancel     context.CancelFunc
	generation uint64
}

// Event is a low-cardinality timing hook for the prefetch waterfall. Job and
// asset identifiers are available to structured-log consumers through the
// callback, but are intentionally not metric labels.
type Event struct {
	Name          string
	At            time.Time
	PlanVersion   uint64
	PlanID        string
	JobID         string
	TaskID        string
	AssetKey      string
	Distance      int
	Generation    uint64
	QueuedAt      time.Time
	StartedAt     time.Time
	ReadyAt       time.Time
	QueueDepth    int
	Active        int
	CacheHit      bool
	DownloadBytes int64
	ErrorMessage  string
}

type workItem struct {
	planVersion uint64
	generation  uint64
	job         futureasset.Job
	asset       futureasset.AssetManifest
	ctx         context.Context
	enqueuedAt  time.Time
	sequence    uint64
	index       int
}

type readyRecord struct {
	at       time.Time
	distance int
}

type workQueue []*workItem

func (q workQueue) Len() int { return len(q) }
func (q workQueue) Less(i, j int) bool {
	if q[i].job.Distance != q[j].job.Distance {
		return q[i].job.Distance < q[j].job.Distance
	}
	if !q[i].enqueuedAt.Equal(q[j].enqueuedAt) {
		return q[i].enqueuedAt.Before(q[j].enqueuedAt)
	}
	if q[i].asset.AssetKey != q[j].asset.AssetKey {
		return q[i].asset.AssetKey < q[j].asset.AssetKey
	}
	return q[i].sequence < q[j].sequence
}
func (q workQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].index = i
	q[j].index = j
}
func (q *workQueue) Push(value interface{}) {
	item := value.(*workItem)
	item.index = len(*q)
	*q = append(*q, item)
}
func (q *workQueue) Pop() interface{} {
	old := *q
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*q = old[:n-1]
	return item
}

type Scheduler struct {
	mu              sync.Mutex
	cfg             Config
	resolver        *downloader.CacheResolver
	protect         workercache.LeaseReservationStore
	ram             *RAMCache
	hints           map[string]futureasset.ProtectedAsset
	prefetched      map[string]int64
	useful          map[string]bool
	assetJobs       map[string]map[string]struct{}
	jobs            map[string]*jobRuntime
	protects        map[string]string
	pendingProtects map[string]struct{}
	protectExpiries map[string]time.Time
	// executionReservations tracks execution-phase pins by asset and owning
	// job. Multiple concurrent jobs may share one cached asset; cleanup must
	// release only the caller's reservation, never another job's pin.
	executionReservations map[string]map[string]string
	bytes                 int64
	state                 diskPressureState
	queue                 workQueue
	nextSequence          uint64
	wake                  chan struct{}
	workerCtx             context.Context
	workerCancel          context.CancelFunc
	activePrefetch        int
	readyAtByJob          map[string]map[string]readyRecord
	prepared              map[string]PreparedJob
	// currentPlanID and currentPlanVersion track the most recently
	// reconciled plan so PreparedJob entries can carry the plan identity
	// for complete lifecycle event correlation.
	currentPlanID      string
	currentPlanVersion uint64
}

type diskPressureState uint8

const (
	diskNormal diskPressureState = iota
	diskRestricted
	diskCritical
)

func NewScheduler(cfg Config) *Scheduler {
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = 2
	}
	if cfg.ByteBudget <= 0 {
		cfg.ByteBudget = 20 * 1024 * 1024 * 1024
	}
	if cfg.DiskRestrictedPercent <= 0 {
		cfg.DiskRestrictedPercent = 70
	}
	if cfg.DiskCriticalPercent <= 0 {
		cfg.DiskCriticalPercent = 85
	}
	if cfg.DiskRecoveryPercent <= 0 {
		cfg.DiskRecoveryPercent = 75
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.RAMMinFutureRefs <= 0 {
		cfg.RAMMinFutureRefs = 2
	}
	if cfg.RAMMaxNextUseDistance <= 0 {
		cfg.RAMMaxNextUseDistance = 3
	}
	if cfg.MetadataResolver == nil {
		cfg.MetadataResolver = defaultMetadataResolver
	}
	workerCtx, workerCancel := context.WithCancel(context.Background())
	s := &Scheduler{cfg: cfg, ram: cfg.RAM, jobs: make(map[string]*jobRuntime), protects: make(map[string]string), pendingProtects: make(map[string]struct{}), protectExpiries: make(map[string]time.Time), executionReservations: make(map[string]map[string]string), hints: make(map[string]futureasset.ProtectedAsset), prefetched: make(map[string]int64), useful: make(map[string]bool), assetJobs: make(map[string]map[string]struct{}), wake: make(chan struct{}, 1), workerCtx: workerCtx, workerCancel: workerCancel, readyAtByJob: make(map[string]map[string]readyRecord), prepared: make(map[string]PreparedJob)}
	heap.Init(&s.queue)
	for i := 0; i < cfg.MaxConcurrent; i++ {
		go s.runWorker()
	}
	return s
}

func (s *Scheduler) SetResolver(r *downloader.CacheResolver) {
	s.mu.Lock()
	s.resolver = r
	s.mu.Unlock()
	s.signalWork()
}

// Close stops scheduler workers and cancels outstanding prefetch waiters.
// The production worker owns the scheduler lifetime; tests and benchmarks
// can use this to avoid leaking dispatcher goroutines between cases.
func (s *Scheduler) Close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	for id, runtime := range s.jobs {
		runtime.cancel()
		delete(s.jobs, id)
	}
	s.queue = nil
	s.workerCancel()
	s.mu.Unlock()
	s.signalWork()
}

func (s *Scheduler) RAMCache() *RAMCache {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ram
}
func (s *Scheduler) SetProtectionStore(store workercache.LeaseReservationStore) {
	s.mu.Lock()
	s.protect = store
	s.mu.Unlock()
}

func (s *Scheduler) SetDiskUsagePercent(fn func() int) {
	if s != nil {
		s.mu.Lock()
		s.cfg.DiskUsagePercent = fn
		s.mu.Unlock()
	}
}

// SetOnPrepared replaces the OnPrepared callback after construction.
// Used by the worker to wire the lifecycle event sender after the
// Worker struct is fully initialized.
func (s *Scheduler) SetOnPrepared(fn func(PreparedJob)) {
	if s != nil {
		s.mu.Lock()
		s.cfg.OnPrepared = fn
		s.mu.Unlock()
	}
}

// RecordPlanEvent lets the receive path put control-plane timestamps in the
// same structured event stream as queue, download, and READY timestamps.
func (s *Scheduler) RecordPlanEvent(name string, planVersion uint64, planID string) {
	if s == nil {
		return
	}
	s.emit(Event{Name: name, At: s.cfg.Now(), PlanVersion: planVersion, PlanID: planID})
}

func (s *Scheduler) currentItemLocked(item *workItem) bool {
	runtime, ok := s.jobs[item.job.JobID]
	return ok && runtime.generation == item.generation && runtime.ctx.Err() == nil
}

func (s *Scheduler) currentItem(item *workItem) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentItemLocked(item)
}

func (s *Scheduler) signalWork() {
	if s == nil || s.wake == nil {
		return
	}
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Scheduler) emit(event Event) {
	if s != nil && s.cfg.OnEvent != nil {
		s.cfg.OnEvent(event)
	}
}

func priorityForDistance(distance int) int {
	switch distance {
	case 1:
		return PriorityPrefetchD1
	case 2:
		return PriorityPrefetchD2
	default:
		return PriorityPrefetchD3
	}
}

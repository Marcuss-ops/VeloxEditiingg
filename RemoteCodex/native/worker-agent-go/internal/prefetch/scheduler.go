package prefetch

// Scheduler is the execution layer for FutureAssetPlan. It deliberately
// depends on the canonical CacheResolver: prefetch has no cache, transfer,
// hash verifier, or eviction implementation of its own.

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"time"

	"velox-shared/assetref"
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

// ReleaseAllExecutionReservations releases every execution-phase pin.
// Intended for graceful shutdown; production callers should prefer
// ReleaseExecutionReservations(jobID) for targeted cleanup.
func (s *Scheduler) ReleaseAllExecutionReservations() {
	if s == nil {
		return
	}
	s.mu.Lock()
	store := s.protect
	execs := s.executionReservations
	s.executionReservations = make(map[string]map[string]string)
	s.mu.Unlock()
	if store == nil {
		return
	}
	for assetKey, reservations := range execs {
		for _, execID := range reservations {
			_ = store.ReleaseReservation(context.Background(), assetref.AssetKey(assetKey), execID)
		}
	}
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

// MarkJobStarted closes the READY -> job-start interval for assets that were
// prefetched for this job. Positive lead means READY happened first; a
// negative lead identifies a foreground catch-up. It also triggers the
// atomic handoff from future reservation to execution reservation for
// every prefetched asset, ensuring eviction cannot reclaim an asset
// between PREPARED and render.
func (s *Scheduler) MarkJobStarted(jobID string) {
	if s == nil || jobID == "" {
		return
	}
	startedAt := s.cfg.Now()
	s.mu.Lock()
	ready := s.readyAtByJob[jobID]
	delete(s.readyAtByJob, jobID)
	job, hasJob := s.jobs[jobID]
	prepared := s.prepared[jobID]
	s.mu.Unlock()
	for assetKey, record := range ready {
		s.emit(Event{Name: "prefetch_ready_lead", At: startedAt, JobID: jobID, AssetKey: assetKey, Distance: record.distance, StartedAt: startedAt, ReadyAt: record.at})
	}
	// Handoff: for each prepared asset, install an execution reservation
	// BEFORE releasing the future reservation. The rule is strict:
	//   reserve execution pin → confirm → release future pin
	//   never the reverse.
	if hasJob && prepared.State == PreparationStatePrepared {
		s.handoffToExecutionLocked(job, prepared)
	}
}

// HandoffToExecution installs execution-phase reservation pins for a job's
// prefetched assets and releases the corresponding future pins atomically.
// The handoff rule is: reserve execution → confirm → release future.
// This ensures eviction never sees a protection gap between the future
// plan expiry and the render's lease acquisition.
func (s *Scheduler) HandoffToExecution(jobID, attemptID string) {
	if s == nil || jobID == "" || attemptID == "" {
		return
	}
	s.mu.Lock()
	job, hasJob := s.jobs[jobID]
	prepared := s.prepared[jobID]
	s.mu.Unlock()
	if hasJob && prepared.State == PreparationStatePrepared {
		s.handoffToExecutionLocked(job, prepared)
	}
}

// handoffToExecutionLocked performs the atomic reservation handoff for every
// prepared asset in the job. It reserves the execution pin first, confirms
// success, then releases the future pin. Caller holds s.mu or has snapshot.
// The store I/O happens outside the lock so SQLite writes never stall the
// control loop.
func (s *Scheduler) handoffToExecutionLocked(job *jobRuntime, prepared PreparedJob) {
	store := s.protect
	if store == nil {
		return
	}
	// Snapshot the future reservations and prepared assets outside the lock
	// for durable I/O.
	assetKeys := make([]string, 0, len(prepared.Assets))
	futureReservationIDs := make(map[string]string, len(prepared.Assets))
	futureExpiries := make(map[string]time.Time, len(prepared.Assets))
	s.mu.Lock()
	for assetKey := range prepared.Assets {
		if futureResID, ok := s.protects[assetKey]; ok {
			assetKeys = append(assetKeys, assetKey)
			futureReservationIDs[assetKey] = futureResID
			futureExpiries[assetKey] = s.protectExpiries[assetKey]
		}
	}
	attemptID := job.job.ReservationID
	if attemptID == "" {
		attemptID = job.job.JobID
	}
	s.mu.Unlock()
	if len(assetKeys) == 0 {
		return
	}
	// Phase 1: install execution reservations for all prepared assets.
	executionReservationIDs := make(map[string]string, len(assetKeys))
	for _, assetKey := range assetKeys {
		execID := fmt.Sprintf("execution:%s:%s", attemptID, assetKey)
		expiresAt := futureExpiries[assetKey]
		if expiresAt.IsZero() {
			expiresAt = s.cfg.Now().Add(time.Hour)
		}
		if err := store.Reserve(context.Background(), assetref.AssetKey(assetKey), execID, expiresAt); err != nil {
			s.emit(Event{Name: "execution_reservation_failed", At: s.cfg.Now(), JobID: job.job.JobID, AssetKey: assetKey, ErrorMessage: err.Error()})
			continue
		}
		executionReservationIDs[assetKey] = execID
	}
	// Phase 2: install execution reservations in the projection and release
	// future reservations for assets that were successfully pinned.
	s.mu.Lock()
	for assetKey, execID := range executionReservationIDs {
		if s.executionReservations[assetKey] == nil {
			s.executionReservations[assetKey] = make(map[string]string)
		}
		s.executionReservations[assetKey][job.job.JobID] = execID
	}
	for _, assetKey := range assetKeys {
		execID, ok := executionReservationIDs[assetKey]
		if !ok {
			continue
		}
		futureID := futureReservationIDs[assetKey]
		if futureID == "" {
			continue
		}
		// Release future reservation outside the lock.
		s.mu.Unlock()
		_ = store.ReleaseReservation(context.Background(), assetref.AssetKey(assetKey), futureID)
		s.mu.Lock()
		// Remove from the future projection so cleanup doesn't double-release.
		if s.protects[assetKey] == futureID {
			delete(s.protects, assetKey)
		}
		_ = execID // already installed
	}
	s.mu.Unlock()
	s.emit(Event{Name: "execution_reservation_handoff", At: s.cfg.Now(), JobID: job.job.JobID, TaskID: job.job.TaskID})
}

// ReleaseExecutionReservations removes execution-phase pins for a job's assets
// after the render completes. This should be called during task cleanup.
func (s *Scheduler) ReleaseExecutionReservations(jobID string) {
	if s == nil || jobID == "" {
		return
	}
	s.mu.Lock()
	store := s.protect
	execs := make(map[string]string)
	for assetKey, reservations := range s.executionReservations {
		if execID, ok := reservations[jobID]; ok {
			execs[assetKey] = execID
			delete(reservations, jobID)
			if len(reservations) == 0 {
				delete(s.executionReservations, assetKey)
			}
		}
	}
	s.mu.Unlock()
	if store == nil {
		return
	}
	for assetKey, execID := range execs {
		_ = store.ReleaseReservation(context.Background(), assetref.AssetKey(assetKey), execID)
	}
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

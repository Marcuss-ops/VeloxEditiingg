package prefetch

// Scheduler is the execution layer for FutureAssetPlan. It deliberately
// depends on the canonical CacheResolver: prefetch has no cache, transfer,
// hash verifier, or eviction implementation of its own.

import (
	"container/heap"
	"context"
	"errors"
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
	RAM                        *RAMCache
	RAMMinFutureRefs           int
	RAMMaxNextUseDistance      int
	OnEvent                    func(Event)
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
	Name         string
	At           time.Time
	PlanVersion  uint64
	PlanID       string
	JobID        string
	TaskID       string
	AssetKey     string
	Distance     int
	Generation   uint64
	QueuedAt     time.Time
	StartedAt    time.Time
	ReadyAt      time.Time
	QueueDepth   int
	Active       int
	ErrorMessage string
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
	bytes           int64
	state           diskPressureState
	queue           workQueue
	nextSequence    uint64
	wake            chan struct{}
	workerCtx       context.Context
	workerCancel    context.CancelFunc
	activePrefetch  int
	readyAtByJob    map[string]map[string]readyRecord
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
	workerCtx, workerCancel := context.WithCancel(context.Background())
	s := &Scheduler{cfg: cfg, ram: cfg.RAM, jobs: make(map[string]*jobRuntime), protects: make(map[string]string), pendingProtects: make(map[string]struct{}), protectExpiries: make(map[string]time.Time), hints: make(map[string]futureasset.ProtectedAsset), prefetched: make(map[string]int64), useful: make(map[string]bool), assetJobs: make(map[string]map[string]struct{}), wake: make(chan struct{}, 1), workerCtx: workerCtx, workerCancel: workerCancel, readyAtByJob: make(map[string]map[string]readyRecord)}
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

// Reconcile applies a complete snapshot. New reservations are installed
// before old ones are released, so eviction never sees a protection gap.
func (s *Scheduler) Reconcile(plan futureasset.Plan) error {
	if s == nil {
		return fmt.Errorf("prefetch: nil scheduler")
	}
	if err := plan.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	if plan.Expired(s.cfg.Now()) {
		for id, runtime := range s.jobs {
			runtime.cancel()
			delete(s.jobs, id)
			s.detachJobLocked(runtime.job)
		}
		s.mu.Unlock()
		s.signalWork()
		return nil
	}
	for id, runtime := range s.jobs {
		if _, ok := findScheduledJob(plan.PrefetchJobs, id); !ok {
			runtime.cancel()
			delete(s.jobs, id)
			s.detachJobLocked(runtime.job)
		}
	}
	store := s.protect
	oldProtects := s.protects
	newProtects := make(map[string]string, len(plan.Protect))
	pendingProtects := make(map[string]struct{})
	protectExpiries := make(map[string]time.Time, len(plan.Protect))
	s.hints = make(map[string]futureasset.ProtectedAsset, len(plan.Protect))
	for _, asset := range plan.Protect {
		s.hints[asset.AssetKey] = asset
		reservationID := fmt.Sprintf("future:%s:%s", s.cfg.WorkerID, asset.AssetKey)
		newProtects[asset.AssetKey] = reservationID
		protectExpiries[asset.AssetKey] = plan.ExpiresAt
		if store != nil {
			// Reserve before releasing the prior snapshot's reservation.
			if err := store.Reserve(context.Background(), assetref.AssetKey(asset.AssetKey), reservationID, plan.ExpiresAt); err != nil {
				// A future asset is allowed to be absent until its prefetch
				// resolver creates the verified canonical-cache row. Protection
				// is an eviction barrier, not a prerequisite for downloading.
				// Keep the desired reservation pending and install it after the
				// resolver returns READY. Other store failures remain fail-closed.
				if errors.Is(err, workercache.ErrNotFound) {
					pendingProtects[asset.AssetKey] = struct{}{}
				} else {
					s.mu.Unlock()
					return fmt.Errorf("prefetch: protect %s: %w", asset.AssetKey, err)
				}
			}
		}
	}
	if store != nil {
		for key, reservationID := range oldProtects {
			if _, keep := newProtects[key]; !keep {
				_ = store.ReleaseReservation(context.Background(), assetref.AssetKey(key), reservationID)
			}
		}
	}
	s.protects = newProtects
	s.pendingProtects = pendingProtects
	s.protectExpiries = protectExpiries
	var events []Event
	for _, job := range plan.PrefetchJobs {
		if runtime, exists := s.jobs[job.JobID]; exists {
			if sameScheduledJob(runtime.job, job) {
				continue
			}
			runtime.job = job
			runtime.generation++
			events = append(events, s.enqueueJobLocked(plan.Version, runtime)...)
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		runtime := &jobRuntime{job: job, ctx: ctx, cancel: cancel, generation: 1}
		s.jobs[job.JobID] = runtime
		events = append(events, s.enqueueJobLocked(plan.Version, runtime)...)
	}
	s.mu.Unlock()
	for _, event := range events {
		s.emit(event)
	}
	s.signalWork()
	return nil
}

// Cancel removes only the prefetch job's waiters. The downloader remains the
// owner of shared transfers and cancels a transfer only after its last waiter.
func (s *Scheduler) Cancel(jobID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	runtime, ok := s.jobs[jobID]
	if !ok {
		return false
	}
	runtime.cancel()
	delete(s.jobs, jobID)
	s.detachJobLocked(runtime.job)
	s.signalWork()
	return true
}

func (s *Scheduler) runWorker() {
	for {
		item, resolver := s.nextWorkItem()
		if item != nil && resolver != nil {
			s.runWorkItem(item, resolver)
			continue
		}
		select {
		case <-s.workerCtx.Done():
			return
		case <-s.wake:
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func (s *Scheduler) nextWorkItem() (*workItem, *downloader.CacheResolver) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for attempts := s.queue.Len(); attempts > 0; attempts-- {
		item := heap.Pop(&s.queue).(*workItem)
		if !s.currentItemLocked(item) {
			continue
		}
		state := s.diskStateLocked()
		if state == diskCritical || (state == diskRestricted && item.job.Distance != 1) {
			heap.Push(&s.queue, item)
			continue
		}
		if s.resolver == nil {
			heap.Push(&s.queue, item)
			return nil, nil
		}
		if item.asset.SizeBytes < 0 || (s.bytes > 0 && s.bytes+item.asset.SizeBytes > s.cfg.ByteBudget) {
			heap.Push(&s.queue, item)
			continue
		}
		s.bytes += item.asset.SizeBytes
		s.activePrefetch++
		return item, s.resolver
	}
	return nil, nil
}

func (s *Scheduler) runWorkItem(item *workItem, resolver *downloader.CacheResolver) {
	job, asset := item.job, item.asset
	startedAt := s.cfg.Now()
	bandwidth := s.cfg.MaxBandwidthBytesPerSecond
	if bandwidth > 0 {
		bandwidth /= int64(s.cfg.MaxConcurrent)
		if bandwidth == 0 {
			bandwidth = 1
		}
	}
	request := downloader.DownloadRequest{
		JobID: job.JobID, TaskID: job.TaskID, AssetKey: assetref.AssetKey(asset.AssetKey), AssetID: asset.AssetID,
		Role: downloader.RoleFromString(asset.Role), Source: "master_asset_bridge",
		SHA256: assetref.ContentHash(asset.SHA256), SizeBytes: asset.SizeBytes, MIMEType: asset.MIMEType,
		Priority:                   priorityForDistance(job.Distance),
		MaxBandwidthBytesPerSecond: bandwidth,
	}
	s.mu.Lock()
	active, queueDepth := s.activePrefetch, s.queue.Len()
	s.mu.Unlock()
	s.emit(Event{Name: "download_started", At: startedAt, PlanVersion: item.planVersion, JobID: job.JobID, TaskID: job.TaskID, AssetKey: asset.AssetKey, Distance: job.Distance, Generation: item.generation, QueuedAt: item.enqueuedAt, StartedAt: startedAt, QueueDepth: queueDepth, Active: active})
	if s.cfg.OnState != nil {
		s.cfg.OnState("requested", job, asset, nil)
	}
	resolved, err := resolver.Resolve(item.ctx, request)
	if err == nil {
		// The canonical transferer commits the verified cache row before
		// Resolve returns. Install a protection that was pending because
		// the row did not exist when the plan arrived.
		_ = s.installPendingProtection(asset.AssetKey)
	}
	if err == nil && !resolved.CacheHit {
		s.mu.Lock()
		s.prefetched[asset.AssetKey] = asset.SizeBytes
		if s.assetJobs[asset.AssetKey] == nil {
			s.assetJobs[asset.AssetKey] = make(map[string]struct{})
		}
		s.assetJobs[asset.AssetKey][job.JobID] = struct{}{}
		s.mu.Unlock()
		if s.cfg.OnState != nil {
			s.cfg.OnState("downloaded", job, asset, nil)
		}
	}
	if err == nil && s.ram != nil {
		s.mu.Lock()
		hint, hinted := s.hints[asset.AssetKey]
		s.mu.Unlock()
		if hinted && hint.FutureRefCount >= s.cfg.RAMMinFutureRefs && hint.NextUseDistance <= s.cfg.RAMMaxNextUseDistance {
			_ = s.ram.Put(item.ctx, request, downloader.DownloadedAsset{AssetKey: request.AssetKey, AssetID: request.AssetID, LocalPath: resolved.LocalPath, SHA256: resolved.SHA256, SizeBytes: asset.SizeBytes})
		}
	}
	s.releaseWork(asset.SizeBytes)
	readyAt := s.cfg.Now()
	if err == nil {
		s.mu.Lock()
		if s.readyAtByJob[job.JobID] == nil {
			s.readyAtByJob[job.JobID] = make(map[string]readyRecord)
		}
		s.readyAtByJob[job.JobID][asset.AssetKey] = readyRecord{at: readyAt, distance: job.Distance}
		s.mu.Unlock()
	}
	if s.currentItem(item) {
		if s.cfg.OnState != nil {
			if err != nil {
				s.cfg.OnState("failed", job, asset, err)
			} else {
				s.cfg.OnState("ready", job, asset, nil)
			}
		}
		s.mu.Lock()
		active, queueDepth := s.activePrefetch, s.queue.Len()
		s.mu.Unlock()
		event := Event{Name: "asset_ready", At: readyAt, PlanVersion: item.planVersion, JobID: job.JobID, TaskID: job.TaskID, AssetKey: asset.AssetKey, Distance: job.Distance, Generation: item.generation, QueuedAt: item.enqueuedAt, StartedAt: startedAt, ReadyAt: readyAt, QueueDepth: queueDepth, Active: active}
		if err != nil {
			event.ErrorMessage = err.Error()
		}
		s.emit(event)
	}
}

func (s *Scheduler) enqueueJobLocked(planVersion uint64, runtime *jobRuntime) []Event {
	events := make([]Event, 0, len(runtime.job.Assets))
	for _, asset := range runtime.job.Assets {
		enqueuedAt := s.cfg.Now()
		s.nextSequence++
		heap.Push(&s.queue, &workItem{
			planVersion: planVersion,
			generation:  runtime.generation,
			job:         runtime.job,
			asset:       asset,
			ctx:         runtime.ctx,
			enqueuedAt:  enqueuedAt,
			sequence:    s.nextSequence,
		})
		events = append(events, Event{
			Name:        "prefetch_queued",
			At:          enqueuedAt,
			PlanVersion: planVersion,
			JobID:       runtime.job.JobID,
			TaskID:      runtime.job.TaskID,
			AssetKey:    asset.AssetKey,
			Distance:    runtime.job.Distance,
			Generation:  runtime.generation,
			QueuedAt:    enqueuedAt,
			QueueDepth:  s.queue.Len(),
			Active:      s.activePrefetch,
		})
	}
	return events
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
// negative lead identifies a foreground catch-up.
func (s *Scheduler) MarkJobStarted(jobID string) {
	if s == nil || jobID == "" {
		return
	}
	startedAt := s.cfg.Now()
	s.mu.Lock()
	ready := s.readyAtByJob[jobID]
	delete(s.readyAtByJob, jobID)
	s.mu.Unlock()
	for assetKey, record := range ready {
		s.emit(Event{Name: "prefetch_ready_lead", At: startedAt, JobID: jobID, AssetKey: assetKey, Distance: record.distance, StartedAt: startedAt, ReadyAt: record.at})
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

func sameScheduledJob(a, b futureasset.Job) bool {
	if a.JobID != b.JobID || a.TaskID != b.TaskID || a.ReservationID != b.ReservationID || a.TaskRevision != b.TaskRevision || a.Distance != b.Distance || len(a.Assets) != len(b.Assets) {
		return false
	}
	for i := range a.Assets {
		if a.Assets[i] != b.Assets[i] {
			return false
		}
	}
	return true
}

// installPendingProtection closes the intentional gap between receiving a
// plan and the first verified download of an asset. A missing row at plan
// time is normal; once Resolve succeeds the canonical cache owns the row and
// can accept the reservation. The desired reservation is checked again under
// the scheduler lock so a newer plan cannot be resurrected by an old job.
func (s *Scheduler) installPendingProtection(assetKey string) error {
	s.mu.Lock()
	if _, pending := s.pendingProtects[assetKey]; !pending {
		s.mu.Unlock()
		return nil
	}
	reservationID, desired := s.protects[assetKey]
	expiresAt := s.protectExpiries[assetKey]
	store := s.protect
	s.mu.Unlock()
	if !desired || store == nil {
		return nil
	}
	if err := store.Reserve(context.Background(), assetref.AssetKey(assetKey), reservationID, expiresAt); err != nil {
		if errors.Is(err, workercache.ErrNotFound) {
			return nil
		}
		return err
	}
	s.mu.Lock()
	if current, ok := s.protects[assetKey]; ok && current == reservationID {
		delete(s.pendingProtects, assetKey)
	}
	s.mu.Unlock()
	return nil
}

func (s *Scheduler) MarkForegroundUse(key assetref.AssetKey) {
	if s == nil {
		return
	}
	s.mu.Lock()
	if _, prefetched := s.prefetched[string(key)]; !prefetched || s.useful[string(key)] {
		s.mu.Unlock()
		return
	}
	s.useful[string(key)] = true
	s.mu.Unlock()
	if s.cfg.OnState != nil {
		s.cfg.OnState("useful", futureasset.Job{}, futureasset.AssetManifest{AssetKey: string(key)}, nil)
	}
}

func (s *Scheduler) detachJobLocked(job futureasset.Job) {
	for _, asset := range job.Assets {
		refs := s.assetJobs[asset.AssetKey]
		delete(refs, job.JobID)
		if len(refs) == 0 {
			if bytes, ok := s.prefetched[asset.AssetKey]; ok && !s.useful[asset.AssetKey] && s.cfg.OnState != nil {
				asset.SizeBytes = bytes
				s.cfg.OnState("wasted", job, asset, nil)
			}
			delete(s.assetJobs, asset.AssetKey)
		}
	}
}

func (s *Scheduler) allowed(distance int, _ int64) bool {
	s.mu.Lock()
	state := s.diskStateLocked()
	s.mu.Unlock()
	return state == diskNormal || (state == diskRestricted && distance == 1)
}

func (s *Scheduler) diskStateLocked() diskPressureState {
	if s.cfg.DiskUsagePercent == nil {
		return diskNormal
	}
	usage := s.cfg.DiskUsagePercent()
	switch s.state {
	case diskCritical:
		if usage < s.cfg.DiskRecoveryPercent {
			s.state = diskNormal
		}
	case diskRestricted:
		if usage >= s.cfg.DiskCriticalPercent {
			s.state = diskCritical
		} else if usage < s.cfg.DiskRecoveryPercent {
			s.state = diskNormal
		}
	default:
		if usage >= s.cfg.DiskCriticalPercent {
			s.state = diskCritical
		} else if usage >= s.cfg.DiskRestrictedPercent {
			s.state = diskRestricted
		}
	}
	return s.state
}

func (s *Scheduler) reserveBytes(n int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n < 0 || s.bytes+n > s.cfg.ByteBudget {
		return false
	}
	s.bytes += n
	return true
}
func (s *Scheduler) releaseWork(n int64) {
	s.mu.Lock()
	s.bytes -= n
	if s.bytes < 0 {
		s.bytes = 0
	}
	if s.activePrefetch > 0 {
		s.activePrefetch--
	}
	s.mu.Unlock()
	s.signalWork()
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

func findScheduledJob(jobs []futureasset.Job, id string) (futureasset.Job, bool) {
	for _, job := range jobs {
		if job.JobID == id {
			return job, true
		}
	}
	return futureasset.Job{}, false
}

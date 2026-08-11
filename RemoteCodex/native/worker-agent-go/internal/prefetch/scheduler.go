package prefetch

// Scheduler is the execution layer for FutureAssetPlan. It deliberately
// depends on the canonical CacheResolver: prefetch has no cache, transfer,
// hash verifier, or eviction implementation of its own.

import (
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
}

type jobRuntime struct {
	job    futureasset.Job
	ctx    context.Context
	cancel context.CancelFunc
}

type Scheduler struct {
	mu       sync.Mutex
	cfg      Config
	resolver *downloader.CacheResolver
	protect  workercache.LeaseReservationStore
	ram      *RAMCache
	hints    map[string]futureasset.ProtectedAsset
	jobs     map[string]jobRuntime
	protects map[string]string
	bytes    int64
	sem      chan struct{}
	state    diskPressureState
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
	return &Scheduler{cfg: cfg, ram: cfg.RAM, jobs: make(map[string]jobRuntime), protects: make(map[string]string), hints: make(map[string]futureasset.ProtectedAsset), sem: make(chan struct{}, cfg.MaxConcurrent)}
}

func (s *Scheduler) SetResolver(r *downloader.CacheResolver) {
	s.mu.Lock()
	s.resolver = r
	s.mu.Unlock()
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
		}
		s.mu.Unlock()
		return nil
	}
	for id, runtime := range s.jobs {
		if _, ok := findScheduledJob(plan.PrefetchJobs, id); !ok {
			runtime.cancel()
			delete(s.jobs, id)
		}
	}
	store := s.protect
	oldProtects := s.protects
	newProtects := make(map[string]string, len(plan.Protect))
	s.hints = make(map[string]futureasset.ProtectedAsset, len(plan.Protect))
	for _, asset := range plan.Protect {
		s.hints[asset.AssetKey] = asset
		reservationID := fmt.Sprintf("future:%s:%s", s.cfg.WorkerID, asset.AssetKey)
		newProtects[asset.AssetKey] = reservationID
		if store != nil {
			// Reserve before releasing the prior snapshot's reservation.
			_ = store.Reserve(context.Background(), assetref.AssetKey(asset.AssetKey), reservationID, plan.ExpiresAt)
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
	resolver := s.resolver
	for _, job := range plan.PrefetchJobs {
		if _, exists := s.jobs[job.JobID]; exists {
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		s.jobs[job.JobID] = jobRuntime{job: job, ctx: ctx, cancel: cancel}
		if resolver != nil {
			go s.runJob(ctx, resolver, job)
		}
	}
	s.mu.Unlock()
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
	return true
}

func (s *Scheduler) runJob(ctx context.Context, resolver *downloader.CacheResolver, job futureasset.Job) {
	for _, asset := range job.Assets {
		if ctx.Err() != nil {
			return
		}
		if !s.allowed(job.Distance, asset.SizeBytes) {
			continue
		}
		select {
		case s.sem <- struct{}{}:
		case <-ctx.Done():
			return
		}
		if !s.reserveBytes(asset.SizeBytes) {
			<-s.sem
			continue
		}
		request := downloader.DownloadRequest{
			JobID: job.JobID, TaskID: job.TaskID, AssetKey: assetref.AssetKey(asset.AssetKey), AssetID: asset.AssetID,
			Role: downloader.RoleFromString(asset.Role), Source: "master_asset_bridge",
			SHA256: assetref.ContentHash(asset.SHA256), SizeBytes: asset.SizeBytes, MIMEType: asset.MIMEType,
			Priority:                   priorityForDistance(job.Distance),
			MaxBandwidthBytesPerSecond: s.cfg.MaxBandwidthBytesPerSecond,
		}
		resolved, err := resolver.Resolve(ctx, request)
		if err == nil && s.ram != nil {
			s.mu.Lock()
			hint, hinted := s.hints[asset.AssetKey]
			s.mu.Unlock()
			if hinted && hint.FutureRefCount >= s.cfg.RAMMinFutureRefs && hint.NextUseDistance <= s.cfg.RAMMaxNextUseDistance {
				_ = s.ram.Put(ctx, request, downloader.DownloadedAsset{AssetKey: request.AssetKey, AssetID: request.AssetID, LocalPath: resolved.LocalPath, SHA256: resolved.SHA256, SizeBytes: asset.SizeBytes})
			}
		}
		s.releaseBytes(asset.SizeBytes)
		<-s.sem
		if s.cfg.OnState != nil {
			if err != nil {
				s.cfg.OnState("failed", job, asset, err)
			} else {
				s.cfg.OnState("ready", job, asset, nil)
			}
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
func (s *Scheduler) releaseBytes(n int64) {
	s.mu.Lock()
	s.bytes -= n
	if s.bytes < 0 {
		s.bytes = 0
	}
	s.mu.Unlock()
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

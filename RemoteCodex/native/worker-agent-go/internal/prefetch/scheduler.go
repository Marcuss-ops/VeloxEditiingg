package prefetch

// Scheduler is the execution layer for FutureAssetPlan. It deliberately
// depends on the canonical CacheResolver: prefetch has no cache, transfer,
// hash verifier, or eviction implementation of its own.

import (
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
}

type jobRuntime struct {
	job    futureasset.Job
	ctx    context.Context
	cancel context.CancelFunc
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
	jobs            map[string]jobRuntime
	protects        map[string]string
	pendingProtects map[string]struct{}
	protectExpiries map[string]time.Time
	bytes           int64
	sem             chan struct{}
	state           diskPressureState
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
	return &Scheduler{cfg: cfg, ram: cfg.RAM, jobs: make(map[string]jobRuntime), protects: make(map[string]string), pendingProtects: make(map[string]struct{}), protectExpiries: make(map[string]time.Time), hints: make(map[string]futureasset.ProtectedAsset), prefetched: make(map[string]int64), useful: make(map[string]bool), assetJobs: make(map[string]map[string]struct{}), sem: make(chan struct{}, cfg.MaxConcurrent)}
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
	s.detachJobLocked(runtime.job)
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
		bandwidth := s.cfg.MaxBandwidthBytesPerSecond
		if bandwidth > 0 {
			bandwidth /= int64(cap(s.sem))
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
		if s.cfg.OnState != nil {
			s.cfg.OnState("requested", job, asset, nil)
		}
		resolved, err := resolver.Resolve(ctx, request)
		if err == nil {
			// The canonical transferer commits the verified cache row before
			// Resolve returns. Install a protection that was pending because
			// the row did not exist when the plan arrived. This remains an
			// optimization-only barrier: a transient protection error never
			// invalidates an already verified asset or the foreground path.
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

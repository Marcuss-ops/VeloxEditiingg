package prefetch

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"time"

	"velox-shared/assetref"
	"velox-shared/futureasset"
	"velox-worker-agent/internal/workercache"
)

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
		store, oldProtects := s.resetForExpiredLocked()
		s.mu.Unlock()
		s.signalWork()
		return releaseProtections(store, oldProtects, nil)
	}
	// Phase 1 (under lock): sweep stale jobs and snapshot the protection
	// projection. Reservation IDs are deterministic per (worker, asset), so
	// rebuilding newProtects here is pure bookkeeping — no durable I/O yet.
	scheduled := make(map[string]struct{}, len(plan.PrefetchJobs))
	for _, job := range plan.PrefetchJobs {
		scheduled[job.JobID] = struct{}{}
	}
	for id, runtime := range s.jobs {
		if _, ok := scheduled[id]; !ok {
			runtime.cancel()
			delete(s.jobs, id)
			delete(s.prepared, id)
			s.detachJobLocked(runtime.job)
		}
	}
	store := s.protect
	oldProtects := s.protects
	newProtects := make(map[string]string, len(plan.Protect))
	protectExpiries := make(map[string]time.Time, len(plan.Protect))
	s.hints = make(map[string]futureasset.ProtectedAsset, len(plan.Protect))
	for _, asset := range plan.Protect {
		s.hints[asset.AssetKey] = asset
		reservationID := fmt.Sprintf("future:%s:%s", s.cfg.WorkerID, asset.AssetKey)
		newProtects[asset.AssetKey] = reservationID
		protectExpiries[asset.AssetKey] = plan.ExpiresAt
	}
	s.mu.Unlock()

	// Phase 2 (no lock): perform the durable reservation I/O outside the
	// scheduler lock so sqlite writes never stall the control loop or the
	// scheduler workers. Reserves run before releases so eviction never sees
	// a protection gap; both are keyed by deterministic reservation IDs.
	pendingProtects, err := reserveProtections(store, plan, newProtects)
	if err != nil {
		return err
	}
	if err := releaseProtections(store, oldProtects, newProtects); err != nil {
		return err
	}

	// Phase 3 (under lock): install the new projection and enqueue jobs.
	s.mu.Lock()
	s.protects = newProtects
	s.pendingProtects = pendingProtects
	s.protectExpiries = protectExpiries
	s.currentPlanID = plan.PlanID
	s.currentPlanVersion = plan.Version
	var events []Event
	for _, job := range plan.PrefetchJobs {
		if runtime, exists := s.jobs[job.JobID]; exists {
			if sameScheduledJob(runtime.job, job) {
				continue
			}
			runtime.job = job
			runtime.generation++
			delete(s.prepared, job.JobID)
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

// resetForExpiredLocked cancels and detaches every active job and resets all
// protection/plan state for an expired snapshot. It returns the protection
// store and the prior protects map so the caller can release them outside the
// lock. Caller holds s.mu.
func (s *Scheduler) resetForExpiredLocked() (store workercache.LeaseReservationStore, oldProtects map[string]string) {
	store = s.protect
	oldProtects = s.protects
	for id, runtime := range s.jobs {
		runtime.cancel()
		delete(s.jobs, id)
		s.detachJobLocked(runtime.job)
	}
	s.protects = make(map[string]string)
	s.pendingProtects = make(map[string]struct{})
	s.protectExpiries = make(map[string]time.Time)
	s.hints = make(map[string]futureasset.ProtectedAsset)
	s.readyAtByJob = make(map[string]map[string]readyRecord)
	s.prepared = make(map[string]PreparedJob)
	// NOTE: executionReservations intentionally NOT cleared here.
	// They outlive the future plan and are the sole protection barrier
	// during the execution phase. Only HandoffToExecution and
	// ReleaseExecutionReservations manage this map.
	return store, oldProtects
}

// reserveProtections performs the durable reservation I/O for an incoming
// snapshot OUTSIDE the scheduler lock. It reserves every barrier (keyed by
// the deterministic per-(worker, asset) reservation ID built under the lock)
// and returns the set of assets whose canonical-cache row does not exist yet,
// so Reconcile can keep them pending. A future asset is allowed to be absent
// until its prefetch resolver creates the verified canonical-cache row
// (protection is an eviction barrier, not a download prerequisite); other
// store failures remain fail-closed.
func reserveProtections(store workercache.LeaseReservationStore, plan futureasset.Plan, newProtects map[string]string) (map[string]struct{}, error) {
	pendingProtects := make(map[string]struct{})
	if store == nil {
		return pendingProtects, nil
	}
	for _, asset := range plan.Protect {
		reservationID := newProtects[asset.AssetKey]
		if err := store.Reserve(context.Background(), assetref.AssetKey(asset.AssetKey), reservationID, plan.ExpiresAt); err != nil {
			if errors.Is(err, workercache.ErrNotFound) {
				pendingProtects[asset.AssetKey] = struct{}{}
				continue
			}
			return nil, fmt.Errorf("prefetch: protect %s: %w", asset.AssetKey, err)
		}
	}
	return pendingProtects, nil
}

// releaseProtections releases every prior reservation that is not kept by the
// new protects map (a nil newProtects releases all). It returns the first
// error so the caller can fail closed without aborting the remaining releases.
func releaseProtections(store workercache.LeaseReservationStore, oldProtects, newProtects map[string]string) error {
	if store == nil {
		return nil
	}
	var releaseErr error
	for key, reservationID := range oldProtects {
		if _, keep := newProtects[key]; !keep {
			if err := store.ReleaseReservation(context.Background(), assetref.AssetKey(key), reservationID); err != nil && releaseErr == nil {
				releaseErr = fmt.Errorf("prefetch: release protection %s: %w", key, err)
			}
		}
	}
	return releaseErr
}

// Cancel removes only the prefetch job's waiters. The downloader remains the
// owner of shared transfers and cancels a transfer only after its last waiter.
func (s *Scheduler) Cancel(jobID string) bool {
	s.mu.Lock()
	runtime, ok := s.jobs[jobID]
	if !ok {
		s.mu.Unlock()
		return false
	}
	runtime.cancel()
	delete(s.jobs, jobID)
	delete(s.prepared, jobID)
	// Release execution reservations for this job's assets.
	for _, asset := range runtime.job.Assets {
		if execID, exists := s.executionReservations[asset.AssetKey]; exists {
			delete(s.executionReservations, asset.AssetKey)
			s.mu.Unlock()
			if s.protect != nil {
				_ = s.protect.ReleaseReservation(context.Background(), assetref.AssetKey(asset.AssetKey), execID)
			}
			s.mu.Lock()
		}
	}
	s.detachJobLocked(runtime.job)
	s.signalWork()
	s.mu.Unlock()
	return true
}

func (s *Scheduler) enqueueJobLocked(planVersion uint64, runtime *jobRuntime) []Event {
	assets := orderedAssets(runtime.job)
	events := make([]Event, 0, len(assets))
	for _, asset := range assets {
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

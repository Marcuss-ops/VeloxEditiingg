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
		store := s.protect
		oldProtects := s.protects
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
		s.mu.Unlock()
		s.signalWork()
		var releaseErr error
		if store != nil {
			for key, reservationID := range oldProtects {
				if err := store.ReleaseReservation(context.Background(), assetref.AssetKey(key), reservationID); err != nil && releaseErr == nil {
					releaseErr = fmt.Errorf("prefetch: release expired protection %s: %w", key, err)
				}
			}
		}
		return releaseErr
	}
	// Index the incoming plan's jobs once so the removal sweep below is O(1)
	// per active job instead of a linear scan of plan.PrefetchJobs per job
	// (the previous O(n²) findScheduledJob loop).
	scheduled := make(map[string]struct{}, len(plan.PrefetchJobs))
	for _, job := range plan.PrefetchJobs {
		scheduled[job.JobID] = struct{}{}
	}
	for id, runtime := range s.jobs {
		if _, ok := scheduled[id]; !ok {
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
		var releaseErr error
		for key, reservationID := range oldProtects {
			if _, keep := newProtects[key]; !keep {
				if err := store.ReleaseReservation(context.Background(), assetref.AssetKey(key), reservationID); err != nil && releaseErr == nil {
					releaseErr = fmt.Errorf("prefetch: release protection %s: %w", key, err)
				}
			}
		}
		if releaseErr != nil {
			s.mu.Unlock()
			return releaseErr
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

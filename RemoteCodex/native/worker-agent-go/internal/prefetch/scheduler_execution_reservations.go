package prefetch

import (
	"context"
	"fmt"
	"time"

	"velox-shared/assetref"
)

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
	s.mu.Unlock()
	if store == nil {
		return
	}
	for assetKey, execID := range execs {
		_ = store.ReleaseReservation(context.Background(), assetref.AssetKey(assetKey), execID)
	}
}

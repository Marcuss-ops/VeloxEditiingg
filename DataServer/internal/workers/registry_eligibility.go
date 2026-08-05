package workers

import (
	"context"
	"time"

	"velox-server/internal/costmodel"
	"velox-server/internal/logging"
)

// registry_eligibility.go owns the cost-aware dispatch eligibility
// surface of the Registry (GetSchedulableWorkers / GetEligibleWorkers)
// plus the stale-worker eviction loop (CleanupStaleWorkers). The
// connection-status derivation, the read endpoints and the hydrate
// plumbing live in registry_query.go.

// GetSchedulableWorkers returns workers that can accept new jobs.
// It routes through GetEligibleWorkers with default permissive
// requirements so dispatcher callers use the canonical costmodel path.
func (r *Registry) GetSchedulableWorkers(ctx context.Context) []Worker {
	return r.GetEligibleWorkers(ctx, costmodel.DefaultRequirements())
}

// GetEligibleWorkers is the canonical cost-aware eligibility entry point.
// Capacity occupancy is hydrated from the lease store before scoring;
// heartbeat active_tasks/task_slots are never used as admission state.
func (r *Registry) GetEligibleWorkers(ctx context.Context, req costmodel.JobRequirements) []Worker {
	now := time.Now().UTC()
	_, workers := r.snapshotRegistered(func(id string, w Worker) bool { return true })
	if len(workers) == 0 {
		return workers
	}
	ids := make([]string, len(workers))
	for i := range workers {
		ids[i] = workers[i].WorkerID.String()
	}
	r.hydrateBulk(ctx, ids, workers, now)

	result := make([]Worker, 0, len(workers))
	for _, w := range workers {
		if w.Quarantined {
			continue
		}
		resources := costmodel.ResourceSnapshotFromMaps(nil, w.Metrics)
		isOffline := IsHeartbeatOffline(w.LastHB, now)
		if !w.Capacity.Authoritative || w.Capacity.MaxSlots <= 0 {
			// Admission must fail closed when the lease-store read or
			// declared worker capacity is unavailable; zero occupancy
			// is not permission to schedule and zero max slots is not
			// an unlimited worker.
			continue
		}
		profile := costmodel.BuildWorkerProfileFromRegistry(
			w.WorkerID.String(),
			w.Schedulable,
			w.Drain,
			isOffline,
			w.Capacity.ActiveSlots,
			w.Capacity.MaxSlots,
			w.ExecutorRegistrySnapshot(),
		)
		profile.Resources = resources
		profile.Pressure = costmodel.DerivePressure(resources, costmodel.DefaultAdmissionPolicy())
		c, _ := costmodel.Score(profile, req)
		if c.Eligible {
			result = append(result, w)
		}
	}
	return result
}

// CleanupStaleWorkers removes workers that haven't sent a heartbeat in the given duration
func (r *Registry) CleanupStaleWorkers(ctx context.Context, maxAge time.Duration) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now().UTC()
	count := 0

	for id, w := range r.inMem {
		if w.LastHB != "" {
			t, err := time.Parse(time.RFC3339, w.LastHB)
			if err == nil && now.Sub(t.UTC()) > maxAge {
				delete(r.inMem, id)
				if r.dbStore != nil {
					if err := r.dbStore.DeleteWorker(id.String()); err != nil {
						registryLog.ErrorWithMsg(logging.CodeRegistryDeleteStaleWorkerFail,
							"Failed to delete stale worker",
							map[string]interface{}{"worker_id": id, "err": err.Error()})
					}
				}
				count++
				registryLog.InfoWithMsg(logging.CodeRegistryStaleWorkerCleanup,
					"Cleaned up stale worker",
					map[string]interface{}{"worker_id": id, "last_seen": w.LastHB})
			}
		}
	}

	return count
}

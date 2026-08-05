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
func (r *Registry) GetSchedulableWorkers(ctx context.Context) []WorkerInfo {
	return r.GetEligibleWorkers(ctx, costmodel.DefaultRequirements())
}

// GetEligibleWorkers is the canonical cost-aware eligibility entry point.
// It builds a WorkerProfile from each registered worker and accepts only
// profiles that costmodel.Score marks eligible. Drain, offline and capacity
// exclusions live in the costmodel path, not in ad-hoc registry filters.
func (r *Registry) GetEligibleWorkers(ctx context.Context, req costmodel.JobRequirements) []WorkerInfo {
	now := time.Now().UTC()
	r.mu.RLock()
	var result []WorkerInfo
	for _, w := range r.inMem {
		if r.revoked[w.WorkerID] {
			continue
		}
		resources := costmodel.ResourceSnapshotFromMaps(w.Capabilities, w.Metrics)
		profile := costmodel.BuildWorkerProfile(
			w.WorkerID.String(),
			w.Schedulable,
			w.Drain,
			w.Status,
			resources.ActiveTasks, resources.TaskSlots,
			w.Capabilities,
		)
		profile.Resources = resources
		profile.Pressure = costmodel.DerivePressure(resources, costmodel.DefaultAdmissionPolicy())
		c, _ := costmodel.Score(profile, req)
		if !c.Eligible {
			continue
		}
		result = append(result, w)
	}
	r.mu.RUnlock()
	if len(result) == 0 {
		return result
	}
	ids := make([]string, len(result))
	for i, w := range result {
		ids[i] = w.WorkerID.String()
	}
	r.hydrateBulk(ctx, ids, result, now)
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

	// No need for bulk save — each deletion already hits SQLite
	return count
}

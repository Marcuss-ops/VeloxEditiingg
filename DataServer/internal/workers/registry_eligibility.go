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
// It builds a WorkerProfile from each registered worker and accepts only
// profiles that costmodel.Score marks eligible. Drain, offline and capacity
// exclusions live in the costmodel path, not in ad-hoc registry filters.
func (r *Registry) GetEligibleWorkers(ctx context.Context, req costmodel.JobRequirements) []Worker {
	now := time.Now().UTC()
	r.mu.RLock()
	var result []Worker
	for _, w := range r.inMem {
		if r.revoked[w.WorkerID] {
			continue
		}
		resources := costmodel.ResourceSnapshotFromMaps(w.Capabilities, w.Metrics)
		// Connectivity gate at eligibility time: the heartbeat-only arm of
		// the canonical ConnectionState (IsHeartbeatOffline). The session
		// arm is hydrated only on the read paths (List/GetWorker) —
		// eligibility is a hot path and must not run per-worker session
		// queries under the registry lock. The legacy free-form agent
		// status string is gone: offline is DERIVED from the heartbeat,
		// never reported by the agent.
		isOffline := IsHeartbeatOffline(w.LastHB, now)
		profile := costmodel.BuildWorkerProfileFromRegistry(
			w.WorkerID.String(),
			w.Schedulable,
			w.Drain,
			isOffline,
			resources.ActiveTasks, resources.TaskSlots,
			w.ExecutorRegistrySnapshot(),
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

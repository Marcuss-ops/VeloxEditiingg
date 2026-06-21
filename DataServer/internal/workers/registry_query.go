package workers

import (
	"context"
	"time"

	"velox-server/internal/costmodel"
	"velox-server/internal/logging"
	"velox-shared/identity"
)

func (r *Registry) IsRegistered(ctx context.Context, workerID string) bool {
	workerID = identity.NormalizeWorkerID(workerID)
	r.mu.RLock()
	_, ok := r.inMem[workerID]
	r.mu.RUnlock()
	return ok
}

// GetWorker returns a single worker's info by ID
func (r *Registry) GetWorker(ctx context.Context, workerID string) *WorkerInfo {
	workerID = identity.NormalizeWorkerID(workerID)
	r.mu.RLock()
	info, ok := r.inMem[workerID]
	r.mu.RUnlock()
	if !ok {
		return nil
	}
	return &info
}

func (r *Registry) List(ctx context.Context) []WorkerInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	list := make([]WorkerInfo, 0, len(r.inMem))
	for _, v := range r.inMem {
		if r.revoked[v.WorkerID] {
			continue
		}
		list = append(list, v)
	}
	return list
}

// StatusSnapshot returns both the registered worker list and the live worker list.
// Registered workers exclude revoked entries; live workers are filtered by heartbeat freshness.
func (r *Registry) StatusSnapshot(ctx context.Context, timeout time.Duration) (registered []WorkerInfo, live []WorkerInfo) {
	return r.List(ctx), r.GetActiveWorkers(ctx, timeout)
}

// GetStaleWorkers returns registered workers that have not heartbeated within timeout.
func (r *Registry) GetStaleWorkers(ctx context.Context, timeout time.Duration) []WorkerInfo {
	registered := r.List(ctx)
	live := r.GetActiveWorkers(ctx, timeout)
	if len(registered) == 0 {
		return nil
	}

	liveSet := make(map[string]struct{}, len(live))
	for _, w := range live {
		liveSet[w.WorkerID] = struct{}{}
	}

	stale := make([]WorkerInfo, 0, len(registered))
	for _, w := range registered {
		if _, ok := liveSet[w.WorkerID]; ok {
			continue
		}
		stale = append(stale, w)
	}
	return stale
}

// GetWorkersByGroup returns all workers in a specific group
func (r *Registry) GetWorkersByGroup(ctx context.Context, group string) []WorkerInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []WorkerInfo
	for _, w := range r.inMem {
		if w.WorkerGroup == group {
			result = append(result, w)
		}
	}
	return result
}

// GetActiveWorkers returns workers that have sent a heartbeat recently
func (r *Registry) GetActiveWorkers(ctx context.Context, timeout time.Duration) []WorkerInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	now := time.Now().UTC()
	var result []WorkerInfo

	for _, w := range r.inMem {
		if r.revoked[w.WorkerID] {
			continue
		}
		if w.LastHB != "" {
			t, err := time.Parse(time.RFC3339, w.LastHB)
			if err == nil && now.Sub(t.UTC()) < timeout {
				result = append(result, w)
			}
		}
	}
	return result
}

// GetSchedulableWorkers returns workers that can accept new jobs.
//
// PR-04.4: routes through GetEligibleWorkers (costmodel.Score).
// The default permissive JobRequirements preserves today's queue
// routing until enqueue publishes per-job requirements on
// QueueItem/Job (a follow-up PR). Callers that want a per-job filter
// should call GetEligibleWorkers directly with a populated
// costmodel.JobRequirements.
func (r *Registry) GetSchedulableWorkers(ctx context.Context) []WorkerInfo {
	return r.GetEligibleWorkers(ctx, costmodel.DefaultRequirements())
}

// GetEligibleWorkers is the canonical cost-aware eligibility entry
// point. PR-04.4 replaces the legacy boolean-AND
// (revoked + drain + offline) with costmodel.Score on a
// WorkerProfile composed by BuildWorkerProfile from heartbeat state
// and the heartbeat `capabilities` map. Empty JobRequirements = no
// four-field gate (preserves legacy behavior). Non-empty
// JobRequirements = canonical four-field resource-class / temporal-
// mode matching per DataServer/internal/costmodel/cost.go.
//
// Why this replaces a hand-rolled boolean AND:
//   - The four canonical Descriptor fields (ResourceClass,
//     TemporalMode, Deterministic, Cacheable) are now the single
//     source of truth for "is this worker eligible for X".
//   - Exhaustiveness: extensible to additional eligibility axes
//     (multi-resource requirements, resource pressure) by extending
//     costmodel.Score — never by editing call sites.
//   - Rank: when per-job requirements appear (PR-04.5), a parallel
//     off-by-default rank call site already exists in the same
//     module, ready to flip on.
//
// Forbidden patterns (see OWNERSHIP.md "Cost-aware eligibility"):
//   - Hand-rolled boolean AND on WorkerInfo fields inside this
//     package — use the cost-modeled path.
//   - Per-job-type switch arms inside Registry methods for
//     placement — they effectively re-create a parallel selector.
func (r *Registry) GetEligibleWorkers(ctx context.Context, req costmodel.JobRequirements) []WorkerInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []WorkerInfo
	for _, w := range r.inMem {
		if r.revoked[w.WorkerID] {
			continue
		}
		profile := costmodel.BuildWorkerProfile(
			w.WorkerID,
			w.Schedulable,
			w.Drain,
			w.Status,
			0, 0,
			w.Capabilities,
		)
		c, _ := costmodel.Score(profile, req)
		if !c.Eligible {
			continue
		}
		result = append(result, w)
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
					if err := r.dbStore.DeleteWorker(id); err != nil {
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

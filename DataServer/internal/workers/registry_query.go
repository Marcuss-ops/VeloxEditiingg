package workers

import (
	"context"
	"time"

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

// GetSchedulableWorkers returns workers that can accept new jobs
func (r *Registry) GetSchedulableWorkers(ctx context.Context) []WorkerInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []WorkerInfo
	for _, w := range r.inMem {
		if r.revoked[w.WorkerID] {
			continue
		}
		if w.Schedulable && !w.Drain && w.Status != "offline" {
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

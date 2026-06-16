package workers

import (
	"context"

	"velox-server/internal/logging"
	"velox-shared/identity"
)

// IsRevoked checks if a worker has been revoked
func (r *Registry) IsRevoked(workerID string) bool {
	workerID = identity.NormalizeWorkerID(workerID)
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.revoked[workerID]
}

// RevokeWorker marks a worker as revoked and removes it from the active set
func (r *Registry) RevokeWorker(ctx context.Context, workerID string) {
	workerID = identity.NormalizeWorkerID(workerID)
	r.mu.Lock()
	r.revoked[workerID] = true
	delete(r.inMem, workerID)

	if r.dbStore != nil {
		if err := r.dbStore.SetWorkerRevoked(workerID, true); err != nil {
			registryLog.ErrorWithMsg(logging.CodeRegistryPersistRevokeFail,
				"Failed to persist worker revoke",
				map[string]interface{}{"worker_id": workerID, "err": err.Error()})
		}
	}
	r.mu.Unlock()
}

// UnrevokeWorker removes a worker from the revoked list
func (r *Registry) UnrevokeWorker(workerID string) {
	workerID = identity.NormalizeWorkerID(workerID)
	r.mu.Lock()
	delete(r.revoked, workerID)

	if r.dbStore != nil {
		if err := r.dbStore.SetWorkerRevoked(workerID, false); err != nil {
			registryLog.ErrorWithMsg(logging.CodeRegistryPersistUnrevokeFail,
				"Failed to persist worker unrevoke",
				map[string]interface{}{"worker_id": workerID, "err": err.Error()})
		}
	}
	r.mu.Unlock()
}

// LoadRevoked loads a set of revoked worker IDs into the in-memory revoked set.
// This is used during bootstrap to persist revocation state across restarts.
func (r *Registry) LoadRevoked(ids []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, id := range ids {
		normID := identity.NormalizeWorkerID(id)
		r.revoked[normID] = true
		delete(r.inMem, normID)
	}
}

// ListRevoked returns the list of revoked worker IDs.
func (r *Registry) ListRevoked() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.revoked))
	for id := range r.revoked {
		ids = append(ids, id)
	}
	return ids
}

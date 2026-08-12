package workers

import (
	"context"
	"fmt"

	"velox-server/internal/logging"
	"velox-shared/identity"
)

// IsRevoked checks if a worker has been revoked
func (r *Registry) IsRevoked(workerID string) bool {
	workerID = identity.NormalizeWorkerID(workerID)
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.revoked[identity.ParseWorkerID(workerID)]
}

// RevokeWorker durably marks a worker as revoked and removes it from the
// active set. The in-memory projection advances only after persistence
// succeeds, so a storage failure cannot create a false revocation state.
func (r *Registry) RevokeWorker(ctx context.Context, workerID string) error {
	workerID = identity.NormalizeWorkerID(workerID)
	id := identity.ParseWorkerID(workerID)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.dbStore != nil {
		if err := r.dbStore.SetWorkerRevoked(workerID, true); err != nil {
			registryLog.ErrorWithMsg(logging.CodeRegistryPersistRevokeFail,
				"Failed to persist worker revoke",
				map[string]interface{}{"worker_id": workerID, "err": err.Error()})
			return fmt.Errorf("persist worker revoke: %w", err)
		}
	}
	r.revoked[id] = true
	delete(r.inMem, id)
	return nil
}

// UnrevokeWorker durably removes a worker from the revoked list. The
// in-memory projection advances only after persistence succeeds.
func (r *Registry) UnrevokeWorker(workerID string) error {
	workerID = identity.NormalizeWorkerID(workerID)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.dbStore != nil {
		if err := r.dbStore.SetWorkerRevoked(workerID, false); err != nil {
			registryLog.ErrorWithMsg(logging.CodeRegistryPersistUnrevokeFail,
				"Failed to persist worker unrevoke",
				map[string]interface{}{"worker_id": workerID, "err": err.Error()})
			return fmt.Errorf("persist worker unrevoke: %w", err)
		}
	}
	delete(r.revoked, identity.ParseWorkerID(workerID))
	return nil
}

// LoadRevoked loads a set of revoked worker IDs into the in-memory revoked set.
// This is used during bootstrap to persist revocation state across restarts.
func (r *Registry) LoadRevoked(ids []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, id := range ids {
		normID := identity.ParseWorkerID(identity.NormalizeWorkerID(id))
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
		ids = append(ids, id.String())
	}
	return ids
}

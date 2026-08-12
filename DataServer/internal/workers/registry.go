package workers

import (
	"encoding/json"
	"fmt"
	"sync"

	"velox-server/internal/logging"
	"velox-server/internal/store"
	"velox-shared/identity"
)

// registryLog is the structured logger for the worker registry package.
// One declaration shared across all registry_*.go files (Go package scope).
var registryLog = logging.NewLogger("workers.registry")

// Registry manages worker registration, heartbeats, and revocation.
// SQLite is the single source of truth; in-memory map is a cache rebuilt at startup.
type Registry struct {
	mu      sync.RWMutex
	inMem   map[identity.WorkerID]Worker
	revoked map[identity.WorkerID]bool
	dbStore *store.SQLiteStore
}

// New creates a Registry with SQLite as the backing store.
func New(dbStore *store.SQLiteStore) *Registry {
	r := &Registry{
		inMem:   make(map[identity.WorkerID]Worker),
		revoked: make(map[identity.WorkerID]bool),
		dbStore: dbStore,
	}
	if err := r.load(); err != nil {
		registryLog.ErrorWithMsg(logging.CodeRegistryLoadWorkersFail,
			"Worker registry loaded with persistence errors; compatibility constructor returned a partial projection",
			map[string]interface{}{"err": err.Error()})
	}
	return r
}

// NewWithError constructs the persistent worker registry for production
// bootstrap. Unlike New, it refuses to return a partially loaded registry
// when SQLite cannot provide the worker or revocation projections.
func NewWithError(dbStore *store.SQLiteStore) (*Registry, error) {
	r := &Registry{
		inMem:   make(map[identity.WorkerID]Worker),
		revoked: make(map[identity.WorkerID]bool),
		dbStore: dbStore,
	}
	if err := r.load(); err != nil {
		return nil, err
	}
	return r, nil
}

// load reads workers and revoked list from SQLite into the in-memory cache.
func (r *Registry) load() error {
	if r.dbStore == nil {
		return nil
	}
	var loadErr error

	// Load workers
	workers, err := r.dbStore.ListWorkers()
	if err != nil {
		registryLog.ErrorWithMsg(logging.CodeRegistryLoadWorkersFail,
			"Failed to load workers from SQLite",
			map[string]interface{}{"err": err.Error()})
		loadErr = fmt.Errorf("load workers: %w", err)
	} else {
		r.mu.Lock()
		for _, m := range workers {
			var info Worker
			raw, _ := json.Marshal(m)
			if err := json.Unmarshal(raw, &info); err != nil {
				continue
			}
			info.WorkerID = info.WorkerID.Normalized()
			info.ExecutorCapabilities = info.ExecutorRegistrySnapshot()
			r.inMem[info.WorkerID] = info
		}
		r.mu.Unlock()
	}

	// Load revoked flags
	revokedIDs, err := r.dbStore.GetRevokedWorkers()
	if err != nil {
		registryLog.ErrorWithMsg(logging.CodeRegistryLoadRevokedFail,
			"Failed to load revoked workers from SQLite",
			map[string]interface{}{"err": err.Error()})
		if loadErr == nil {
			loadErr = fmt.Errorf("load revoked workers: %w", err)
		} else {
			loadErr = fmt.Errorf("%v; load revoked workers: %w", loadErr, err)
		}
	} else {
		r.mu.Lock()
		for _, id := range revokedIDs {
			r.revoked[identity.ParseWorkerID(id).Normalized()] = true
		}
		r.mu.Unlock()
	}

	r.mu.RLock()
	registryLog.InfoWithMsg(logging.CodeRegistryLoadedSummary,
		"Workers loaded from SQLite",
		map[string]interface{}{
			"worker_count":  len(r.inMem),
			"revoked_count": len(r.revoked),
		})
	r.mu.RUnlock()
	return loadErr
}

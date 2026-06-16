package workers

import (
	"encoding/json"
	"sync"

	"velox-server/internal/logging"
	"velox-server/internal/store"
)

// registryLog is the structured logger for the worker registry package.
// One declaration shared across all registry_*.go files (Go package scope).
var registryLog = logging.NewLogger("workers.registry")

// Registry manages worker registration, heartbeats, and revocation.
// SQLite is the single source of truth; in-memory map is a cache rebuilt at startup.
type Registry struct {
	mu      sync.RWMutex
	inMem   map[string]WorkerInfo
	revoked map[string]bool
	dbStore *store.SQLiteStore
}

// New creates a Registry with SQLite as the backing store.
func New(dbStore *store.SQLiteStore) *Registry {
	r := &Registry{
		inMem:   make(map[string]WorkerInfo),
		revoked: make(map[string]bool),
		dbStore: dbStore,
	}
	r.load()
	return r
}

// load reads workers and revoked list from SQLite into the in-memory cache.
func (r *Registry) load() {
	if r.dbStore == nil {
		return
	}

	// Load workers
	workers, err := r.dbStore.ListWorkers()
	if err != nil {
		registryLog.ErrorWithMsg(logging.CodeRegistryLoadWorkersFail,
			"Failed to load workers from SQLite",
			map[string]interface{}{"err": err.Error()})
	} else {
		r.mu.Lock()
		for _, m := range workers {
			var info WorkerInfo
			raw, _ := json.Marshal(m)
			if err := json.Unmarshal(raw, &info); err != nil {
				continue
			}
			normID := NormalizeWorkerID(info.WorkerID)
			info.WorkerID = normID
			r.inMem[normID] = info
		}
		r.mu.Unlock()
	}

	// Load revoked flags
	revokedIDs, err := r.dbStore.GetRevokedWorkers()
	if err != nil {
		registryLog.ErrorWithMsg(logging.CodeRegistryLoadRevokedFail,
			"Failed to load revoked workers from SQLite",
			map[string]interface{}{"err": err.Error()})
	} else {
		r.mu.Lock()
		for _, id := range revokedIDs {
			r.revoked[NormalizeWorkerID(id)] = true
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
}

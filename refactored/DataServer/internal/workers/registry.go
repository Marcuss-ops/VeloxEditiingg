package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"velox-server/internal/store"
)

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
		log.Printf("registry: failed to load workers from SQLite: %v", err)
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
		log.Printf("registry: failed to load revoked workers from SQLite: %v", err)
	} else {
		r.mu.Lock()
		for _, id := range revokedIDs {
			r.revoked[NormalizeWorkerID(id)] = true
		}
		r.mu.Unlock()
	}

	r.mu.RLock()
	log.Printf("registry: loaded %d workers from SQLite, %d revoked", len(r.inMem), len(r.revoked))
	r.mu.RUnlock()
}

func (r *Registry) Heartbeat(ctx context.Context, workerID, workerName, status, currentJob string, extra map[string]interface{}) error {
	now := time.Now().UTC().Format(time.RFC3339)

	workerID = NormalizeWorkerID(workerID)

	r.mu.Lock()
	defer r.mu.Unlock()

	// Reject heartbeat for revoked workers
	if r.revoked[workerID] {
		return fmt.Errorf("worker %s is revoked", workerID)
	}

	// Preserve existing state unless explicitly updated by heartbeat payload.
	existing, hasExisting := r.inMem[workerID]

	info := WorkerInfo{
		WorkerID:    workerID,
		WorkerName:  workerName,
		Status:      status,
		LastHB:      now,
		CurrentJob:  currentJob,
		Schedulable: true,
	}
	if hasExisting {
		info = existing
		info.WorkerID = workerID
		if workerName != "" {
			info.WorkerName = workerName
		}
		info.Status = status
		info.LastHB = now
		info.CurrentJob = currentJob
	}

	if extra != nil {
		if v, ok := extra["drain"]; ok {
			if b, ok := v.(bool); ok {
				info.Drain = b
			}
		}
		if v, ok := extra["schedulable"]; ok {
			if b, ok := v.(bool); ok {
				info.Schedulable = b
			}
		}
		if v, ok := extra["worker_group"]; ok {
			if s, ok := v.(string); ok && s != "" {
				info.WorkerGroup = s
			}
		}
		applyMetadataFields(extra, &info)
		if v, ok := extra["readiness"].(map[string]interface{}); ok {
			info.Readiness = v
		}
		if v, ok := extra["metrics"].(map[string]interface{}); ok {
			info.Metrics = v
		}
		if v, ok := extra["recent_logs"]; ok {
			info.RecentLogs = ExtractStringSlice(v)
		}
		if v, ok := extra["recent_errors"]; ok {
			info.RecentErrors = ExtractStringSlice(v)
		}
		if v, ok := extra["jobs_completed"].(float64); ok {
			if info.Metrics == nil {
				info.Metrics = make(map[string]interface{})
			}
			info.Metrics["jobs_completed"] = int64(v)
		}
		if v, ok := extra["jobs_failed"].(float64); ok {
			if info.Metrics == nil {
				info.Metrics = make(map[string]interface{})
			}
			info.Metrics["jobs_failed"] = int64(v)
		}
	}

	r.inMem[workerID] = info

	// Persist to SQLite (single source of truth)
	if r.dbStore != nil {
		raw, _ := json.Marshal(info)
		if err := r.dbStore.UpsertWorker(raw); err != nil {
			log.Printf("registry: sqlite upsert worker heartbeat failed: %v", err)
		}
	}
	return nil
}

func (r *Registry) IsRegistered(ctx context.Context, workerID string) bool {
	workerID = NormalizeWorkerID(workerID)
	r.mu.RLock()
	_, ok := r.inMem[workerID]
	r.mu.RUnlock()
	return ok
}

// GetWorker returns a single worker's info by ID
func (r *Registry) GetWorker(ctx context.Context, workerID string) *WorkerInfo {
	workerID = NormalizeWorkerID(workerID)
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

// RegisterWorker registers a new worker or updates an existing one
func (r *Registry) RegisterWorker(ctx context.Context, workerID, workerName, ipAddress string, extra map[string]interface{}) error {
	workerID = NormalizeWorkerID(workerID)
	now := time.Now().UTC().Format(time.RFC3339)

	r.mu.Lock()
	defer r.mu.Unlock()

	// Check if already registered (preserve first_seen, display_name, worker_group)
	existing, ok := r.inMem[workerID]
	firstSeen := now
	displayName := workerName
	workerGroup := ""

	if ok {
		firstSeen = existing.FirstSeen
		if existing.DisplayName != "" {
			displayName = existing.DisplayName
		}
		if existing.WorkerGroup != "" {
			workerGroup = existing.WorkerGroup
		}
	}

	// Extract extra fields
	if extra != nil {
		if v, ok := extra["display_name"].(string); ok && v != "" {
			displayName = v
		}
		if v, ok := extra["worker_group"].(string); ok && v != "" {
			workerGroup = v
		}
	}

	info := WorkerInfo{
		WorkerID:    workerID,
		WorkerName:  workerName,
		DisplayName: displayName,
		Status:      "online",
		LastHB:      now,
		FirstSeen:   firstSeen,
		IPAddress:   ipAddress,
		Host:        ipAddress,
		Schedulable: true,
		WorkerGroup: workerGroup,
	}
	applyMetadataFields(extra, &info)

	r.inMem[workerID] = info

	// Persist to SQLite
	if r.dbStore != nil {
		raw, _ := json.Marshal(info)
		if err := r.dbStore.UpsertWorker(raw); err != nil {
			log.Printf("registry: sqlite upsert worker register failed: %v", err)
		}
	}
	return nil
}

// UnregisterWorker removes a worker from the registry
func (r *Registry) UnregisterWorker(ctx context.Context, workerID string) error {
	workerID = NormalizeWorkerID(workerID)
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.inMem, workerID)

	if r.dbStore != nil {
		if err := r.dbStore.DeleteWorker(workerID); err != nil {
			log.Printf("registry: failed to delete worker %s: %v", workerID, err)
		}
	}
	return nil
}

// IsRevoked checks if a worker has been revoked
func (r *Registry) IsRevoked(workerID string) bool {
	workerID = NormalizeWorkerID(workerID)
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.revoked[workerID]
}

// RevokeWorker marks a worker as revoked and removes it from the active set
func (r *Registry) RevokeWorker(ctx context.Context, workerID string) {
	workerID = NormalizeWorkerID(workerID)
	r.mu.Lock()
	r.revoked[workerID] = true
	delete(r.inMem, workerID)

	if r.dbStore != nil {
		if err := r.dbStore.SetWorkerRevoked(workerID, true); err != nil {
			log.Printf("registry: failed to persist worker revoke: %v", err)
		}
	}
	r.mu.Unlock()
}

// UnrevokeWorker removes a worker from the revoked list
func (r *Registry) UnrevokeWorker(workerID string) {
	workerID = NormalizeWorkerID(workerID)
	r.mu.Lock()
	delete(r.revoked, workerID)

	if r.dbStore != nil {
		if err := r.dbStore.SetWorkerRevoked(workerID, false); err != nil {
			log.Printf("registry: failed to persist worker unrevoke: %v", err)
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
		normID := NormalizeWorkerID(id)
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

// UpdateWorker updates specific fields of a worker
func (r *Registry) UpdateWorker(ctx context.Context, workerID string, updates map[string]interface{}) error {
	workerID = NormalizeWorkerID(workerID)
	r.mu.Lock()
	defer r.mu.Unlock()

	info, ok := r.inMem[workerID]
	if !ok {
		return fmt.Errorf("worker not found: %s", workerID)
	}

	// Apply updates
	if v, ok := updates["worker_name"].(string); ok {
		info.WorkerName = v
	}
	if v, ok := updates["display_name"].(string); ok {
		info.DisplayName = v
	}
	if v, ok := updates["worker_group"].(string); ok {
		info.WorkerGroup = v
	}
	if v, ok := updates["status"].(string); ok {
		info.Status = v
	}
	if v, ok := updates["drain"].(bool); ok {
		info.Drain = v
	}
	if v, ok := updates["schedulable"].(bool); ok {
		info.Schedulable = v
	}
	if v, ok := updates["current_job"].(string); ok {
		info.CurrentJob = v
	}
	if v, ok := updates["code_version"].(string); ok {
		info.CodeVersion = v
	}
	if v, ok := updates["bundle_version"].(string); ok {
		info.BundleVersion = v
	}
	if v, ok := updates["bundle_hash"].(string); ok {
		info.BundleHash = v
	}
	if v, ok := updates["protocol_version"].(string); ok {
		info.ProtocolVersion = v
	}
	if v, ok := updates["engine_version"].(string); ok {
		info.EngineVersion = v
	}
	if v, ok := updates["capabilities"]; ok {
		info.Capabilities = normalizeCapabilities(v)
	}
	if v, ok := updates["ip_address"].(string); ok {
		info.IPAddress = v
		info.Host = v
	}
	if v, ok := updates["recent_logs"].([]string); ok {
		info.RecentLogs = v
	}
	if v, ok := updates["recent_errors"].([]string); ok {
		info.RecentErrors = v
	}
	if v, ok := updates["readiness"].(map[string]interface{}); ok {
		info.Readiness = v
	}
	if v, ok := updates["metrics"].(map[string]interface{}); ok {
		info.Metrics = v
	}

	info.LastHB = time.Now().UTC().Format(time.RFC3339)
	r.inMem[workerID] = info

	if r.dbStore != nil {
		raw, _ := json.Marshal(info)
		if err := r.dbStore.UpsertWorker(raw); err != nil {
			log.Printf("registry: sqlite upsert worker update failed: %v", err)
		}
	}
	return nil
}

// SetWorkerDrain sets the drain status for a worker
func (r *Registry) SetWorkerDrain(ctx context.Context, workerID string, drain bool) error {
	return r.UpdateWorker(ctx, workerID, map[string]interface{}{"drain": drain})
}

// SetWorkerGroup sets the group for a worker
func (r *Registry) SetWorkerGroup(ctx context.Context, workerID string, group string) error {
	return r.UpdateWorker(ctx, workerID, map[string]interface{}{"worker_group": group})
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

// Save persists the current registry state to SQLite immediately.
// This is a no-op now since every mutation already writes to SQLite.
// Kept for API compatibility; can be removed in a future cleanup.
func (r *Registry) Save() error {
	return nil
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
						log.Printf("registry: failed to delete stale worker %s: %v", id, err)
					}
				}
				count++
				log.Printf("registry: cleaned up stale worker: %s (last seen %s)", id, w.LastHB)
			}
		}
	}

	// No need for bulk save — each deletion already hits SQLite
	return count
}

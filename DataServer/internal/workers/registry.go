package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"velox-server/internal/store"
)

const workerPrefix = "velox:worker:"

type Registry struct {
	mu       sync.RWMutex
	redis    *redis.Client
	inMem    map[string]WorkerInfo
	revoked  map[string]bool
	useRedis bool
	dbStore  *store.SQLiteStore

	// File-based persistence (replaces WorkerRegistry)
	filePath string
}

func New(rdb *redis.Client, useRedis bool, dbStore *store.SQLiteStore) *Registry {
	return &Registry{redis: rdb, inMem: make(map[string]WorkerInfo), revoked: make(map[string]bool), useRedis: useRedis, dbStore: dbStore}
}

// NewWithPersistence creates a Registry with file-based persistence for revoked workers.
// dataDir is the directory where workers.json will be stored.
func NewWithPersistence(rdb *redis.Client, useRedis bool, dbStore *store.SQLiteStore, dataDir string) *Registry {
	r := &Registry{
		redis:    rdb,
		inMem:    make(map[string]WorkerInfo),
		revoked:  make(map[string]bool),
		useRedis: useRedis,
		dbStore:  dbStore,
		filePath: filepath.Join(dataDir, "workers.json"),
	}
	r.load()
	return r
}

// load reads workers and revoked list from the JSON file.
func (r *Registry) load() {
	if r.filePath == "" {
		return
	}

	data, err := os.ReadFile(r.filePath)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("registry: failed to load workers file: %v", err)
		}
		return
	}

	var persisted struct {
		Workers map[string]WorkerInfo `json:"workers"`
		Revoked []string              `json:"revoked,omitempty"`
	}

	if err := json.Unmarshal(data, &persisted); err != nil {
		log.Printf("registry: failed to parse workers file: %v", err)
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if persisted.Workers != nil {
		r.inMem = persisted.Workers
	}
	for _, id := range persisted.Revoked {
		r.revoked[id] = true
	}
}

// save persists workers and revoked list to the JSON file atomically.
// Must be called with r.mu held (at least RLock).
func (r *Registry) save() error {
	if r.filePath == "" {
		return nil
	}

	var revoked []string
	for id := range r.revoked {
		revoked = append(revoked, id)
	}

	persisted := struct {
		Workers map[string]WorkerInfo `json:"workers"`
		Revoked []string              `json:"revoked,omitempty"`
	}{
		Workers: r.inMem,
		Revoked: revoked,
	}

	data, err := json.MarshalIndent(persisted, "", "  ")
	if err != nil {
		return err
	}

	tempPath := r.filePath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return err
	}
	if err := os.Rename(tempPath, r.filePath); err != nil {
		return err
	}

	// Best-effort dual-write to SQLite
	if r.dbStore != nil {
		rawWorkers := make(map[string][]byte, len(r.inMem))
		for id, w := range r.inMem {
			b, err := json.Marshal(w)
			if err != nil {
				continue
			}
			rawWorkers[id] = b
		}
		if err := r.dbStore.ReplaceWorkers(rawWorkers, r.revoked); err != nil {
			log.Printf("registry: sqlite dual-write workers failed: %v", err)
		}
	}
	return nil
}

func (r *Registry) Heartbeat(ctx context.Context, workerID, workerName, status, currentJob string, extra map[string]interface{}) error {
	now := time.Now().UTC().Format(time.RFC3339)

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
		if v, ok := extra["code_version"].(string); ok && v != "" {
			info.CodeVersion = v
		}
		if v, ok := extra["bundle_version"].(string); ok && v != "" {
			info.BundleVersion = v
		}
		if v, ok := extra["readiness"].(map[string]interface{}); ok {
			info.Readiness = v
		}
		if v, ok := extra["metrics"].(map[string]interface{}); ok {
			info.Metrics = v
		}
		if v, ok := extra["recent_logs"]; ok {
			info.RecentLogs = extractStringSlice(v)
		}
		if v, ok := extra["recent_errors"]; ok {
			info.RecentErrors = extractStringSlice(v)
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

	if r.dbStore != nil {
		raw, _ := json.Marshal(info)
		if err := r.dbStore.UpsertWorker(raw); err != nil {
			log.Printf("registry: sqlite upsert worker heartbeat failed: %v", err)
		}
	}
	if r.useRedis && r.redis != nil {
		key := workerPrefix + workerID
		extraJSON, _ := json.Marshal(extra)
		return r.redis.HSet(ctx, key,
			"worker_id", workerID,
			"worker_name", workerName,
			"status", status,
			"last_heartbeat", now,
			"current_job", currentJob,
			"extra", string(extraJSON),
			"drain", strconv.FormatBool(info.Drain),
			"schedulable", strconv.FormatBool(info.Schedulable),
			"worker_group", info.WorkerGroup,
		).Err()
	}
	return nil
}

func (r *Registry) IsRegistered(ctx context.Context, workerID string) bool {
	if r.useRedis && r.redis != nil {
		n, _ := r.redis.Exists(ctx, workerPrefix+workerID).Result()
		return n > 0
	}
	r.mu.RLock()
	_, ok := r.inMem[workerID]
	r.mu.RUnlock()
	return ok
}

// GetWorker returns a single worker's info by ID
func (r *Registry) GetWorker(ctx context.Context, workerID string) *WorkerInfo {
	if r.useRedis && r.redis != nil {
		m, err := r.redis.HGetAll(ctx, workerPrefix+workerID).Result()
		if err != nil || len(m) == 0 {
			return nil
		}
		drain := m["drain"] == "true"
		schedulable := m["schedulable"] != "false"
		info := &WorkerInfo{
			WorkerID:    m["worker_id"],
			WorkerName:  m["worker_name"],
			Status:      m["status"],
			LastHB:      m["last_heartbeat"],
			CurrentJob:  m["current_job"],
			Drain:       drain,
			Schedulable: schedulable,
			WorkerGroup: m["worker_group"],
		}
		if extraRaw, ok := m["extra"]; ok && extraRaw != "" {
			var extra map[string]interface{}
			if err := json.Unmarshal([]byte(extraRaw), &extra); err == nil {
				info.RecentLogs = extractStringSlice(extra["recent_logs"])
				info.RecentErrors = extractStringSlice(extra["recent_errors"])
			}
		}
		return info
	}
	r.mu.RLock()
	info, ok := r.inMem[workerID]
	r.mu.RUnlock()
	if !ok {
		return nil
	}
	return &info
}

func (r *Registry) List(ctx context.Context) []WorkerInfo {
	if r.useRedis && r.redis != nil {
		var out []WorkerInfo
		iter := r.redis.Scan(ctx, 0, workerPrefix+"*", 100).Iterator()
		for iter.Next(ctx) {
			workerID := strings.TrimPrefix(iter.Val(), workerPrefix)
			if r.IsRevoked(workerID) {
				continue
			}
			m, _ := r.redis.HGetAll(ctx, iter.Val()).Result()
			if len(m) > 0 {
				info := WorkerInfo{
					WorkerID:   workerID,
					WorkerName: m["worker_name"],
					Status:     m["status"],
					LastHB:     m["last_heartbeat"],
					CurrentJob: m["current_job"],
				}
				if extraRaw, ok := m["extra"]; ok && extraRaw != "" {
					var extra map[string]interface{}
					if err := json.Unmarshal([]byte(extraRaw), &extra); err == nil {
						info.RecentLogs = extractStringSlice(extra["recent_logs"])
						info.RecentErrors = extractStringSlice(extra["recent_errors"])
					}
				}
				out = append(out, info)
			}
		}
		return out
	}
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

	r.inMem[workerID] = info
	if err := r.save(); err != nil {
		log.Printf("registry: failed to persist worker register: %v", err)
	}

	if r.dbStore != nil {
		raw, _ := json.Marshal(info)
		if err := r.dbStore.UpsertWorker(raw); err != nil {
			log.Printf("registry: sqlite upsert worker register failed: %v", err)
		}
	}

	if r.useRedis && r.redis != nil {
		key := workerPrefix + workerID
		return r.redis.HSet(ctx, key,
			"worker_id", workerID,
			"worker_name", workerName,
			"display_name", displayName,
			"status", "online",
			"last_heartbeat", now,
			"first_seen", firstSeen,
			"ip_address", ipAddress,
			"host", ipAddress,
			"schedulable", "true",
			"worker_group", workerGroup,
		).Err()
	}
	return nil
}

// UnregisterWorker removes a worker from the registry
func (r *Registry) UnregisterWorker(ctx context.Context, workerID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.inMem, workerID)

	if r.useRedis && r.redis != nil {
		return r.redis.Del(ctx, workerPrefix+workerID).Err()
	}
	return nil
}

// IsRevoked checks if a worker has been revoked
func (r *Registry) IsRevoked(workerID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.revoked[workerID]
}

// RevokeWorker marks a worker as revoked and removes it from the active set
func (r *Registry) RevokeWorker(ctx context.Context, workerID string) {
	r.mu.Lock()
	r.revoked[workerID] = true
	delete(r.inMem, workerID)
	if err := r.save(); err != nil {
		log.Printf("registry: failed to persist worker revoke: %v", err)
	}
	r.mu.Unlock()

	if r.useRedis && r.redis != nil {
		_ = r.redis.Del(ctx, workerPrefix+workerID).Err()
	}
}

// UnrevokeWorker removes a worker from the revoked list
func (r *Registry) UnrevokeWorker(workerID string) {
	r.mu.Lock()
	delete(r.revoked, workerID)
	if err := r.save(); err != nil {
		log.Printf("registry: failed to persist worker unrevoke: %v", err)
	}
	r.mu.Unlock()
}

// LoadRevoked loads a set of revoked worker IDs into the in-memory revoked set.
// This is used during bootstrap to persist revocation state across restarts.
func (r *Registry) LoadRevoked(ids []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, id := range ids {
		r.revoked[id] = true
		delete(r.inMem, id)
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

	if r.useRedis && r.redis != nil {
		key := workerPrefix + workerID
		fields := make([]interface{}, 0, len(updates)*2)
		for k, v := range updates {
			fields = append(fields, k, fmt.Sprintf("%v", v))
		}
		if len(fields) > 0 {
			return r.redis.HSet(ctx, key, fields...).Err()
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
				count++
				log.Printf("registry: cleaned up stale worker: %s (last seen %s)", id, w.LastHB)
			}
		}
	}

	if count > 0 {
		if err := r.save(); err != nil {
			log.Printf("registry: failed to persist stale worker cleanup: %v", err)
		}
	}

	return count
}

package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"velox-server/internal/logging"
	"velox-shared/identity"
)

// RegisterWorker registers a new worker or updates an existing one
func (r *Registry) RegisterWorker(ctx context.Context, workerID, workerName, ipAddress string, extra map[string]interface{}) error {
	workerID = identity.NormalizeWorkerID(workerID)
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

	// Persist to SQLite. RegisterWorker builds a fresh struct (no prior
	// SessionActive / ConnectionStatus) so no scrub is needed here.
	if r.dbStore != nil {
		raw, _ := json.Marshal(info)
		if err := r.dbStore.UpsertWorker(raw); err != nil {
			registryLog.ErrorWithMsg(logging.CodeSQLiteUpsertRegisterFail,
				"SQLite upsert worker register failed",
				map[string]interface{}{"worker_id": workerID, "err": err.Error()})
		}
	}
	return nil
}

// UnregisterWorker removes a worker from the registry
func (r *Registry) UnregisterWorker(ctx context.Context, workerID string) error {
	workerID = identity.NormalizeWorkerID(workerID)
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.inMem, workerID)

	if r.dbStore != nil {
		if err := r.dbStore.DeleteWorker(workerID); err != nil {
			registryLog.ErrorWithMsg(logging.CodeRegistryDeleteWorkerFail,
				"Failed to delete worker",
				map[string]interface{}{"worker_id": workerID, "err": err.Error()})
		}
	}
	return nil
}

// UpdateWorker updates specific fields of a worker
func (r *Registry) UpdateWorker(ctx context.Context, workerID string, updates map[string]interface{}) error {
	workerID = identity.NormalizeWorkerID(workerID)
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
		persisted := info
		ScrubForPersist(&persisted)
		raw, _ := json.Marshal(persisted)
		if err := r.dbStore.UpsertWorker(raw); err != nil {
			registryLog.ErrorWithMsg(logging.CodeSQLiteUpsertWorkerUpdateFail,
				"SQLite upsert worker update failed",
				map[string]interface{}{"worker_id": workerID, "err": err.Error()})
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

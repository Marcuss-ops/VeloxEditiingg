package workers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"velox-server/internal/logging"
	"velox-shared/controltransport"
	"velox-shared/identity"
)

// RegisterWorker registers a new worker or updates an existing one
func (r *Registry) RegisterWorker(ctx context.Context, workerID, workerName, ipAddress string, extra map[string]interface{}) error {
	workerID = identity.NormalizeWorkerID(workerID)
	id := identity.ParseWorkerID(workerID)
	now := time.Now().UTC().Format(time.RFC3339)

	r.mu.Lock()
	defer r.mu.Unlock()

	// Check if already registered (preserve first_seen, display_name, worker_group)
	existing, ok := r.inMem[id]
	firstSeen := now
	displayName := workerName
	workerGroup := ""
	var preservedDrain, preservedQuarantine, preservedResuming bool
	var preservedResumeOperationID string

	if ok {
		firstSeen = existing.FirstSeen
		preservedDrain = existing.Drain
		preservedQuarantine = existing.Quarantined
		preservedResuming = existing.Resuming
		preservedResumeOperationID = existing.ResumeOperationID
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

	info := Worker{
		WorkerID:          id,
		WorkerName:        workerName,
		DisplayName:       displayName,
		LastHB:            now,
		FirstSeen:         firstSeen,
		IPAddress:         ipAddress,
		Host:              ipAddress,
		Schedulable:       true,
		WorkerGroup:       workerGroup,
		Drain:             preservedDrain,
		Quarantined:       preservedQuarantine,
		Resuming:          preservedResuming,
		ResumeOperationID: preservedResumeOperationID,
	}
	applyMetadataFields(extra, &info)

	r.inMem[id] = info

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

	delete(r.inMem, identity.ParseWorkerID(workerID))

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
	id := identity.ParseWorkerID(workerID)
	r.mu.Lock()
	defer r.mu.Unlock()

	info, ok := r.inMem[id]
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
	if v, ok := updates["drain"].(bool); ok {
		if info.Resuming && v != info.Drain {
			return ErrWorkerResumeInFlight
		}
		info.Drain = v
	}
	if v, ok := updates["quarantine"].(bool); ok {
		if info.Resuming && v != info.Quarantined {
			return ErrWorkerResumeInFlight
		}
		// Step 6/15 fleet-operator: admin quarantine endpoint writes
		// via the map-driven path; SetWorkerQuarantine (typed helper)
		// is the canonical entry. Both paths converge on the same
		// flag. Persisted (NOT scrubbed in ScrubForPersist) so the
		// operator's quarantine decision survives a registry restart.
		info.Quarantined = v
	}
	if v, ok := updates["resuming"].(bool); ok {
		// RESUMING is a persisted, fail-closed scheduling gate. It is
		// set before the asynchronous smoke operation starts and cleared
		// only by ResumeExecutor after a terminal smoke outcome.
		info.Resuming = v
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
		// Decode legacy update payloads at the boundary; retain only the
		// canonical typed registry in the worker model.
		if registry, err := controltransport.ExecutorRegistryFromLegacyStrict(v); err == nil {
			info.ExecutorCapabilities = registry
		} else {
			info.ExecutorCapabilities = controltransport.EmptyExecutorRegistry()
		}
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
	r.inMem[id] = info

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

// SetWorkerQuarantine sets the quarantine flag for a worker.
// Step 6/15 fleet-operator: the admin POST /api/v1/admin/workers/{id}/
// /quarantine endpoint calls this synchronously so the placement
// matcher (costmodel.Score via GetEligibleWorkers) excludes the
// worker on the next match attempt.
//
// The flag is operator-persisted (NOT scrubbed in
// ScrubForPersist) so a registry restart preserves the
// quarantine decision — see worker_info.go:Worker.Quarantined
// doc comment. The companion UpdateWorker map key "quarantine"
// (added below in UpdateWorker) accepts the same flag from
// map-driven updates; this helper is the canonical typed-path
// entry point.
func (r *Registry) SetWorkerQuarantine(ctx context.Context, workerID string, quarantined bool) error {
	return r.UpdateWorker(ctx, workerID, map[string]interface{}{"quarantine": quarantined})
}

// ErrWorkerResumeInFlight is returned when a second resume admission races
// with an already active RESUMING gate.
var ErrWorkerResumeInFlight = errors.New("worker resume smoke gate is already in-flight")

// SetWorkerResuming marks the worker as undergoing a resume smoke gate.
// The flag is persisted so a master restart cannot accidentally make a
// worker eligible while the operation is still in flight. New code should
// use SetWorkerResumingIfClear/ClearWorkerResumingIfOwner for ownership.
func (r *Registry) SetWorkerResuming(ctx context.Context, workerID string, resuming bool) error {
	return r.UpdateWorker(ctx, workerID, map[string]interface{}{"resuming": resuming})
}

// ClearWorkerResumingIfOwner clears only a gate owned by operationID.
// Stale operation failure/cleanup cannot release a newer resume gate.
func (r *Registry) ClearWorkerResumingIfOwner(ctx context.Context, workerID, operationID string) error {
	workerID = identity.NormalizeWorkerID(workerID)
	id := identity.ParseWorkerID(workerID)
	r.mu.Lock()
	defer r.mu.Unlock()
	info, ok := r.inMem[id]
	if !ok {
		return fmt.Errorf("worker not found: %s", workerID)
	}
	if !info.Resuming || info.ResumeOperationID != operationID {
		return fmt.Errorf("resume operation %q no longer owns worker gate", operationID)
	}
	previous := info
	info.Resuming = false
	info.ResumeOperationID = ""
	info.LastHB = time.Now().UTC().Format(time.RFC3339)
	r.inMem[id] = info
	if r.dbStore != nil {
		persisted := info
		ScrubForPersist(&persisted)
		raw, _ := json.Marshal(persisted)
		if err := r.dbStore.UpsertWorker(raw); err != nil {
			// Keep the in-memory gate fail-closed when the durable
			// cleanup did not succeed; a retry must still have the
			// original owner and exclusion state available.
			r.inMem[id] = previous
			return err
		}
	}
	return nil
}

// SetWorkerResumingIfClear atomically claims the RESUMING gate. This closes
// the check-then-set race between concurrent admin requests: exactly one
// request can become responsible for publishing/executing the resume smoke.
func (r *Registry) SetWorkerResumingIfClear(ctx context.Context, workerID, operationID string) error {
	workerID = identity.NormalizeWorkerID(workerID)
	id := identity.ParseWorkerID(workerID)
	r.mu.Lock()
	defer r.mu.Unlock()
	info, ok := r.inMem[id]
	if !ok {
		return fmt.Errorf("worker not found: %s", workerID)
	}
	if info.Resuming {
		return ErrWorkerResumeInFlight
	}
	if operationID == "" {
		return errors.New("resume operation id required")
	}
	info.Resuming = true
	info.ResumeOperationID = operationID
	info.LastHB = time.Now().UTC().Format(time.RFC3339)
	r.inMem[id] = info
	if r.dbStore != nil {
		persisted := info
		ScrubForPersist(&persisted)
		raw, _ := json.Marshal(persisted)
		if err := r.dbStore.UpsertWorker(raw); err != nil {
			// Do not leave an in-memory gate that was never durably
			// claimed; the caller can safely retry the admission.
			info.Resuming = false
			r.inMem[id] = info
			return err
		}
	}
	return nil
}

// CompleteResume atomically clears the resume gate and the exclusion flags
// after a green Level D smoke. External drain/quarantine mutations are
// rejected while Resuming is true, so this transition cannot erase a
// concurrent operator decision. The operation ID prevents stale executors
// from completing a newer resume.
func (r *Registry) CompleteResume(ctx context.Context, workerID, operationID string) error {
	workerID = identity.NormalizeWorkerID(workerID)
	id := identity.ParseWorkerID(workerID)
	r.mu.Lock()
	defer r.mu.Unlock()
	info, ok := r.inMem[id]
	if !ok {
		return fmt.Errorf("worker not found: %s", workerID)
	}
	if !info.Resuming || info.ResumeOperationID != operationID {
		return fmt.Errorf("resume operation %q no longer owns worker gate", operationID)
	}
	previous := info
	info.Drain = false
	info.Quarantined = false
	info.Resuming = false
	info.ResumeOperationID = ""
	info.LastHB = time.Now().UTC().Format(time.RFC3339)
	r.inMem[id] = info
	if r.dbStore != nil {
		persisted := info
		ScrubForPersist(&persisted)
		raw, _ := json.Marshal(persisted)
		if err := r.dbStore.UpsertWorker(raw); err != nil {
			// Do not expose a healthy/eligible in-memory projection
			// when the durable completion failed. Restore the exact
			// owner and exclusion snapshot so the operation can retry.
			r.inMem[id] = previous
			return err
		}
	}
	return nil
}

// SetWorkerGroup sets the group for a worker
func (r *Registry) SetWorkerGroup(ctx context.Context, workerID string, group string) error {
	return r.UpdateWorker(ctx, workerID, map[string]interface{}{"worker_group": group})
}

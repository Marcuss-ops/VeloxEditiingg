package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"velox-server/internal/logging"
	"velox-shared/identity"
)

func (r *Registry) Heartbeat(ctx context.Context, workerID, workerName, status, currentJob string, extra map[string]interface{}) error {
	return r.HeartbeatWithSession(ctx, "", workerID, workerName, status, currentJob, extra)
}

// HeartbeatWithSession is the canonical heartbeat write path. The registry
// cache and all structured SQLite projections are committed from the same
// heartbeat snapshot; sessionID is the authenticated gRPC session when the
// caller has one.
func (r *Registry) HeartbeatWithSession(ctx context.Context, sessionID, workerID, workerName, status, currentJob string, extra map[string]interface{}) error {
	now := time.Now().UTC().Format(time.RFC3339)

	workerID = identity.NormalizeWorkerID(workerID)

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
		if v, ok := extra["active_jobs"]; ok {
			if info.Metrics == nil {
				info.Metrics = make(map[string]interface{})
			}
			info.Metrics["active_jobs"] = v
		}
		for _, key := range []string{"active_task_count", "active_jobs_count", "active_tasks", "task_slots", "cpu_utilization_ratio", "memory_used_bytes", "disk_free_bytes"} {
			if v, ok := extra[key]; ok {
				if info.Metrics == nil {
					info.Metrics = make(map[string]interface{})
				}
				info.Metrics[key] = v
			}
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

	// Persist to SQLite (single source of truth). ONLY heartbeat-derived
	// state is persisted; the read-time-hydrated SessionActive +
	// ConnectionStatus fields are scrubbed before UpsertWorker so a
	// cached WorkerInfo returned by a previous GetWorker cannot leak
	// its derived state into workers.raw_json (which would re-hydrate
	// stale across a registry restart).
	if r.dbStore != nil {
		persisted := info
		ScrubForPersist(&persisted)
		raw, _ := json.Marshal(persisted)
		if err := r.dbStore.PersistWorkerHeartbeat(ctx, raw, sessionID); err != nil {
			registryLog.ErrorWithMsg(logging.CodeSQLiteUpsertHeartbeatFail,
				"SQLite upsert worker heartbeat failed",
				map[string]interface{}{"worker_id": workerID, "err": err.Error()})
		}
	}
	return nil
}

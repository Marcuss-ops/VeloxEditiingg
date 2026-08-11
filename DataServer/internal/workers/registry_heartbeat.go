package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"velox-server/internal/logging"
	"velox-shared/identity"
)

func (r *Registry) Heartbeat(ctx context.Context, workerID, workerName, currentJob string, extra map[string]interface{}) error {
	return r.HeartbeatWithSession(ctx, "", workerID, workerName, currentJob, extra)
}

// HeartbeatWithSession is the canonical heartbeat write path. The registry
// cache and all structured SQLite projections are committed from the same
// heartbeat snapshot; sessionID is the authenticated gRPC session when the
// caller has one.
func (r *Registry) HeartbeatWithSession(ctx context.Context, sessionID, workerID, workerName, currentJob string, extra map[string]interface{}) error {
	now := time.Now().UTC().Format(time.RFC3339)

	workerID = identity.NormalizeWorkerID(workerID)
	id := identity.ParseWorkerID(workerID)

	r.mu.Lock()
	defer r.mu.Unlock()

	// Reject heartbeat for revoked workers
	if r.revoked[id] {
		return fmt.Errorf("worker %s is revoked", workerID)
	}

	// Preserve existing state unless explicitly updated by heartbeat payload.
	existing, hasExisting := r.inMem[id]

	info := Worker{
		WorkerID:    id,
		WorkerName:  workerName,
		LastHB:      now,
		CurrentJob:  currentJob,
		Schedulable: true,
	}
	if hasExisting {
		info = existing
		info.WorkerID = id
		if workerName != "" {
			info.WorkerName = workerName
		}
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
		for _, key := range []string{"resource_sample_present", "sampled_at", "ffmpeg_processes", "active_task_count", "active_jobs_count", "active_tasks", "task_slots", "cpu_utilization_ratio", "cpu_iowait_ratio", "cpu_steal_ratio", "memory_used_bytes", "memory_available_bytes", "disk_free_bytes", "disk_read_bytes_total", "disk_write_bytes_total", "load_average", "load1", "process_rss_bytes", "network_rx_bytes", "network_receive_bytes_total", "network_tx_bytes", "network_transmit_bytes_total"} {
			if v, ok := extra[key]; ok {
				if info.Metrics == nil {
					info.Metrics = make(map[string]interface{})
				}
				info.Metrics[key] = v
			}
		}
		if v, ok := int64FromHeartbeatExtra(extra, "jobs_completed"); ok {
			if info.Metrics == nil {
				info.Metrics = make(map[string]interface{})
			}
			info.Metrics["jobs_completed"] = v
		}
		if v, ok := int64FromHeartbeatExtra(extra, "jobs_failed"); ok {
			if info.Metrics == nil {
				info.Metrics = make(map[string]interface{})
			}
			info.Metrics["jobs_failed"] = v
		}
	}

	r.inMem[id] = info

	// Persist to SQLite (single source of truth). ONLY heartbeat-derived
	// state is persisted; the read-time-hydrated SessionActive +
	// ConnectionStatus fields are scrubbed before UpsertWorker so a
	// cached Worker returned by a previous GetWorker cannot leak
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
			return fmt.Errorf("persist worker heartbeat: %w", err)
		}
	}
	return nil
}

// int64FromHeartbeatExtra extracts an int64 from a heartbeat extra map.
// The gRPC heartbeat sets these values as int64, but JSON-decoded paths
// may surface them as float64, int, int32, int64, string or json.Number.
// Float values are truncated to whole integers (job counters are always whole).
func int64FromHeartbeatExtra(extra map[string]interface{}, key string) (int64, bool) {
	v, ok := extra[key]
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case float64:
		return int64(n), true
	case float32:
		return int64(n), true
	case string:
		i, err := strconv.ParseInt(n, 10, 64)
		return i, err == nil
	case json.Number:
		i, err := strconv.ParseInt(string(n), 10, 64)
		return i, err == nil
	}
	return 0, false
}

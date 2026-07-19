// Package api — PR 4 canonical worker endpoint handlers.
//
// Two endpoints:
//
//	GET /api/v1/workers           — list all workers with computed status
//	GET /api/v1/workers/:worker_id — get a single worker by ID
//
// Both read from the workers.Registry (the operational read model), NOT
// from the gRPC session map. The session map is a transient connection
// artefact; the Registry is the SQLite-backed authoritative source.
//
// Security: the response DTO deliberately excludes secret, credential_hash,
// TLS file paths, tokens, and raw IP addresses that could leak internal
// topology. See WorkerResponse for the whitelist of exposed fields.
package api

import (
	"net/http"
	"sort"
	"time"

	"github.com/gin-gonic/gin"
	workersreg "velox-server/internal/workers"
)

// WorkerResponse is the sanitized, operator-facing JSON shape for a single
// worker. It carries derived fields (status, reason, heartbeat_age_seconds,
// session_active) computed from the raw WorkerInfo. Fields intentionally
// EXCLUDED: credential hash, TLS file paths, worker secret, raw IP
// addresses, internal readiness blob.
//
// `status` is the canonical connection state
// (CONNECTED | STALE | DISCONNECTED | DRAINING) derived from the
// worker's heartbeat freshness AND whether the worker still has a
// non-revoked, non-expired auth session in `worker_sessions`.
//
// `reason` is the canonical reason code for non-CONNECTED states
// (RW-PROD-005 A2). Set to "drain" | "detached_session" |
// "heartbeat_stale" when status != CONNECTED; empty string otherwise.
//
// `session_active` is the raw boolean that drove the derivation:
// `true` when the worker has at least one valid session. Useful for
// dashboards that want to display "session lost, but heartbeat still
// recent" as a separate diagnostic state from outright DISCONNECTED.
type WorkerResponse struct {
	WorkerID            string              `json:"worker_id"`
	WorkerName          string              `json:"worker_name"`
	Status              string              `json:"status"`           // CONNECTED | STALE | DISCONNECTED | DRAINING
	Reason              string              `json:"reason,omitempty"` // drain | detached_session | heartbeat_stale | ""
	SessionActive       bool                `json:"session_active"`
	Hostname            string              `json:"hostname"`
	WorkerClass         string              `json:"worker_class,omitempty"`
	RolloutGroup        string              `json:"rollout_group,omitempty"`
	ProtocolVersion     string              `json:"protocol_version"`
	EngineVersion       string              `json:"engine_version,omitempty"`
	BundleVersion       string              `json:"bundle_version,omitempty"`
	ConnectedAt         string              `json:"connected_at,omitempty"`
	LastHeartbeatAt     string              `json:"last_heartbeat_at,omitempty"`
	HeartbeatAgeSeconds int64               `json:"heartbeat_age_seconds"`
	CurrentTaskID       string              `json:"current_task_id,omitempty"`
	ActiveTasks         int32               `json:"active_tasks"`
	ActiveTaskRuntime   []ActiveTaskRuntime `json:"active_task_runtime,omitempty"`
	TaskSlots           int32               `json:"task_slots"`
	CPUUtilizationRatio float64             `json:"cpu_utilization_ratio"`
	MemoryUsedBytes     int64               `json:"memory_used_bytes"`
	DiskFreeBytes       int64               `json:"disk_free_bytes"`
	JobsCompleted       int64               `json:"jobs_completed"`
	JobsFailed          int64               `json:"jobs_failed"`
	Executors           []ExecutorEntry     `json:"executors,omitempty"`
}

// ActiveTaskRuntime is the sanitized live projection. Definitive lifecycle
// state remains in tasks/task_attempts; this is only the worker's heartbeat
// view for operators.
type ActiveTaskRuntime struct {
	JobID       string `json:"job_id"`
	TaskID      string `json:"task_id,omitempty"`
	AttemptID   string `json:"attempt_id,omitempty"`
	Executor    string `json:"executor,omitempty"`
	Stage       string `json:"stage,omitempty"`
	Percent     int64  `json:"percent"`
	Scene       int64  `json:"scene"`
	TotalScenes int64  `json:"total_scenes"`
	LeaseID     string `json:"lease_id,omitempty"`
	StartedAt   string `json:"started_at,omitempty"`
}

// ExecutorEntry is a single executor advertised by a worker in its
// capabilities blob.
type ExecutorEntry struct {
	ID      string `json:"id"`
	Version int32  `json:"version"`
}

// WorkersListResponse wraps the array for the list endpoint.
type WorkersListResponse struct {
	Workers []WorkerResponse `json:"workers"`
}

// heartbeatAgeSeconds returns the number of seconds since last heartbeat,
// or 0 if the timestamp is unparseable.
func heartbeatAgeSeconds(lastHB string) int64 {
	if lastHB == "" {
		return 0
	}
	t, err := time.Parse(time.RFC3339, lastHB)
	if err != nil {
		return 0
	}
	age := time.Since(t).Seconds()
	if age < 0 {
		return 0
	}
	return int64(age)
}

// sanitizeWorker converts a raw workers.WorkerInfo into the operator-facing
// WorkerResponse, stripping all sensitive fields.
//
// Connection status: trust the registry's `WorkerInfo.ConnectionStatus`
// (CONNECTED | STALE | DISCONNECTED | DRAINING) since it merges heartbeat
// freshness with the canonical `session_active` signal from `worker_sessions`.
// The canonical `workers.ConnectionStatus` always returns one of the four
// enum strings on every read path (registry_query.go guarantees this),
// so no legacy/heartbeat-only fallback is needed.
func sanitizeWorker(w workersreg.WorkerInfo) WorkerResponse {
	resp := WorkerResponse{
		WorkerID:            w.WorkerID,
		WorkerName:          w.WorkerName,
		SessionActive:       w.SessionActive,
		Status:              w.ConnectionStatus,
		Reason:              w.Reason,
		Hostname:            w.Host,
		WorkerClass:         w.Class,
		RolloutGroup:        w.RolloutGroup,
		ProtocolVersion:     w.ProtocolVersion,
		EngineVersion:       w.EngineVersion,
		BundleVersion:       w.BundleVersion,
		ConnectedAt:         w.FirstSeen,
		LastHeartbeatAt:     w.LastHB,
		HeartbeatAgeSeconds: heartbeatAgeSeconds(w.LastHB),
		CurrentTaskID:       w.CurrentJob,
		Executors:           extractExecutors(w.Capabilities),
	}

	// Resource counters: extracted from the typed metrics map produced
	// by the gRPC heartbeat handler (registry_heartbeat.go stores the
	// proto WorkerResourceCounters fields under the "metrics" key).
	if m := w.Metrics; m != nil {
		if v, ok := intFromMap(m, "active_tasks"); ok {
			resp.ActiveTasks = int32(v)
		}
		if v, ok := intFromMap(m, "task_slots"); ok {
			resp.TaskSlots = int32(v)
		}
		if v, ok := floatFromMap(m, "cpu_utilization_ratio"); ok {
			resp.CPUUtilizationRatio = v
		}
		if v, ok := intFromMap(m, "memory_used_bytes"); ok {
			resp.MemoryUsedBytes = v
		}
		if v, ok := intFromMap(m, "disk_free_bytes"); ok {
			resp.DiskFreeBytes = v
		}
		if v, ok := intFromMap(m, "jobs_completed"); ok {
			resp.JobsCompleted = v
		}
		if v, ok := intFromMap(m, "jobs_failed"); ok {
			resp.JobsFailed = v
		}
		if raw, ok := m["active_jobs"].([]interface{}); ok {
			for _, item := range raw {
				if task, ok := item.(map[string]interface{}); ok {
					resp.ActiveTaskRuntime = append(resp.ActiveTaskRuntime, ActiveTaskRuntime{
						JobID: asString(task["job_id"]), TaskID: asString(task["task_id"]),
						AttemptID: asString(task["attempt_id"]), Executor: asString(task["job_type"]),
						Stage: asString(task["progress_stage"]), Percent: intFromAny(task["progress_percent"]),
						Scene: intFromAny(task["progress_scene"]), TotalScenes: intFromAny(task["progress_total"]),
						LeaseID: asString(task["lease_id"]), StartedAt: asString(task["started_at"]),
					})
				}
			}
		}
	}

	return resp
}

// extractExecutors pulls the canonical executor list from the worker's
// capabilities map. Supports both the proto-structured form
// ("executors": [{"id":"...","version":1}]) and the flat-map form.
func extractExecutors(caps map[string]interface{}) []ExecutorEntry {
	if caps == nil {
		return nil
	}
	// Proto-structured form: {"executors": [{"id":"...","version":1}]}
	if raw, ok := caps["executors"]; ok {
		switch list := raw.(type) {
		case []interface{}:
			out := make([]ExecutorEntry, 0, len(list))
			for _, item := range list {
				m, ok := item.(map[string]interface{})
				if !ok {
					continue
				}
				id, _ := m["id"].(string)
				if id == "" {
					continue
				}
				var ver int32
				if v, ok := floatFromGeneric(m["version"]); ok {
					ver = int32(v)
				}
				out = append(out, ExecutorEntry{ID: id, Version: ver})
			}
			return out
		}
	}
	return nil
}

// floatFromGeneric handles JSON-unmarshalled numeric values that may be
// float64, json.Number, or int types.
func floatFromGeneric(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	}
	return 0, false
}

// intFromMap extracts an int64 from a map with numeric-type tolerance.
func intFromMap(m map[string]interface{}, key string) (int64, bool) {
	v, ok := m[key]
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	}
	return 0, false
}

func asString(v interface{}) string {
	s, _ := v.(string)
	return s
}

func intFromAny(v interface{}) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	case int32:
		return int64(n)
	}
	return 0
}

// floatFromMap extracts a float64 from a map with numeric-type tolerance.
func floatFromMap(m map[string]interface{}, key string) (float64, bool) {
	v, ok := m[key]
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case float64:
		return n, true
	case int64:
		return float64(n), true
	case int:
		return float64(n), true
	}
	return 0, false
}

// WorkersHandler holds the dependency on the worker registry.
type WorkersHandler struct {
	reg *workersreg.Registry
}

// NewWorkersHandler creates a WorkersHandler wired to the Registry read model.
func NewWorkersHandler(reg *workersreg.Registry) *WorkersHandler {
	return &WorkersHandler{reg: reg}
}

// ListWorkers returns GET /api/v1/workers — a sanitized JSON array of all
// registered workers with computed status.
func (h *WorkersHandler) ListWorkers() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h.reg == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "worker registry not available"})
			return
		}
		list := h.reg.List(c.Request.Context())
		resp := WorkersListResponse{
			Workers: make([]WorkerResponse, 0, len(list)),
		}
		for _, w := range list {
			resp.Workers = append(resp.Workers, sanitizeWorker(w))
		}
		// Stable order: sort by worker_id so dashboards don't flicker.
		sort.Slice(resp.Workers, func(i, j int) bool {
			return resp.Workers[i].WorkerID < resp.Workers[j].WorkerID
		})
		c.JSON(http.StatusOK, resp)
	}
}

// GetWorker returns GET /api/v1/workers/:worker_id — a single sanitized
// worker or 404.
func (h *WorkersHandler) GetWorker() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h.reg == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "worker registry not available"})
			return
		}
		workerID := c.Param("worker_id")
		if workerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id is required"})
			return
		}
		info := h.reg.GetWorker(c.Request.Context(), workerID)
		if info == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "worker not found"})
			return
		}
		c.JSON(http.StatusOK, sanitizeWorker(*info))
	}
}

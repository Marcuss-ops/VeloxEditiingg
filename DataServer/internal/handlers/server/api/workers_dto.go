// Package api — RW-PROD-005 DTO shapes (read-model projection).
//
// workers_dto.go owns ALL DTO shapes the operator-facing workers endpoint
// emits after the canonical-state migration. The split into a dedicated
// file keeps workers_handler.go readable: the existing handler's
// responsibility is the route + handler logic; the new mapper file
// owns the conversion/sanitization/parsing surface; this file owns
// the value-types and the canonical enum constants.
//
// SECURITY posture (canonical, see OWNERSHIP.md §3):
//   - HostSummary deliberately omits Hostname when it would leak IPv4/IPv6;
//     the workers_mapper.sanitiseHostname() helper replaces offending
//     patterns with "[redacted-...]" tokens.
//   - Bundle_hash / credential_hash / TLS file paths / worker secret /
//     raw IP addresses of any kind are NEVER carried into the response.
//   - Reasons are an enumerated 3-element taxonomy (see Reason* consts);
//     ad-hoc string literals must not be added at call sites.
package api

import (
	workersreg "velox-server/internal/workers"
)

// WorkerInfo aliases the canonical registry read-model type so this package
// can refer to the worker shape consistently across build, vet, and tests.
type WorkerInfo = workersreg.WorkerInfo

// Re-export ConnectionStaleThreshold so the canonical reason is
// computed against the same threshold the registry uses for STALE.
// Drift would create a window where the API reports reason=heartbeat_stale
// but status=CONNECTED — operators would have to look up the threshold
// definition to make sense of the inconsistency.
var ConnectionStaleThreshold = workersreg.ConnectionStaleThreshold

// Reason canonical taxonomy (RW-PROD-005 §2.2).
//
//	drain             — Drain=true overrides everything (precedence 1).
//	detached_session  — session_active=false (stream closed), all other
//	                    signals ignored (precedence 2). Mirrors spec
//	                    "Stream chiuso → detached_session senza
//	                    aspettare 150s".
//	heartbeat_stale   — session_active=true but last_heartbeat is stale,
//	                    empty, or unparseable. With a fresh session the
//	                    canonical state is STALE (150s-5min). With an old
//	                    session the state is DISCONNECTED but the reason
//	                    is still heartbeat_stale (the session is up but
//	                    the heartbeat has stopped).
//	""                — fresh: status=CONNECTED, no reason emitted.
const (
	ReasonDrain           = "drain"
	ReasonDetachedSession = "detached_session"
	ReasonHeartbeatStale  = "heartbeat_stale"
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

// HostSummary is the sanitized host-side metadata exposed in the
// operator-facing WorkerResponse. NO IPs, NO creds, NO cert paths,
// NO worker secret. Hostname goes through sanitiseHostname() which
// replaces IPv4/IPv6/secret-looking/absolute-path patterns with
// "[redacted-...]" — the path-filter is the defense-in-depth surface
// against a future operator setting WorkerGroup to a directory literal
// (an Ansible pragmatic mistake that the fuzz test catches). Numeric
// resource counters are exposed because they are operator-observability
// signals and have no PII value.
type HostSummary struct {
	Hostname        string `json:"hostname"`        // sanitised via sanitiseHostname()
	CPUCount        int    `json:"cpu_count"`       // runtime.NumCPU()
	HasGPU          bool   `json:"has_gpu"`         // sampler-derived
	RAMBytes        int64  `json:"ram_bytes"`       // sampler-derived
	DiskFreeBytes   int64  `json:"disk_free_bytes"` // sampler-derived (snapshot, not realtime)
	MaxParallelJobs int32  `json:"max_parallel_jobs"`
}

// ExecutorSummary is a single executor descriptor flattened from the
// capabilities blob. ResourceClass is included so a
// dispatcher can render "velox-worker-fleet: 4×cpu + 2×gpu" without
// re-decoding the full capabilities map.
type ExecutorSummary struct {
	ID            string `json:"id"`
	Version       int32  `json:"version"`
	ResourceClass string `json:"resource_class,omitempty"`
}

// TaskSummary is the per-worker current_task projection. Empty when
// the worker has no RUNNING TaskAttempt. Race-tolerant: LoadCurrentTask
// is called from the handler read path with row-level locking via the
// task_attempts table primary index, so concurrent TaskAttempt updates
// surface as either (RUNNING, task X) or (no row) for at most one tick.
type TaskSummary struct {
	TaskID    string `json:"task_id"`
	JobID     string `json:"job_id"`
	Executor  string `json:"executor,omitempty"` // e.g. "scene.composite.v1@1"
	Status    string `json:"status,omitempty"`   // always "RUNNING" today
	StartedAt string `json:"started_at,omitempty"`
}

// Filter canonical status enum (case-insensitive accepted by ParseFilters,
// canonical case emitted). Mirrors the canonical ConnectionStatus
// taxonomy so the operator-facing list endpoint and the diagnostic
// reason code never disagree on what the four states are.
const (
	FilterStatusConnected    = "CONNECTED"
	FilterStatusStale        = "STALE"
	FilterStatusDisconnected = "DISCONNECTED"
	FilterStatusDraining     = "DRAINING"
)

// Filters is the typed result of ParseFilters. Empty fields mean
// "no filter on this dimension" (the corresponding GET param was
// absent).
//
// Encoding matches the lowercase class / rollout_group carried on
// the worker_info table; status is one of the four CONNECTED|STALE|
// DISCONNECTED|DRAINING enum strings (case-insensitive accepted,
// canonical case emitted).
type Filters struct {
	Class         string
	Status        string
	RolloutGroup  string
	NeedsExecutor string
}

// IsZero returns true iff every filter field is empty — i.e. the
// caller did not pass any GET param. Handler uses this to skip the
// applier entirely when the request is the unfiltered list.
func (f Filters) IsZero() bool {
	return f.Class == "" && f.Status == "" && f.RolloutGroup == "" && f.NeedsExecutor == ""
}

// =====================================================================
// DTOs for the per-worker metrics / sessions / events endpoints
// (workers_metrics_handler.go, workers_sessions_handler.go,
// workers_events_handler.go).
// =====================================================================

// MetricSampleResponse is the sanitized, operator-facing JSON shape
// for a single worker_metric_samples row.
//
// Field set mirrors the schema (migration 094):
//   - sampled_at, connection_status, active_tasks, task_slots,
//     cpu_utilization_ratio, memory_used_bytes, disk_free_bytes
//     are NOT NULL at the schema level and are surfaced unconditionally.
//   - session_id is the originating heartbeat session; surfaced for
//     audit correlation but omitempty so the rare NULL row renders
//     as a clean omission.
//   - load_average / process_rss_bytes / network_rx_bytes /
//     network_tx_bytes are nullable at the schema level. The handler
//     renders them as 0 / null / omitted via the *float64 / *int64
//     pointer types so a NULL row carries no false-positive signal.
//
// No sensitive fields exist on this DTO (no IPs, no creds, no paths);
// the time-series counters are pure operator-observability signals.
type MetricSampleResponse struct {
	SampledAt           string   `json:"sampled_at"`
	SessionID           string   `json:"session_id,omitempty"`
	ConnectionStatus    string   `json:"connection_status"`
	ActiveTasks         int64    `json:"active_tasks"`
	TaskSlots           int64    `json:"task_slots"`
	CPUUtilizationRatio float64  `json:"cpu_utilization_ratio"`
	MemoryUsedBytes     int64    `json:"memory_used_bytes"`
	DiskFreeBytes       int64    `json:"disk_free_bytes"`
	LoadAverage         *float64 `json:"load_average,omitempty"`
	ProcessRSSBytes     *int64   `json:"process_rss_bytes,omitempty"`
	NetworkRxBytes      *int64   `json:"network_rx_bytes,omitempty"`
	NetworkTxBytes      *int64   `json:"network_tx_bytes,omitempty"`
}

// WorkerMetricsListResponse wraps the metrics array for
// GET /api/v1/workers/:worker_id/metrics. `WorkerID` is echoed back
// so dashboards consuming the JSON can correlate the response with
// the request without parsing the URL.
type WorkerMetricsListResponse struct {
	WorkerID string                 `json:"worker_id"`
	Count    int                    `json:"count"`
	Metrics  []MetricSampleResponse `json:"metrics"`
}

// SessionResponse is the sanitized, operator-facing JSON shape for a
// single worker_sessions row.
//
// SECURITY posture (canonical, see OWNERSHIP.md §3):
//   - token_hash is NEVER carried into the response (it is the SHA-256
//     of the bearer token used by the worker). The store-layer
//     ListWorkerSessions helper omits the column from the SELECT so
//     a future regression that adds it back to the row struct will
//     fail to compile.
//   - ip_address IS surfaced but MUST go through sanitiseHostname()
//     at the handler boundary (IPv4/IPv6/long-hex redaction).
//   - session_id is the canonical opaque identifier; operators use
//     it to correlate with the audit ledger.
//   - disconnect_reason is surfaced verbatim; it can contain an
//     operator-configured string ("replaced", "migration_normalized",
//     "expired") and is NOT considered sensitive.
//
// session_type is the canonical "control" | "asset" enum from
// migration 095. status is one of "ACTIVE" | "DISCONNECTED" |
// "REVOKED" | "EXPIRED".
type SessionResponse struct {
	SessionID        string `json:"session_id"`
	WorkerID         string `json:"worker_id"`
	SessionType      string `json:"session_type"` // control | asset
	Status           string `json:"status"`       // ACTIVE | DISCONNECTED | REVOKED | EXPIRED
	IPAddress        string `json:"ip_address"`   // sanitised via sanitiseHostname()
	Revoked          bool   `json:"revoked"`
	ProtocolVersion  string `json:"protocol_version"`
	BundleVersion    string `json:"bundle_version,omitempty"`
	CreatedAt        string `json:"created_at"`
	ExpiresAt        string `json:"expires_at"`
	ConnectedAt      string `json:"connected_at,omitempty"`
	LastSeenAt       string `json:"last_seen_at,omitempty"`
	DisconnectedAt   string `json:"disconnected_at,omitempty"`
	DisconnectReason string `json:"disconnect_reason,omitempty"`
}

// WorkerSessionsListResponse wraps the sessions array for
// GET /api/v1/workers/:worker_id/sessions.
type WorkerSessionsListResponse struct {
	WorkerID string            `json:"worker_id"`
	Count    int               `json:"count"`
	Sessions []SessionResponse `json:"sessions"`
}

// EventResponse is the sanitized, operator-facing JSON shape for a
// single worker_events row.
//
// SECURITY posture:
//   - details_json is the raw audit detail blob; the handler parses
//     it into a map[string]any and routes every string value through
//     sanitiseHostname() so an embedded IP / path / long hex cannot
//     leak. Parsing failures fall back to the raw string (still
//     sanitised) so the audit ledger is never silently dropped.
//   - event_type / severity / reason_code are the canonical
//     operators-controlled taxonomy; surfaced verbatim.
//
// No token hashes or credentials live on this table.
type EventResponse struct {
	EventID    string         `json:"event_id"`
	WorkerID   string         `json:"worker_id,omitempty"`
	SessionID  string         `json:"session_id,omitempty"`
	JobID      string         `json:"job_id,omitempty"`
	TaskID     string         `json:"task_id,omitempty"`
	AttemptID  string         `json:"attempt_id,omitempty"`
	EventType  string         `json:"event_type"`
	Severity   string         `json:"severity"`
	ReasonCode string         `json:"reason_code,omitempty"`
	Details    map[string]any `json:"details,omitempty"`
	CreatedAt  string         `json:"created_at"`
}

// WorkerEventsListResponse wraps the events array for
// GET /api/v1/workers/:worker_id/events.
type WorkerEventsListResponse struct {
	WorkerID string          `json:"worker_id"`
	Count    int             `json:"count"`
	Events   []EventResponse `json:"events"`
}

// Default + max page size for the new per-worker endpoints. Centralised
// here so the three handlers share the same clamp logic and a future
// operator can tune the cap by editing one constant.
const (
	DefaultListLimit = 100
	MaxListLimit     = 1000
)

// clampLimit clamps a user-supplied limit query param into
// [1, MaxListLimit], defaulting to DefaultListLimit when the value
// is missing, non-numeric, or non-positive.
func clampLimit(raw string) int {
	if raw == "" {
		return DefaultListLimit
	}
	n := 0
	for _, c := range raw {
		if c < '0' || c > '9' {
			return DefaultListLimit
		}
		n = n*10 + int(c-'0')
	}
	if n < 1 {
		return DefaultListLimit
	}
	if n > MaxListLimit {
		return MaxListLimit
	}
	return n
}

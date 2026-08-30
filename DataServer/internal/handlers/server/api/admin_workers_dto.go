// Package api — Step 1/15 of the fleet-operator rollout: the canonical
// worker card DTO.
//
// The shape is intentionally distinct from the diagnostic WorkerResponse
// (PR 4) — the admin card trades diagnostic depth for the fields a fleet
// dashboard needs to decide when to drain, when to update, and when to
// roll back. The two surfaces stay co-resident so a reformat of the
// diagnostic response cannot accidentally leak into the admin endpoint.
//
// SECURITY posture (mirrors WorkerResponse, see OWNERSHIP.md §3):
//
//   - No secret, credential_hash, TLS file paths, worker secret, raw
//     IPv4/IPv6 of internal interfaces.
//   - `hostname` and `host` go through `sanitiseHostname()` (see
//     workers_sanitise.go) which redacts IP addresses, long hex strings
//     (≥40-char SHA halves), and credential paths before the value
//     lands in the response.
//   - `executor` and `executor_version` are flattened from the
//     canonical `Worker.ExecutorCapabilities` registry — the same
//     typed source used by dispatch, so the operator projection cannot
//     drift from master capability admission.
package api

import "time"

// WorkerCard is the canonical fleet-operator-facing JSON shape for a
// single registered worker.
//
// Schema policy. Fields already in the registry read model
// (`workers.Worker`) are populated by `buildWorkerCard` (see
// admin_workers_handler.go). Runtime identity and host telemetry are
// populated from worker heartbeat metadata. Deployment and smoke-ledger
// fields remain optional until their corresponding operation has produced
// a record.
//
// SOURCE MAPPING (see `buildWorkerCard` for the canonical impl):
//
//	worker_id         Worker.WorkerID       (post-NormalizeWorkerID)
//	worker_name       Worker.WorkerName     (operator-facing mutable name)
//	hostname          sanitiseHostname(Worker.WorkerName)
//	host              sanitiseHostname(Worker.IPAddress)
//	status            Worker.ConnectionStatus  (canonical enum)
//	session_active    Worker.SessionActive  (post-hydration)
//	executor          Worker.ExecutorCapabilities.All()[0].ID
//	executor_version  Worker.ExecutorCapabilities.All()[0].Version
//	software_version  Worker.CodeVersion     (worker-reported code)
//	last_heartbeat_at Worker.LastHB
//	active_jobs       Worker.Capacity.ActiveSlots (compatibility alias)
//	max_active_jobs   Worker.Capacity.MaxSlots (compatibility alias)
//
// `image_digest`, `desired_version`, and `deployment_state` come from
// heartbeat metadata. `health` is derived by the registry. `image_state`
// and `operation_state` are the canonical typed digest views: real-time
// image match (separate from rollout history) and the last deployment
// ledger row respectively. Smoke and restart timestamps remain empty
// unless an operation ledger supplies them.
//
// `software_version` is intentionally mapped to CodeVersion (NOT
// BundleVersion) because the operator's dashboard question is
// "what software is the worker running right now" — the worker-
// reported code version — not "what bundle was the master staging".
// BundleVersion remains available through the diagnostic
// /api/v1/workers endpoint if the operator needs the staging context.
//
// `last_restart_at` is intentionally LEFT EMPTY (no BootTS fallback)
// because `Worker.BootTS` is "when the worker process started",
// which is semantically distinct from "when the Fleet Controller
// restarted the worker". Using BootTS here would mislead the operator
// when the worker self-restarts due to a crash; the field's semantic
// contract belongs to step §6 of the rollout.
type WorkerCard struct {
	WorkerID             string `json:"worker_id"`
	WorkerName           string `json:"worker_name"`
	Hostname             string `json:"hostname"`
	Host                 string `json:"host"`
	Status               string `json:"status"`
	ConnectionState      string `json:"connection_state"`
	SchedulingState      string `json:"scheduling_state"`
	DeploymentState      string `json:"deployment_state,omitempty"`
	HealthState          string `json:"health_state"`
	SessionActive        bool   `json:"session_active"`
	Executor             string `json:"executor"`
	ExecutorVersion      int32  `json:"executor_version"`
	ImageDigest          string `json:"image_digest,omitempty"`
	RunningDigest        string `json:"running_digest,omitempty"`
	DesiredDigest        string `json:"desired_digest,omitempty"`
	TargetDigest         string `json:"target_digest,omitempty"`
	PreviousDigest       string `json:"previous_digest,omitempty"`
	LastSuccessfulDigest string `json:"last_successful_digest,omitempty"`
	LastPhase            string `json:"last_phase,omitempty"`
	// InSync reports whether the worker is ACTUALLY running what the fleet
	// WANTS (desired_digest == running_digest, compared by digest part). It is
	// computed from the read model — never persisted and never reconstructed
	// from the journal — so a FAILED rollout leaves DESIRED=B / RUNNING=A
	// visible as in_sync=false (drift cannot be hidden). Empty desired or
	// running digests (no state row / no heartbeat yet) yield false: an
	// unknown digest is not a matching digest.
	InSync bool `json:"in_sync"` // LastOperation is the CURRENT fleet operation, straight from the read
	// model (worker_deployment_state.last_operation_*): the last operation the
	// control plane actually recorded, its status and its failure. It is the
	// operation view of the SAME read model that drives the digest fields —
	// never a reconstruction from deployment_records history. Absent while no
	// operation has been recorded for the worker. This nested object is the
	// canonical current-operation view; the flat LastOperationErrorCode /
	// LastOperationError fields below are retained for backward compatibility
	// with the migration-153 contract.
	LastOperation          *WorkerLastOperation  `json:"last_operation,omitempty"`
	LastOperationErrorCode string                `json:"last_operation_error_code,omitempty"`
	LastOperationError     string                `json:"last_operation_error,omitempty"`
	ImageState             *WorkerImageState     `json:"image_state,omitempty"`
	OperationState         *WorkerOperationState `json:"operation_state,omitempty"`
	SoftwareVersion        string                `json:"software_version"`
	DesiredVersion         string                `json:"desired_version,omitempty"`
	LastHeartbeatAt        string                `json:"last_heartbeat_at,omitempty"`
	ActiveJobs             int32                 `json:"active_jobs"`     // compatibility alias for active_slots
	MaxActiveJobs          int32                 `json:"max_active_jobs"` // compatibility alias for max_slots
	ActiveSlots            int32                 `json:"active_slots"`
	MaxSlots               int32                 `json:"max_slots"`
	AvailableSlots         int32                 `json:"available_slots"`
	// Per-phase slot limits from the CapacityScorecard.
	RenderSlots         int32   `json:"render_slots,omitempty"`
	PrefetchSlots       int32   `json:"prefetch_slots,omitempty"`
	PublisherSlots      int32   `json:"publisher_slots,omitempty"`
	ActiveRender        int32   `json:"active_render,omitempty"`
	ActivePrefetch      int32   `json:"active_prefetch,omitempty"`
	ActivePublisher     int32   `json:"active_publisher,omitempty"`
	LimitingResource    string  `json:"limiting_resource,omitempty"`
	CPUUtilizationRatio float64 `json:"cpu_utilization_ratio,omitempty"`
	MemoryUsedBytes     int64   `json:"memory_used_bytes,omitempty"`
	DiskFreeBytes       int64   `json:"disk_free_bytes,omitempty"`
	Load1               float64 `json:"load1,omitempty"`
	CurrentJob          string  `json:"current_job,omitempty"`
	Health              string  `json:"health,omitempty"`
	LastSmokeStatus     string  `json:"last_smoke_status,omitempty"`
	LastSmokeAt         string  `json:"last_smoke_at,omitempty"`
	LastRestartAt       string  `json:"last_restart_at,omitempty"`
	// Readiness and Runtime are worker-reported diagnostic snapshots. They
	// are optional because older agents do not publish these dimensions yet;
	// absence is explicit and must not be interpreted as PASS.
	Readiness    map[string]any `json:"readiness,omitempty"`
	Runtime      map[string]any `json:"runtime,omitempty"`
	RecentErrors []string       `json:"recent_errors,omitempty"`

	// ActiveTaskRuntime exposes per-active-job live progress from the
	// worker heartbeat. Each entry carries phase, percent, scene/segment,
	// frames, speed, elapsed and last_progress_at — the same data the
	// diagnostic /api/v1/workers endpoint already surfaces.
	ActiveTaskRuntime []ActiveTaskRuntime `json:"active_task_runtime,omitempty"`
}

// WorkerImageState is the canonical real-time image state of a worker:
// the digest actually running vs. the digest the fleet wants, and whether
// they match (digest-part comparison — a pinned ref and a bare digest of
// the same image compare equal). It deliberately contains NO
// operation-history fields — an old FAILED rollout must not make a worker
// with a matching running digest appear unhealthy.
type WorkerImageState struct {
	RunningDigest string `json:"running_digest"`
	TargetDigest  string `json:"target_digest"`
	Match         bool   `json:"digest_match"`
}

// WorkerLastOperation is the read-model view of the last fleet operation.
// It is deliberately separate from WorkerOperationState (the journal
// history view): this struct mirrors worker_deployment_state.last_operation_*
// so the dashboard sees the CURRENT operation the read model knows about
// — same source as desired/running/last_successful — with the stable error
// code split from the human-readable message.
type WorkerLastOperation struct {
	ID        string `json:"id"`
	Kind      string `json:"kind,omitempty"` // update | rollback
	Status    string `json:"status"`
	ErrorCode string `json:"error_code,omitempty"`
	Error     string `json:"error,omitempty"`
}

// WorkerOperationState preserves the last deployment ledger row as an
// operation-history view. It is deliberately separate from
// WorkerImageState: status here describes the OPERATION (update/rollback
// cascade), never the worker's current image health. ErrorCode comes from
// the journal row (deployment_records.error_code, migration 153); Error
// carries the failure reason when the operation failed.
type WorkerOperationState struct {
	OperationID string     `json:"operation_id"`
	Type        string     `json:"type"` // update | rollback
	Status      string     `json:"status"`
	ErrorCode   string     `json:"error_code,omitempty"`
	Error       string     `json:"error,omitempty"`
	StartedAt   string     `json:"started_at"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
}

// AdminWorkersListResponse is the JSON envelope for
// GET /api/v1/admin/workers. The `count` field is a convenience for
// dashboards; `len(workers)` is the canonical count and the two MUST
// agree on every response (handler-side invariant, asserted by test).
type AdminWorkersListResponse struct {
	Count   int          `json:"count"`
	Workers []WorkerCard `json:"workers"`
}

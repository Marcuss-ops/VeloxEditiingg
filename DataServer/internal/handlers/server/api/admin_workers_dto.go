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
// heartbeat metadata. `health` is derived by the registry. Smoke and
// restart timestamps remain empty unless an operation ledger supplies them.
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
	WorkerID            string  `json:"worker_id"`
	WorkerName          string  `json:"worker_name"`
	Hostname            string  `json:"hostname"`
	Host                string  `json:"host"`
	Status              string  `json:"status"`
	ConnectionState     string  `json:"connection_state"`
	SchedulingState     string  `json:"scheduling_state"`
	DeploymentState     string  `json:"deployment_state,omitempty"`
	HealthState         string  `json:"health_state"`
	SessionActive       bool    `json:"session_active"`
	Executor            string  `json:"executor"`
	ExecutorVersion     int32   `json:"executor_version"`
	ImageDigest         string  `json:"image_digest,omitempty"`
	TargetDigest        string  `json:"target_digest,omitempty"`
	PreviousDigest      string  `json:"previous_digest,omitempty"`
	DigestState         string  `json:"digest_state,omitempty"`
	SoftwareVersion     string  `json:"software_version"`
	DesiredVersion      string  `json:"desired_version,omitempty"`
	LastHeartbeatAt     string  `json:"last_heartbeat_at,omitempty"`
	ActiveJobs          int32   `json:"active_jobs"`     // compatibility alias for active_slots
	MaxActiveJobs       int32   `json:"max_active_jobs"` // compatibility alias for max_slots
	ActiveSlots         int32   `json:"active_slots"`
	MaxSlots            int32   `json:"max_slots"`
	AvailableSlots      int32   `json:"available_slots"`
	CPUUtilizationRatio float64 `json:"cpu_utilization_ratio,omitempty"`
	MemoryUsedBytes     int64   `json:"memory_used_bytes,omitempty"`
	DiskFreeBytes       int64   `json:"disk_free_bytes,omitempty"`
	Load1               float64 `json:"load1,omitempty"`
	CurrentJob          string  `json:"current_job,omitempty"`
	Health              string  `json:"health,omitempty"`
	LastSmokeStatus     string  `json:"last_smoke_status,omitempty"`
	LastSmokeAt         string  `json:"last_smoke_at,omitempty"`
	LastRestartAt       string  `json:"last_restart_at,omitempty"`
}

// AdminWorkersListResponse is the JSON envelope for
// GET /api/v1/admin/workers. The `count` field is a convenience for
// dashboards; `len(workers)` is the canonical count and the two MUST
// agree on every response (handler-side invariant, asserted by test).
type AdminWorkersListResponse struct {
	Count   int          `json:"count"`
	Workers []WorkerCard `json:"workers"`
}

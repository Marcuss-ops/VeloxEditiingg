// Package api provides HTTP client for communicating with the Velox Master server.
package api

import (
	obs "velox-shared/obs"
)

// API event names for structured logging. The cross-component transport
// codes (Retry/Error/Success) are aliased to pkg/obs so the canonical
// owners live there — pkg/api just re-exports the typed EventCode values.
// Local-only codes (Request, Fallback) stay defined directly here.
const (
	EventAPIRequest  = "API_REQUEST"
	EventAPIFallback = "API_FALLBACK"

	// Aliases to pkg/obs.EventCode constants.
	EventAPIRetry   = obs.EventAPIRetry
	EventAPIError   = obs.EventAPIError
	EventAPISuccess = obs.EventAPISuccess

	// ContractVersionV2 is the current worker/master job contract.
	ContractVersionV2 = 2
)

// WorkerInfo represents worker identification sent to the master.
type WorkerInfo struct {
	WorkerID        string                 `json:"worker_id"`
	WorkerName      string                 `json:"worker_name"`
	Capabilities    map[string]interface{} `json:"capabilities"`
	Hostname        string                 `json:"hostname"`
	IP              string                 `json:"ip"`
	Version         string                 `json:"version"`
	CodeVersion     string                 `json:"code_version,omitempty"`
	BundleVersion   string                 `json:"bundle_version,omitempty"`
	BundleHash      string                 `json:"bundle_hash,omitempty"`
	ProtocolVersion string                 `json:"protocol_version,omitempty"`
	EngineVersion   string                 `json:"engine_version,omitempty"`
	Credential      string                 `json:"credential,omitempty"`
}

// JobRequest represents a request to get a job from the master.
type JobRequest struct {
	WorkerID string `json:"worker_id"`
}

// Job represents a job returned by the master.
type Job struct {
	JobID           string                 `json:"job_id"`
	JobRunID        string                 `json:"job_run_id"`
	JobType         string                 `json:"job_type"`
	Priority        int                    `json:"priority"`
	Parameters      map[string]interface{} `json:"parameters"`
	CreatedAt       interface{}            `json:"created_at"`
	TimeoutSecs     int                    `json:"timeout_secs"`
	ContractVersion int                    `json:"contract_version,omitempty"`
	LeaseID         string                 `json:"lease_id,omitempty"`
	LeaseExpiry     string                 `json:"lease_expiry,omitempty"`
	Attempt         int                    `json:"attempt,omitempty"`
}

// JobResult represents the result of a job execution.
type JobResult struct {
	JobID           string                 `json:"job_id"`
	JobRunID        string                 `json:"job_run_id"`
	WorkerID        string                 `json:"worker_id"`
	Status          string                 `json:"status"`
	Output          map[string]interface{} `json:"output"`
	Error           string                 `json:"error,omitempty"`
	StartTime       string                 `json:"start_time"`
	EndTime         string                 `json:"end_time"`
	ContractVersion int                    `json:"contract_version,omitempty"`
	LeaseID         string                 `json:"lease_id,omitempty"`
	Attempt         int                    `json:"attempt,omitempty"`
	ArtifactID      string                 `json:"artifact_id,omitempty"`
	OutputSHA256    string                 `json:"output_sha256,omitempty"`
	IdempotencyKey  string                 `json:"idempotency_key,omitempty"`
}

// HeartbeatPayload represents a heartbeat message.
type HeartbeatPayload struct {
	WorkerID        string                 `json:"worker_id"`
	WorkerName      string                 `json:"worker_name,omitempty"`
	Status          string                 `json:"status"`
	JobID           string                 `json:"job_id,omitempty"`
	CurrentJob      string                 `json:"current_job,omitempty"`
	CodeVersion     string                 `json:"code_version,omitempty"`
	BundleVersion   string                 `json:"bundle_version,omitempty"`
	BundleHash      string                 `json:"bundle_hash,omitempty"`
	ProtocolVersion string                 `json:"protocol_version,omitempty"`
	EngineVersion   string                 `json:"engine_version,omitempty"`
	Extra           map[string]interface{} `json:"extra,omitempty"`
}

// APIResponse represents a generic API response.
type APIResponse struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// WorkerCommand represents a command from the master to the worker.
type WorkerCommand struct {
	CommandID string                 `json:"command_id,omitempty"`
	Command   string                 `json:"command"`
	Timestamp string                 `json:"timestamp"`
	Payload   map[string]interface{} `json:"payload,omitempty"`
}

// ── Capability report (PR-3.5: registry-driven hello) ───────────────────────
//
// PR-3.5 drops the legacy boolean capability flags ("render_scene_image",
// "ffmpeg", "cpp_engine", "supported_job_types", ...) and replaces them with
// a typed, versioned schema derived directly from
// worker-agent-go/internal/executor/Registry.Descriptors().
//
// The transport envelope is `map[string]interface{}` so we keep
// CapabilityReport on pkg/api and provide AsMap() to flatten it for
// heartbeat/hello envelopes without breaking the existing API.

// CapabilitySchemaVersion is the canonical version of the CapabilityReport
// schema. Bump on every ADDITIVE OR BREAKING shape change. The master uses
// this to pick the right decoder.
const CapabilitySchemaVersion = 1

// ExecutorCapability mirrors one executor.Descriptor in the canonical
// format the master expects to see in the hello payload.
type ExecutorCapability struct {
	ID            string   `json:"id"`
	Version       int      `json:"version"`
	ResourceClass string   `json:"resource_class"`
	TemporalMode  string   `json:"temporal_mode"`
	Deterministic bool     `json:"deterministic"`
	Cacheable     bool     `json:"cacheable"`
	SupportsAlpha bool     `json:"supports_alpha"`
	OutputTypes   []string `json:"output_types,omitempty"`
}

// HostInfo is the static host layer of the report. Fields are pre-shaped
// so PR-3.6's resource sampler can fill them in without changing the wire
// contract. Unknown fields are zero-valued (never omitted) so the master
// can distinguish "not sampled" from "actually zero".
type HostInfo struct {
	WorkerID        string `json:"worker_id"`
	Hostname        string `json:"hostname"`
	CPUCount        int    `json:"cpu_count"`
	MaxParallelJobs int    `json:"max_parallel_jobs"`
	HasGPU          bool   `json:"has_gpu"`
	RAMBytes        int64  `json:"ram_bytes"`
	DiskFreeBytes   int64  `json:"disk_free_bytes"`
}

// CapabilityReport is the typed hello/heartbeat capability payload.
// PR-3.5 derives this entirely from executor.Registry — no duplicate
// state lives anywhere else.
type CapabilityReport struct {
	SchemaVersion int                  `json:"schema_version"`
	Executors     []ExecutorCapability `json:"executors"`
	Host          HostInfo             `json:"host"`
}

// AsMap flattens the typed report into the map envelope used by the
// control-transport heartbeat/hello wire format. Map ordering is
// deterministic in Go's encoding/json — callers relying on byte stable
// output MUST call this rather than building a map by hand.
func (r CapabilityReport) AsMap() map[string]interface{} {
	executors := make([]interface{}, 0, len(r.Executors))
	for _, e := range r.Executors {
		ec := map[string]interface{}{
			"id":             e.ID,
			"version":        e.Version,
			"resource_class": e.ResourceClass,
			"temporal_mode":  e.TemporalMode,
			"deterministic":  e.Deterministic,
			"cacheable":      e.Cacheable,
			"supports_alpha": e.SupportsAlpha,
		}
		if len(e.OutputTypes) > 0 {
			outs := make([]interface{}, 0, len(e.OutputTypes))
			for _, o := range e.OutputTypes {
				outs = append(outs, o)
			}
			ec["output_types"] = outs
		}
		executors = append(executors, ec)
	}
	return map[string]interface{}{
		"schema_version": r.SchemaVersion,
		"executors":      executors,
		"host": map[string]interface{}{
			"worker_id":         r.Host.WorkerID,
			"hostname":          r.Host.Hostname,
			"cpu_count":         r.Host.CPUCount,
			"max_parallel_jobs": r.Host.MaxParallelJobs,
			"has_gpu":           r.Host.HasGPU,
			"ram_bytes":         r.Host.RAMBytes,
			"disk_free_bytes":   r.Host.DiskFreeBytes,
		},
	}
}

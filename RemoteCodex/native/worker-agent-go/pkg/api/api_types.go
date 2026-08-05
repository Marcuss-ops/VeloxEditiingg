// Package api provides HTTP client for communicating with the Velox Master server.
package api

import (
	"velox-shared/controltransport"
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
	WorkerID        string                            `json:"worker_id"`
	WorkerName      string                            `json:"worker_name"`
	Capabilities    controltransport.CapabilityReport `json:"capabilities"`
	Hostname        string                            `json:"hostname"`
	IP              string                            `json:"ip"`
	Version         string                            `json:"version"`
	CodeVersion     string                            `json:"code_version,omitempty"`
	BundleVersion   string                            `json:"bundle_version,omitempty"`
	BundleHash      string                            `json:"bundle_hash,omitempty"`
	ProtocolVersion string                            `json:"protocol_version,omitempty"`
	EngineVersion   string                            `json:"engine_version,omitempty"`
	Credential      string                            `json:"credential,omitempty"`
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

// CapabilitySchemaVersion is retained as a source-compatible alias. The
// canonical owner is shared/controltransport so worker and master cannot
// silently drift to different report versions.
const CapabilitySchemaVersion = controltransport.CapabilitySchemaVersion

// ExecutorCapability, HostInfo, and CapabilityReport are aliases to the
// shared canonical transport model. The map representation exists only at
// the protobuf Struct boundary and is produced by CapabilityReport.AsMap.
type ExecutorCapability = controltransport.ExecutorCapability
type HostInfo = controltransport.HostInfo
type CapabilityReport = controltransport.CapabilityReport

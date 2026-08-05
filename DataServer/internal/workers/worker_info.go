package workers

import (
	"strings"

	"velox-shared/controltransport"
	"velox-shared/identity"
)

// Worker is the canonical master-side worker read model.
//
// WorkerID is the only identity key. Hostname, IP, container name,
// systemd unit and display name are operational attributes and must
// never be used to identify a worker. The JSON wire form remains a
// plain string at the API/storage boundary.
//
// Two fields are NOT persisted in workers.raw_json and are recomputed at
// READ time on every List/GetWorker call so an explicit DB revoke (or a
// new session) instantly demotes/promotes the cached worker without a
// registry refresh:
//
//   - SessionActive   (bool)    — derived from worker_sessions
//   - ConnectionStatus (string) — derived from drain + SessionActive +
//     heartbeat freshness
//
// See registry_query.go (Hydrate / ConnectionStatusForInfo) for the
// canonical read-time derivation. Persistence paths in Heartbeat and
// UpdateWorker explicitly ZERO both fields before UpsertWorker so a
// cached Worker returned by a previous GetWorker cannot leak derived
// state across a registry restart.
type Worker struct {
	// WorkerID is the canonical typed worker identity (velox-shared/identity).
	WorkerID               identity.WorkerID `json:"worker_id"`
	WorkerName             string            `json:"worker_name"`
	DisplayName            string            `json:"display_name"`
	LastHB                 string            `json:"last_heartbeat"`
	FirstSeen              string            `json:"first_seen"`
	CurrentJob             string            `json:"current_job"`
	Drain                  bool              `json:"drain"`
	Schedulable            bool              `json:"schedulable"`
	WorkerGroup            string            `json:"worker_group"`
	IPAddress              string            `json:"ip_address"`
	Host                   string            `json:"host"`
	NodeID                 string            `json:"node_id,omitempty"`
	NodeRole               string            `json:"node_role,omitempty"`
	ClusterID              string            `json:"cluster_id,omitempty"`
	HostFingerprint        string            `json:"host_fingerprint,omitempty"`
	CertificateFingerprint string            `json:"certificate_fingerprint,omitempty"`

	// Class is the operator-assigned fleet class used by dispatchers and
	// the GET /api/v1/workers?class= filter.
	Class string `json:"worker_class,omitempty"`

	// RolloutGroup is the operator-assigned rollout cohort used to phase
	// worker fleets into a new bundle.
	RolloutGroup    string `json:"rollout_group,omitempty"`
	CodeVersion     string `json:"code_version"`
	BundleVersion   string `json:"bundle_version"`
	BundleHash      string `json:"bundle_hash,omitempty"`
	ImageDigest     string `json:"image_digest,omitempty"`
	DesiredVersion  string `json:"desired_version,omitempty"`
	ProtocolVersion string `json:"protocol_version,omitempty"`
	EngineVersion   string `json:"engine_version,omitempty"`

	// Capabilities is retained at the compatibility boundary. The typed
	// ExecutorCapabilities registry is the source of truth for executor
	// discovery and placement.
	Capabilities         map[string]interface{}            `json:"capabilities,omitempty"`
	ExecutorCapabilities controltransport.ExecutorRegistry `json:"-"`
	BootID               string                            `json:"boot_id,omitempty"`
	BootTS               string                            `json:"boot_ts,omitempty"`

	SessionActive    bool   `json:"session_active"`
	ConnectionStatus string `json:"connection_status"`
	Reason           string `json:"reason"`

	// Canonical independent worker-state dimensions. These are populated
	// at read time from the session/heartbeat, operator scheduling inputs,
	// deployment metadata, and telemetry. They are the source for new
	// consumers; the legacy ConnectionStatus and Health strings below are
	// compatibility projections only.
	ConnectionState ConnectionState `json:"connection_state"`
	SchedulingState SchedulingState `json:"scheduling_state"`
	DeploymentState DeploymentState `json:"deployment_state,omitempty"`
	HealthState     HealthState     `json:"health_state"`
	Health          string          `json:"health"`
	Quarantined     bool            `json:"quarantined"`

	// Deprecated: consume the typed state dimensions above. This field is
	// retained for clients that still expect the operator 9-state string.
	// It is written only by the compatibility projection.

	Readiness    map[string]interface{} `json:"readiness,omitempty"`
	RecentLogs   []string               `json:"recent_logs,omitempty"`
	RecentErrors []string               `json:"recent_errors,omitempty"`
	Metrics      map[string]interface{} `json:"metrics,omitempty"`
}

// WorkerInfo is retained as a source-compatible alias during the migration
// of external consumers. New code must use Worker. The alias does not create
// a second model or a second identity representation.
//
// Deprecated: use Worker.
type WorkerInfo = Worker

// ExecutorRegistrySnapshot returns the typed executor view used by master
// consumers. Persisted legacy capability maps are decoded only here as a
// rolling-deployment compatibility adapter; new heartbeats populate the
// ExecutorCapabilities field directly.
func (info Worker) ExecutorRegistrySnapshot() controltransport.ExecutorRegistry {
	if !info.ExecutorCapabilities.IsEmpty() {
		return info.ExecutorCapabilities
	}
	registry, err := controltransport.ExecutorRegistryFromLegacy(info.Capabilities)
	if err != nil {
		return controltransport.EmptyExecutorRegistry()
	}
	return registry
}

// ScrubForPersist zeroes the read-time-hydrated fields on info so a cached
// Worker returned by a previous GetWorker cannot leak derived state into
// workers.raw_json. Quarantined is intentionally persisted.
func ScrubForPersist(info *Worker) {
	if info == nil {
		return
	}
	info.SessionActive = false
	info.ConnectionStatus = ""
	info.Reason = ""
	info.ConnectionState = ""
	info.SchedulingState = ""
	info.HealthState = ""
	info.Health = ""
}

const DefaultWorkerProtocolVersion = "v3"

func applyMetadataFields(extra map[string]interface{}, info *Worker) {
	if extra == nil || info == nil {
		return
	}
	if v, ok := extra["code_version"].(string); ok && v != "" {
		info.CodeVersion = v
	}
	if v, ok := extra["bundle_version"].(string); ok && v != "" {
		info.BundleVersion = v
	}
	if v, ok := extra["bundle_hash"].(string); ok && v != "" {
		info.BundleHash = v
	}
	if v, ok := extra["image_digest"].(string); ok && v != "" {
		info.ImageDigest = v
	}
	if v, ok := extra["desired_version"].(string); ok && v != "" {
		info.DesiredVersion = v
	}
	if v, ok := extra["deployment_state"].(string); ok && v != "" {
		info.DeploymentState = NormalizeDeploymentState(v)
	}
	if v, ok := extra["protocol_version"].(string); ok && v != "" {
		info.ProtocolVersion = v
	}
	if v, ok := extra["engine_version"].(string); ok && v != "" {
		info.EngineVersion = v
	}
	if v, ok := extra["node_id"].(string); ok && v != "" {
		info.NodeID = v
	}
	if v, ok := extra["node_role"].(string); ok && v != "" {
		info.NodeRole = v
	}
	if v, ok := extra["cluster_id"].(string); ok && v != "" {
		info.ClusterID = v
	}
	if v, ok := extra["host_fingerprint"].(string); ok && v != "" {
		info.HostFingerprint = v
	}
	if v, ok := extra["certificate_fingerprint"].(string); ok && v != "" {
		info.CertificateFingerprint = v
	}
	if v, ok := extra["worker_class"].(string); ok && v != "" {
		info.Class = v
	}
	if v, ok := extra["rollout_group"].(string); ok && v != "" {
		info.RolloutGroup = v
	}
	if v, ok := extra["capabilities"]; ok {
		info.Capabilities = normalizeCapabilities(v)
		if registry, err := controltransport.ExecutorRegistryFromLegacy(v); err == nil {
			info.ExecutorCapabilities = registry
		}
	}
	if v, ok := extra["supported_job_types"]; ok {
		if info.Capabilities == nil {
			info.Capabilities = map[string]interface{}{}
		}
		info.Capabilities["supported_job_types"] = ExtractStringSlice(v)
	}
}

func normalizeCapabilities(v interface{}) map[string]interface{} {
	switch t := v.(type) {
	case map[string]interface{}:
		return t
	case map[string]bool:
		out := make(map[string]interface{}, len(t))
		for k, b := range t {
			out[k] = b
		}
		return out
	case map[string]string:
		out := make(map[string]interface{}, len(t))
		for k, s := range t {
			out[k] = s
		}
		return out
	default:
		return nil
	}
}

// ExtractStringSlice converts various slice-like types to []string.
func ExtractStringSlice(v interface{}) []string {
	if v == nil {
		return nil
	}
	switch t := v.(type) {
	case []string:
		out := make([]string, 0, len(t))
		for _, s := range t {
			s = strings.TrimSpace(s)
			if s != "" {
				out = append(out, s)
			}
		}
		return out
	case []interface{}:
		out := make([]string, 0, len(t))
		for _, it := range t {
			if s, ok := it.(string); ok {
				s = strings.TrimSpace(s)
				if s != "" {
					out = append(out, s)
				}
			}
		}
		return out
	default:
		return nil
	}
}

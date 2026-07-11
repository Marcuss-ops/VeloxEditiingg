package workers

import "strings"

// WorkerInfo contains all information about a registered worker.
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
// cached WorkerInfo returned by a previous GetWorker cannot leak derived
// state across a registry restart.
type WorkerInfo struct {
	WorkerID    string `json:"worker_id"`
	WorkerName  string `json:"worker_name"`
	DisplayName string `json:"display_name"`
	Status      string `json:"status"`
	LastHB      string `json:"last_heartbeat"`
	FirstSeen   string `json:"first_seen"`
	CurrentJob  string `json:"current_job"`
	Drain       bool   `json:"drain"`
	Schedulable bool   `json:"schedulable"`
	WorkerGroup string `json:"worker_group"`
	IPAddress   string `json:"ip_address"`
	Host        string `json:"host"`

	// Class (RW-PROD-005 §2.1) is the operator-assigned fleet class
	// (cpu-xlarge / gpu-a100 / mixed / io ...) used by dispatchers
	// and by the GET /api/v1/workers?class= filter. Empty string
	// means "unclassified"; the handler ignores empty-class rows
	// when the filter is active.
	Class string `json:"worker_class,omitempty"`

	// RolloutGroup (RW-PROD-005 §2.1) is the operator-assigned
	// rollout cohort (v3.4 / canary / holdout ...) used to phase
	// worker fleets into a new bundle. Empty string means
	// "unassigned"; the handler ignores empty-rollout rows when
	// the filter is active.
	RolloutGroup    string                 `json:"rollout_group,omitempty"`
	CodeVersion     string                 `json:"code_version"`
	BundleVersion   string                 `json:"bundle_version"`
	BundleHash      string                 `json:"bundle_hash,omitempty"`
	ProtocolVersion string                 `json:"protocol_version,omitempty"`
	EngineVersion   string                 `json:"engine_version,omitempty"`
	Capabilities    map[string]interface{} `json:"capabilities,omitempty"`
	BootID          string                 `json:"boot_id,omitempty"`
	BootTS          string                 `json:"boot_ts,omitempty"`

	// SessionActive — computed at READ time from worker_sessions: true
	// iff the worker has at least one non-revoked, non-expired auth
	// session whose last_seen is inside WorkerSessionFreshnessWindow
	// (5 min — see internal/store/store_worker_control.go). Combined with
	// heartbeat freshness to derive ConnectionStatus.
	//
	// Note: deliberately NOT omitempty. Clients consuming this field
	// MUST be able to distinguish "session_active=false (offline)" from
	// "field missing (legacy client)" — the latter is ambiguous.
	SessionActive bool `json:"session_active"`

	// ConnectionStatus — canonical derived state, one of:
	//   CONNECTED:    session_active && (now - last_heartbeat) < 30s
	//   STALE:        session_active && 30s ≤ (now - last_heartbeat) < 5min
	//   DISCONNECTED: !session_active OR (now - last_heartbeat) ≥ 5min
	//   DRAINING:     drain=true (overrides heartbeat freshness)
	// Always serialized (no omitempty) so the legacy/fallback path emits
	// "" rather than silently dropping the field, which would dodge the
	// sanitizeWorker invariant Status != "" (see handler-side guard).
	ConnectionStatus string `json:"connection_status"`

	// Reason — canonical reason code for non-CONNECTED states (RW-PROD-005 A2).
	// Set by ConnectionStatusForInfo alongside ConnectionStatus. One of:
	//   drain            — Drain=true (precedence 1).
	//   detached_session — session_active=false (stream closed).
	//   heartbeat_stale  — session_active but last_heartbeat is stale/empty/unparseable.
	//   ""               — status=CONNECTED: no reason emitted.
	// Always serialized (no omitempty) so clients can distinguish "" from absent.
	Reason string `json:"reason"`

	Readiness    map[string]interface{} `json:"readiness,omitempty"`
	RecentLogs   []string               `json:"recent_logs,omitempty"`
	RecentErrors []string               `json:"recent_errors,omitempty"`
	Metrics      map[string]interface{} `json:"metrics,omitempty"`
}

// ScrubForPersist zeroes the read-time-hydrated fields on `info` so a
// cached WorkerInfo returned by a previous GetWorker cannot leak its
// derived state into workers.raw_json (which would re-hydrate stale on
// the next registry.Load).
//
// Persistence call sites that marshal a *WorkerInfo to workers.raw_json
// (currently ONLY: Heartbeat in registry_heartbeat.go and UpdateWorker
// in registry_update.go — RegisterWorker builds a fresh struct so it
// cannot leak) MUST call ScrubForPersist on a COPY of `info` immediately
// before json.Marshal. The canonical pattern is:
//
//	persisted := *info
//	workers.ScrubForPersist(&persisted)
//	raw, _ := json.Marshal(persisted)
//	dbStore.UpsertWorker(raw)
//
// IMPORTANT — this helper is ONLY for sites that marshal a *WorkerInfo.
// Other worker persistence paths (SetWorkerRevoked → worker_flags.raw_json)
// deliberately persist a DIFFERENT shape — a tiny three-key audit blob —
// and have no read-time-hydration contract. Calling ScrubForPersist on
// a hardcoded map there would be a no-op; do NOT "harmonize" the two
// raw_json paths, or you reintroduce the leak. The shape contract on
// worker_flags.raw_json is enforced by store_workers_test.go.
//
// Treating SessionActive + ConnectionStatus as "never persisted" is the
// only way to keep the workers.raw_json JSON contract and the
// read-model enum consistent across restarts.
func ScrubForPersist(info *WorkerInfo) {
	if info == nil {
		return
	}
	info.SessionActive = false
	info.ConnectionStatus = ""
	info.Reason = ""
}

const DefaultWorkerProtocolVersion = "v3"

func applyMetadataFields(extra map[string]interface{}, info *WorkerInfo) {
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
	if v, ok := extra["protocol_version"].(string); ok && v != "" {
		info.ProtocolVersion = v
	}
	if v, ok := extra["engine_version"].(string); ok && v != "" {
		info.EngineVersion = v
	}
	// RW-PROD-005 A9: class + rollout_group arrive in Hello metadata
	// from the worker's buildHello; the master hydrates WorkerInfo.Class
	// and .RolloutGroup here so the GET /api/v1/workers?class= filter works.
	if v, ok := extra["worker_class"].(string); ok && v != "" {
		info.Class = v
	}
	if v, ok := extra["rollout_group"].(string); ok && v != "" {
		info.RolloutGroup = v
	}
	if v, ok := extra["capabilities"]; ok {
		info.Capabilities = normalizeCapabilities(v)
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

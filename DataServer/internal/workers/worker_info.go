package workers

import "strings"

// WorkerInfo contains all information about a registered worker
type WorkerInfo struct {
	WorkerID        string                 `json:"worker_id"`
	WorkerName      string                 `json:"worker_name"`
	DisplayName     string                 `json:"display_name"`
	Status          string                 `json:"status"`
	LastHB          string                 `json:"last_heartbeat"`
	FirstSeen       string                 `json:"first_seen"`
	CurrentJob      string                 `json:"current_job"`
	Drain           bool                   `json:"drain"`
	Schedulable     bool                   `json:"schedulable"`
	WorkerGroup     string                 `json:"worker_group"`
	IPAddress       string                 `json:"ip_address"`
	Host            string                 `json:"host"`
	CodeVersion     string                 `json:"code_version"`
	BundleVersion   string                 `json:"bundle_version"`
	BundleHash      string                 `json:"bundle_hash,omitempty"`
	ProtocolVersion string                 `json:"protocol_version,omitempty"`
	EngineVersion   string                 `json:"engine_version,omitempty"`
	Capabilities    map[string]interface{} `json:"capabilities,omitempty"`
	BootID          string                 `json:"boot_id,omitempty"`
	BootTS          string                 `json:"boot_ts,omitempty"`
	Readiness       map[string]interface{} `json:"readiness,omitempty"`
	RecentLogs      []string               `json:"recent_logs,omitempty"`
	RecentErrors    []string               `json:"recent_errors,omitempty"`
	Metrics         map[string]interface{} `json:"metrics,omitempty"`
}

const DefaultWorkerProtocolVersion = "2026-06-worker-v1"

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
	if v, ok := extra["capabilities"]; ok {
		info.Capabilities = normalizeCapabilities(v)
	}
	if v, ok := extra["supported_job_types"]; ok {
		if info.Capabilities == nil {
			info.Capabilities = map[string]interface{}{}
		}
		info.Capabilities["supported_job_types"] = normalizeStringSlice(v)
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

func normalizeStringSlice(v interface{}) []string {
	if v == nil {
		return nil
	}
	switch t := v.(type) {
	case []string:
		return t
	case []interface{}:
		out := make([]string, 0, len(t))
		for _, it := range t {
			if s, ok := it.(string); ok {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

// GetSupportedJobTypes returns the list of job types a worker supports.
func (w *WorkerInfo) GetSupportedJobTypes() []string {
	if w.Capabilities == nil {
		return nil
	}
	v, ok := w.Capabilities["supported_job_types"]
	if !ok {
		return nil
	}
	return ExtractStringSlice(v)
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

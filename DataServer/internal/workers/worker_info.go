package workers

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
)

// WorkerInfo contains all information about a registered worker
type WorkerInfo struct {
	WorkerID      string                 `json:"worker_id"`
	WorkerName    string                 `json:"worker_name"`
	DisplayName   string                 `json:"display_name"`
	Status        string                 `json:"status"`
	LastHB        string                 `json:"last_heartbeat"`
	FirstSeen     string                 `json:"first_seen"`
	CurrentJob    string                 `json:"current_job"`
	Drain         bool                   `json:"drain"`
	Schedulable   bool                   `json:"schedulable"`
	WorkerGroup   string                 `json:"worker_group"`
	IPAddress     string                 `json:"ip_address"`
	Host          string                 `json:"host"`
	CodeVersion   string                 `json:"code_version"`
	BundleVersion string                 `json:"bundle_version"`
	BootID        string                 `json:"boot_id,omitempty"`
	BootTS        string                 `json:"boot_ts,omitempty"`
	Readiness     map[string]interface{} `json:"readiness,omitempty"`
	RecentLogs    []string               `json:"recent_logs,omitempty"`
	RecentErrors  []string               `json:"recent_errors,omitempty"`
	Metrics       map[string]interface{} `json:"metrics,omitempty"`
}

// GenerateWorkerID generates a unique worker ID
func GenerateWorkerID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func extractStringSlice(v interface{}) []string {
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

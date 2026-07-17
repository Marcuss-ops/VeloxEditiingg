package config

import (
	"os"
	"strings"
)

// GetMasterURL returns the publicly-reachable master URL using the canonical
// priority chain: MASTER_PUBLIC_URL > VELOX_MASTER_URL > MASTER_URL. Returns
// an empty string when none of the variables are set. This matches the
// onboarding/install flow and the value surfaced as Config.MasterURL.
func GetMasterURL() string {
	if v := strings.TrimSpace(os.Getenv("MASTER_PUBLIC_URL")); v != "" {
		return v
	}
	if v := strings.TrimSpace(os.Getenv("VELOX_MASTER_URL")); v != "" {
		return v
	}
	if v := strings.TrimSpace(os.Getenv("MASTER_URL")); v != "" {
		return v
	}
	return ""
}

// GetMasterServerURL returns the upstream master-server URL using the priority
// chain: VELOX_MASTER_SERVER_URL > VELOX_REMOTE_WORKER_URL. This is the URL
// workers reach for orchestration (proxy/draft/create-master), not the one
// advertised to clients.
func GetMasterServerURL() string {
	if v := strings.TrimSpace(os.Getenv("VELOX_MASTER_SERVER_URL")); v != "" {
		return v
	}
	if v := strings.TrimSpace(os.Getenv("VELOX_REMOTE_WORKER_URL")); v != "" {
		return v
	}
	return ""
}

// GetAnsibleMasterURL returns the URL used as bootstrap target for
// ansible/deploy flows: VELOX_MASTER_URL > VELOX_MASTER_SERVER_URL. Note the
// different fallback order from GetMasterURL — ansible prefers the public URL
// but tolerates the internal server URL when only the latter is set.
func GetAnsibleMasterURL() string {
	if v := strings.TrimSpace(os.Getenv("VELOX_MASTER_URL")); v != "" {
		return v
	}
	if v := strings.TrimSpace(os.Getenv("VELOX_MASTER_SERVER_URL")); v != "" {
		return v
	}
	return ""
}

// GetDataDir returns VELOX_DATA_DIR as configured by the operator, or empty if
// the variable is not set. Callers must apply their own defaulting.
func GetDataDir() string {
	return strings.TrimSpace(os.Getenv("VELOX_DATA_DIR"))
}



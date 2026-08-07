package config

import "testing"

func TestProjectBridgeContract_DefaultsToCanonicalVersion(t *testing.T) {
	cfg := FromRaw(NewRawConfig(map[string]string{
		"VELOX_DB_PATH":         "/tmp/velox-project-bridge-test.db",
		"VELOX_ALLOWED_WORKERS": "worker-a,worker-b",
	}))
	if got := cfg.Auth.ProjectBridgeContractVersion; got != "instaedit.velox.project-bridge.v1" {
		t.Fatalf("project bridge contract version: got %q", got)
	}
}

func TestProjectBridgeContract_RejectsUnknownVersion(t *testing.T) {
	cfg := FromRaw(NewRawConfig(map[string]string{
		"VELOX_DB_PATH":                         "/tmp/velox-project-bridge-test.db",
		"VELOX_ALLOWED_WORKERS":                 "worker-a,worker-b",
		"VELOX_PROJECT_BRIDGE_CONTRACT_VERSION": "instaedit.velox.project-bridge.v9",
	}))
	if err := cfg.Validate(); err == nil {
		t.Fatal("unknown project bridge contract version was accepted")
	}
}

package config

import (
	"os"
	"testing"
)

func TestFromEnv_Defaults(t *testing.T) {
	// Clear env vars
	os.Unsetenv("VELOX_MASTER_PORT")
	os.Unsetenv("VELOX_DB_DRIVER")
	os.Unsetenv("VELOX_ADMIN_TOKEN")

	cfg := FromEnv()

	// Check defaults
	if cfg.MasterPort != 8000 {
		t.Errorf("expected MasterPort=8000, got %d", cfg.MasterPort)
	}
	if cfg.DBDriver != "sqlite3" {
		t.Errorf("expected DBDriver=sqlite3, got %s", cfg.DBDriver)
	}
	if cfg.MaxJobAttempts != 3 {
		t.Errorf("expected MaxJobAttempts=3, got %d", cfg.MaxJobAttempts)
	}
	if cfg.WorkerHeartbeatTimeout != 900 {
		t.Errorf("expected WorkerHeartbeatTimeout=900, got %d", cfg.WorkerHeartbeatTimeout)
	}

	// Check sub-configs
	if cfg.Server.Port != 8000 {
		t.Errorf("expected Server.Port=8000, got %d", cfg.Server.Port)
	}
	if cfg.Database.Driver != "sqlite3" {
		t.Errorf("expected Database.Driver=sqlite3, got %s", cfg.Database.Driver)
	}
	if cfg.Workers.MaxJobAttempts != 3 {
		t.Errorf("expected Workers.MaxJobAttempts=3, got %d", cfg.Workers.MaxJobAttempts)
	}
}

func TestFromEnv_CustomValues(t *testing.T) {
	os.Setenv("VELOX_MASTER_PORT", "9000")
	os.Setenv("VELOX_ADMIN_TOKEN", "my-secret-token")
	defer os.Unsetenv("VELOX_MASTER_PORT")
	defer os.Unsetenv("VELOX_ADMIN_TOKEN")

	cfg := FromEnv()

	if cfg.MasterPort != 9000 {
		t.Errorf("expected MasterPort=9000, got %d", cfg.MasterPort)
	}
	if cfg.AdminToken != "my-secret-token" {
		t.Errorf("expected AdminToken=my-secret-token, got %s", cfg.AdminToken)
	}

	if cfg.Server.Port != 9000 {
		t.Errorf("expected Server.Port=9000, got %d", cfg.Server.Port)
	}
	if cfg.Auth.AdminToken != "my-secret-token" {
		t.Errorf("expected Auth.AdminToken=my-secret-token, got %s", cfg.Auth.AdminToken)
	}
}

package config

import (
	"os"
	"testing"
)

func TestFromEnv_Defaults(t *testing.T) {
	// Clear env vars
	os.Unsetenv("VELOX_MASTER_PORT")
	os.Unsetenv("VELOX_ADMIN_TOKEN")
	os.Setenv("VELOX_DB_PATH", t.TempDir()+"/velox.db")
	os.Setenv("VELOX_GRPC_PORT", "50051")
	defer os.Unsetenv("VELOX_DB_PATH")
	defer os.Unsetenv("VELOX_GRPC_PORT")

	cfg := FromEnv()

	// Check defaults via sub-configs
	if cfg.Server.Port != 8000 {
		t.Errorf("expected Server.Port=8000, got %d", cfg.Server.Port)
	}
	if cfg.Database.DBPath == "" {
		t.Error("expected Database.DBPath to be set from VELOX_DB_PATH")
	}
	if cfg.Workers.MaxJobAttempts != 3 {
		t.Errorf("expected Workers.MaxJobAttempts=3, got %d", cfg.Workers.MaxJobAttempts)
	}
	if cfg.Workers.HeartbeatTimeout != 900 {
		t.Errorf("expected Workers.HeartbeatTimeout=900, got %d", cfg.Workers.HeartbeatTimeout)
	}

	// Check Validate
	if err := cfg.Validate(); err != nil {
		t.Errorf("expected valid config, got error: %v", err)
	}

	// Check sub-configs
	if cfg.Server.Port != 8000 {
		t.Errorf("expected Server.Port=8000, got %d", cfg.Server.Port)
	}
	if cfg.Database.DBPath == "" {
		t.Error("expected Database.DBPath to be set")
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

	if cfg.Server.Port != 9000 {
		t.Errorf("expected Server.Port=9000, got %d", cfg.Server.Port)
	}
	if cfg.Auth.AdminToken != "my-secret-token" {
		t.Errorf("expected Auth.AdminToken=my-secret-token, got %s", cfg.Auth.AdminToken)
	}
}

func TestValidate_RelativeDBPath(t *testing.T) {
	cfg := &Config{
		Database: DatabaseConfig{DBPath: "relative/path/velox.db"},
	}
	err := cfg.Validate()
	if err == nil {
		t.Error("expected error for relative DB path, got nil")
	}
}

func TestValidate_AbsoluteDBPath(t *testing.T) {
	// Use an OS-appropriate absolute path
	absPath := t.TempDir() + "/velox.db"
	cfg := &Config{
		Database: DatabaseConfig{DBPath: absPath},
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("expected valid config for absolute path, got: %v", err)
	}
}

func TestValidate_EmptyDBPath(t *testing.T) {
	cfg := &Config{}
	err := cfg.Validate()
	if err == nil {
		t.Error("expected error for empty DB path, got nil")
	}
}

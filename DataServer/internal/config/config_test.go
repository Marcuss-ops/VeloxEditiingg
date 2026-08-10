package config

import (
	"os"
	"strings"
	"testing"
)

func TestFromEnv_Defaults(t *testing.T) {
	// Clear env vars
	os.Unsetenv("VELOX_MASTER_PORT")
	os.Unsetenv("VELOX_ADMIN_TOKEN")
	os.Setenv("VELOX_DB_PATH", t.TempDir()+"/velox.db")
	os.Setenv("VELOX_GRPC_PORT", "50051")
	os.Setenv("MASTER_PUBLIC_URL", "https://master.example")
	os.Setenv("VELOX_GRPC_MASTER_URL", "master.example:50051")
	// Production allowlist: 2 distinct worker IDs. Without this the
	// canonical ValidateProductionWorkers check fails the test.
	os.Setenv("VELOX_ALLOWED_WORKERS", "velox-worker-1,velox-worker-2")
	defer os.Unsetenv("VELOX_DB_PATH")
	defer os.Unsetenv("VELOX_GRPC_PORT")
	defer os.Unsetenv("MASTER_PUBLIC_URL")
	defer os.Unsetenv("VELOX_GRPC_MASTER_URL")
	defer os.Unsetenv("VELOX_ALLOWED_WORKERS")

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
	if len(cfg.Workers.AllowedWorkerIDs) != 2 {
		t.Errorf("expected Workers.AllowedWorkerIDs=2 entries, got %d", len(cfg.Workers.AllowedWorkerIDs))
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
	if cfg.Runtime.Supervisor.CriticalMaxRetries != 0 || cfg.Runtime.Supervisor.CriticalFailAfter != 10 {
		t.Errorf("unexpected supervisor defaults: %+v", cfg.Runtime.Supervisor)
	}
	if cfg.Runtime.Alerts.ErrorRatePct != 5.0 || cfg.Runtime.Alerts.P95WallMS != 300000 {
		t.Errorf("unexpected alert defaults: %+v", cfg.Runtime.Alerts)
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

func TestFromEnv_BootstrapTypedValues(t *testing.T) {
	t.Setenv("VELOX_REQUIRE_LIVE_WORKERS", "true")
	t.Setenv("VELOX_CRITICAL_MAX_RETRIES", "4")
	t.Setenv("VELOX_CRITICAL_FAIL_AFTER", "7")
	t.Setenv("VELOX_ALERT_ERROR_RATE_PCT", "8.5")
	t.Setenv("VELOX_ALERT_P95_WALL_MS", "1234")
	t.Setenv("VELOX_ALERT_DISK_FREE_GB", "2.5")
	t.Setenv("VELOX_ALERT_FFMPEG_MIN", "1.25")
	t.Setenv("VELOX_RETENTION_WORKER_METRICS_DAYS", "11")
	t.Setenv("VELOX_RETENTION_WORKER_EVENTS_DAYS", "22")
	t.Setenv("VELOX_RETENTION_WORKER_RESOURCE_RAW_DAYS", "33")
	t.Setenv("VELOX_RETENTION_WORKER_RESOURCE_ROLLUP_DAYS", "44")
	t.Setenv("VELOX_CPU_CORE_SECOND_COST", "0.000007")
	t.Setenv("VELOX_NETWORK_GB_COST", "0.02")
	t.Setenv("VELOX_STORAGE_GB_COST", "0.0003")
	t.Setenv("VELOX_SMOKE_MODE", "development")
	t.Setenv("VELOX_SMOKE_DRIVE_FOLDER_ID", "folder-a")
	t.Setenv("VELOX_GRPC_REQUIRE_TLS", "true")
	t.Setenv("VELOX_LOG_ROUTES_AT_BOOT", "true")

	cfg := FromEnv()
	if !cfg.Runtime.Supervisor.RequireLiveWorkers || cfg.Runtime.Supervisor.CriticalMaxRetries != 4 || cfg.Runtime.Supervisor.CriticalFailAfter != 7 {
		t.Fatalf("unexpected supervisor config: %+v", cfg.Runtime.Supervisor)
	}
	if cfg.Runtime.Alerts.ErrorRatePct != 8.5 || cfg.Runtime.Alerts.P95WallMS != 1234 || cfg.Runtime.Alerts.DiskFreeGB != 2.5 || cfg.Runtime.Alerts.FFmpegMin != 1.25 {
		t.Fatalf("unexpected alert config: %+v", cfg.Runtime.Alerts)
	}
	if cfg.Retention.WorkerMetricsDays != 11 || cfg.Retention.WorkerEventsDays != 22 || cfg.Retention.WorkerResourceRawDays != 33 || cfg.Retention.WorkerResourceRollupDays != 44 {
		t.Fatalf("unexpected retention config: %+v", cfg.Retention)
	}
	if cfg.Runtime.Metrics.CPUCostEUR != 0.000007 || cfg.Runtime.Metrics.NetworkCostEUR != 0.02 || cfg.Runtime.Metrics.StorageCostEUR != 0.0003 {
		t.Fatalf("unexpected metrics config: %+v", cfg.Runtime.Metrics)
	}
	if cfg.Fleet.SmokeMode != "development" || cfg.Fleet.SmokeDriveFolderID != "folder-a" {
		t.Fatalf("unexpected fleet config: %+v", cfg.Fleet)
	}
	if !cfg.Server.GRPCRequireTLS || !cfg.Server.LogRoutesAtBoot {
		t.Fatalf("unexpected server bootstrap config: %+v", cfg.Server)
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
		Workers:  WorkersConfig{AllowedWorkerIDs: []string{"velox-worker-1", "velox-worker-2"}},
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

// TestValidate_RejectsWildcardAllowlist pins the wildcard guard at the
// Config.Validate layer. The check is in Config.Validate (not in
// ValidateProductionWorkers — which only does count + uniqueness) so a
// future refactor that bypasses ValidateProductionWorkers still fails
// closed on "*" in production.
//
// Without this test, a regression that removes the wildcard iteration
// loop in Config.Validate would silently let `"*"` slip past the
// bootstrap fail-fast and generate a master that admits any worker.
func TestCompatibilityModeDefaultsAndStrictParsing(t *testing.T) {
	cfg := &Config{Database: DatabaseConfig{DBPath: t.TempDir() + "/velox.db"}, Workers: WorkersConfig{AllowedWorkerIDs: []string{"worker-a", "worker-b"}}}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("empty compatibility mode should default to strict: %v", err)
	}
	if cfg.Compatibility.Mode != "strict" {
		t.Fatalf("default compatibility mode = %q, want strict", cfg.Compatibility.Mode)
	}
	cfg.Compatibility.Mode = "strict"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("strict compatibility mode rejected: %v", err)
	}
	cfg.Compatibility.Mode = "invalid"
	if err := cfg.Validate(); err == nil {
		t.Fatal("invalid compatibility mode accepted")
	}
}

// TestLoadCompatibilityConfigDefaultsToStrict locks the canonical boot
// default: with no VELOX_COMPATIBILITY_MODE the typed Config produced by
// FromEnv must be strict. The master boot (main.go / bootstrap_composition.go)
// maps "strict" to compatibility.ModeStrict, so this is the line that flips
// new deployments to reject registered legacy aliases by default.
func TestLoadCompatibilityConfigDefaultsToStrict(t *testing.T) {
	t.Setenv("VELOX_COMPATIBILITY_MODE", "")
	cfg := FromEnv()
	if cfg.Compatibility.Mode != "strict" {
		t.Fatalf("default compatibility mode = %q, want strict", cfg.Compatibility.Mode)
	}
}

func TestLoadPipelineConfigRetainsOnlyLiveSettings(t *testing.T) {
	raw := NewRawConfig(map[string]string{
		"OLLAMA_ADDR":  "http://ollama.internal:11434",
		"OLLAMA_MODEL": "test-model",
	})
	cfg := FromRaw(raw)
	if cfg.Pipeline.OllamaURL != "http://ollama.internal:11434" || cfg.Pipeline.OllamaModel != "test-model" {
		t.Fatalf("live pipeline settings were not loaded: %+v", cfg.Pipeline)
	}
}

func TestValidate_RejectsWildcardAllowlist(t *testing.T) {
	cfg := &Config{
		Database: DatabaseConfig{DBPath: t.TempDir() + "/velox.db"},
		Workers:  WorkersConfig{AllowedWorkerIDs: []string{"*", "velox-worker-1"}},
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for wildcard '*' allowlist, got nil")
	}
	if !strings.Contains(err.Error(), "must not contain '*'") {
		t.Fatalf("expected wildcard-specific error, got: %v", err)
	}
}

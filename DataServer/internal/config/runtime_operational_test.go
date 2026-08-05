package config

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

func TestLoadOperationalRuntimeDefaults(t *testing.T) {
	for _, key := range []string{
		"VELOX_TASKGRAPH_TICK", "VELOX_ARTIFACT_RECONCILE_INTERVAL",
		"VELOX_METRICS_SNAPSHOT_INTERVAL", "VELOX_RESTARTABLE_MAX_RETRIES",
		"VELOX_METRICS_TICK", "VELOX_CACHE_LOOKAHEAD_JOBS",
		"VELOX_CACHE_SNAPSHOT_INTERVAL", "VELOX_ALERT_EVALUATION_INTERVAL",
		"VELOX_ALERT_COOLDOWN",
	} {
		t.Setenv(key, "")
	}
	cfg := &Config{Runtime: RuntimeConfig{DataDir: t.TempDir()}, Database: DatabaseConfig{DBPath: t.TempDir() + "/velox.db"}}
	cfg.LoadOperationalRuntime()
	if cfg.Runtime.Scheduler.TaskGraphTick != 2*time.Second {
		t.Fatalf("taskgraph tick = %s", cfg.Runtime.Scheduler.TaskGraphTick)
	}
	if cfg.Runtime.Scheduler.ArtifactReconcileInterval != 15*time.Minute {
		t.Fatalf("artifact interval = %s", cfg.Runtime.Scheduler.ArtifactReconcileInterval)
	}
	if cfg.Runtime.Metrics.Tick != 15*time.Second {
		t.Fatalf("metrics tick = %s", cfg.Runtime.Metrics.Tick)
	}
	if cfg.Runtime.Cache.SnapshotInterval != 30*time.Second {
		t.Fatalf("cache interval = %s", cfg.Runtime.Cache.SnapshotInterval)
	}
}

func TestRuntimeSnapshotRedactsSecretsAndIsDeterministic(t *testing.T) {
	cfg := &Config{
		Runtime: RuntimeConfig{
			DataDir:       "/srv/velox/data",
			Environment:   "production",
			CommitHMACKey: strings.Repeat("a", 64),
			Supervisor:    SupervisorConfig{CriticalMaxRetries: 4},
			Cache:         CacheConfig{ProtectedAssetLookaheadJobs: 12, SnapshotInterval: 20 * time.Second},
			Scheduler:     SchedulerConfig{TaskGraphTick: 2 * time.Second, ArtifactReconcileInterval: 15 * time.Minute, MetricsSnapshotInterval: 5 * time.Minute, RestartableMaxRetries: 5},
			Metrics:       MetricsConfig{Tick: 15 * time.Second, AttemptLimit: 1000},
			Alerts:        AlertConfig{ErrorRatePct: 5, P95WallMS: 300000, DiskFreeGB: 10, FFmpegMin: 1.5, WebhookURL: "https://secret.example/hook"},
		},
		Auth:     AuthConfig{AdminToken: "admin-secret"},
		Storage:  StorageConfig{AccessKeyID: "access-secret", SecretKey: "storage-secret"},
		Database: DatabaseConfig{Driver: "sqlite", DBPath: "/srv/velox/data/velox.db"},
	}
	one := cfg.Snapshot()
	two := cfg.Snapshot()
	if one.SchemaVersion != RuntimeConfigSchemaVersion || one.Fingerprint == "" || one.Fingerprint != two.Fingerprint {
		t.Fatalf("non-deterministic snapshot: %#v %#v", one, two)
	}
	for key, value := range one.Values {
		if strings.Contains(value, "secret") || strings.Contains(value, "admin-") {
			t.Fatalf("snapshot value %s leaked secret: %q", key, value)
		}
	}
	if one.Values["secret.admin_token"] != "[REDACTED]" || one.Values["secret.commit_hmac_key"] != "[REDACTED]" {
		t.Fatalf("missing redaction: %#v", one.Values)
	}
	encoded, err := json.Marshal(one)
	if err != nil || strings.Contains(string(encoded), "admin-secret") || strings.Contains(string(encoded), "access-secret") {
		t.Fatalf("snapshot JSON leaked secret: %s (%v)", encoded, err)
	}
}

func TestMalformedOperationalEnvironmentIsRejected(t *testing.T) {
	for _, key := range []string{"VELOX_TASKGRAPH_TICK", "VELOX_CACHE_LOOKAHEAD_JOBS", "SOCIAL_API_TIMEOUT_MS"} {
		t.Setenv(key, "")
	}
	t.Setenv("VELOX_TASKGRAPH_TICK", "not-a-duration")
	t.Setenv("VELOX_CACHE_LOOKAHEAD_JOBS", "zero")
	t.Setenv("SOCIAL_API_TIMEOUT_MS", "-1")
	cfg := &Config{
		Database: DatabaseConfig{DBPath: t.TempDir() + "/velox.db"},
		Workers:  WorkersConfig{AllowedWorkerIDs: []string{"worker-a", "worker-b"}},
	}
	cfg.LoadOperationalRuntime()
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate accepted malformed operational environment")
	} else if !strings.Contains(err.Error(), "VELOX_TASKGRAPH_TICK") || !strings.Contains(err.Error(), "VELOX_CACHE_LOOKAHEAD_JOBS") {
		t.Fatalf("Validate error omitted malformed keys: %v", err)
	}
}

func TestRuntimeSourcesTrackEnvFileAndReset(t *testing.T) {
	os.Unsetenv("VELOX_TASKGRAPH_TICK")
	defer os.Unsetenv("VELOX_TASKGRAPH_TICK")
	cfg := &Config{Database: DatabaseConfig{DBPath: t.TempDir() + "/velox.db"}}
	cfg.LoadOperationalRuntime()
	if got := cfg.Runtime.Sources["scheduler.taskgraph_tick"]; got != SourceDefault {
		t.Fatalf("default source = %q, want %q", got, SourceDefault)
	}
	// SourceFile is exercised by LoadEnvFile in the config package; use a
	// temporary file and clear the process variable first so the loader owns
	// the value and records its origin.
	path := t.TempDir() + "/runtime.env"
	if err := os.WriteFile(path, []byte("VELOX_TASKGRAPH_TICK=7s\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := LoadEnvFile(path); err != nil {
		t.Fatal(err)
	}
	cfg.LoadOperationalRuntime()
	if got := cfg.Runtime.Sources["scheduler.taskgraph_tick"]; got != SourceFile {
		t.Fatalf("file source = %q, want %q", got, SourceFile)
	}
}

func TestFingerprintChangesForTypedRuntimeValue(t *testing.T) {
	cfg := &Config{Database: DatabaseConfig{DBPath: t.TempDir() + "/velox.db"}}
	cfg.LoadOperationalRuntime()
	first := cfg.Snapshot().Fingerprint
	cfg.Runtime.Scheduler.TaskGraphTick += time.Second
	second := cfg.Snapshot().Fingerprint
	if first == second {
		t.Fatal("fingerprint did not change after typed runtime value changed")
	}
}

func TestConfigFreezeLifecycle(t *testing.T) {
	cfg := &Config{
		Database: DatabaseConfig{DBPath: t.TempDir() + "/velox.db"},
		Server:   ServerConfig{GRPCPort: 50051, GRPCPushMode: true},
		Workers:  WorkersConfig{AllowedWorkerIDs: []string{"worker-a", "worker-b"}},
	}
	if cfg.IsFrozen() {
		t.Fatal("new config unexpectedly frozen")
	}
	if err := cfg.Freeze(); err != nil {
		t.Fatalf("Freeze: %v", err)
	}
	if !cfg.IsFrozen() {
		t.Fatal("Freeze did not mark config frozen")
	}
}

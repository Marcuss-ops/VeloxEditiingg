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
		"VELOX_CACHE_SNAPSHOT_INTERVAL", "VELOX_CALENDAR_SCHEDULER_INTERVAL_SECONDS",
		"VELOX_ALERT_EVALUATION_INTERVAL", "VELOX_ALERT_COOLDOWN",
		"VELOX_RETENTION_WORKER_METRICS_DAYS", "VELOX_RETENTION_WORKER_EVENTS_DAYS",
		"VELOX_RETENTION_WORKER_RESOURCE_RAW_DAYS", "VELOX_RETENTION_WORKER_RESOURCE_ROLLUP_DAYS",
		"VELOX_CPU_CORE_SECOND_COST", "VELOX_NETWORK_GB_COST", "VELOX_STORAGE_GB_COST",
	} {
		t.Setenv(key, "")
	}
	cfg := FromRaw(RawConfigFromEnv())
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
	if !cfg.Runtime.Cache.StrictPrefetchClaim {
		t.Fatal("strict prefetch claim default is disabled")
	}
	if cfg.Runtime.Scheduler.CalendarInterval != 30*time.Second {
		t.Fatalf("calendar interval = %s", cfg.Runtime.Scheduler.CalendarInterval)
	}
	if cfg.Runtime.Metrics.CPUCostEUR != 5e-6 || cfg.Runtime.Metrics.NetworkCostEUR != 0.01 || cfg.Runtime.Metrics.StorageCostEUR != 0.00012 {
		t.Fatalf("metrics defaults = %+v", cfg.Runtime.Metrics)
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
			Scheduler:     SchedulerConfig{TaskGraphTick: 2 * time.Second, ArtifactReconcileInterval: 15 * time.Minute, MetricsSnapshotInterval: 5 * time.Minute, CalendarInterval: 30 * time.Second, RestartableMaxRetries: 5},
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
	t.Setenv("VELOX_CALENDAR_SCHEDULER_INTERVAL_SECONDS", "zero")
	t.Setenv("SOCIAL_API_TIMEOUT_MS", "-1")
	cfg := FromRaw(RawConfigFromEnv())
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate accepted malformed operational environment")
	} else if !strings.Contains(err.Error(), "VELOX_TASKGRAPH_TICK") || !strings.Contains(err.Error(), "VELOX_CACHE_LOOKAHEAD_JOBS") {
		t.Fatalf("Validate error omitted malformed keys: %v", err)
	}
}

func TestRuntimeSourcesTrackEnvFileAndReset(t *testing.T) {
	const key = "VELOX_TASKGRAPH_TICK"
	oldValue, wasSet := os.LookupEnv(key)
	if err := os.Unsetenv(key); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if wasSet {
			_ = os.Setenv(key, oldValue)
		} else {
			_ = os.Unsetenv(key)
		}
	})
	raw := RawConfigFromEnv()
	_ = FromRaw(raw)
	if got := raw.Source("VELOX_TASKGRAPH_TICK"); got != SourceDefault {
		t.Fatalf("default source = %q, want %q", got, SourceDefault)
	}
	// SourceFile is exercised by LoadEnvFile in the config package; use a
	// temporary file and clear the process variable first so the loader owns
	// the value and records its origin.
	path := t.TempDir() + "/runtime.env"
	if err := os.WriteFile(path, []byte("VELOX_TASKGRAPH_TICK=7s\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	raw, err := RawConfigFromEnvFile(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = FromRaw(raw)
	if got := raw.Source("VELOX_TASKGRAPH_TICK"); got != SourceFile {
		t.Fatalf("file source = %q, want %q", got, SourceFile)
	}
}

func TestFingerprintChangesForTypedRuntimeValue(t *testing.T) {
	cfg := FromRaw(RawConfigFromEnv())
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

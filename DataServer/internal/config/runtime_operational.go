package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const RuntimeConfigSchemaVersion = "velox.runtime.v1"

// CacheConfig controls master-side cache protection refreshes.
type CacheConfig struct {
	ProtectedAssetLookaheadJobs int
	SnapshotInterval            time.Duration
}

// SchedulerConfig controls periodic master scheduling/reconciliation loops.
type SchedulerConfig struct {
	TaskGraphTick             time.Duration
	ArtifactReconcileInterval time.Duration
	MetricsSnapshotInterval   time.Duration
	RestartableMaxRetries     int
}

// MetricsConfig controls the periodic metrics supervisor.
type MetricsConfig struct {
	Tick           time.Duration
	AttemptLimit   int
	SeenIDsCap     int
	CPUCostEUR     float64
	NetworkCostEUR float64
	StorageCostEUR float64
}

// RuntimeConfigSource records where a runtime setting came from.
type RuntimeConfigSource string

const (
	SourceDefault RuntimeConfigSource = "default"
	SourceEnv     RuntimeConfigSource = "env"
	SourceFile    RuntimeConfigSource = "file"
)

// RuntimeSnapshot is safe to emit at startup. Values containing credentials
// are represented by [REDACTED], never by their plaintext content.
type RuntimeSnapshot struct {
	SchemaVersion string            `json:"schema_version"`
	Fingerprint   string            `json:"fingerprint"`
	Sources       map[string]string `json:"sources"`
	Values        map[string]string `json:"values"`
}

var envFileKeys = struct {
	sync.RWMutex
	keys map[string]struct{}
}{keys: make(map[string]struct{})}

func markEnvFileKey(key string) {
	envFileKeys.Lock()
	envFileKeys.keys[key] = struct{}{}
	envFileKeys.Unlock()
}

func resetEnvFileKeys() {
	envFileKeys.Lock()
	envFileKeys.keys = make(map[string]struct{})
	envFileKeys.Unlock()
}

func sourceForEnv(key string) RuntimeConfigSource {
	if _, ok := os.LookupEnv(key); !ok {
		return SourceDefault
	}
	envFileKeys.RLock()
	_, fromFile := envFileKeys.keys[key]
	envFileKeys.RUnlock()
	if fromFile {
		return SourceFile
	}
	return SourceEnv
}

func loadOperationalRuntimeConfig() (SupervisorConfig, CacheConfig, SchedulerConfig, MetricsConfig, AlertConfig, map[string]RuntimeConfigSource) {
	sources := make(map[string]RuntimeConfigSource)
	source := func(field, env string) { sources[field] = sourceForEnv(env) }

	supervisor := loadSupervisorConfig()
	source("supervisor.require_live_workers", "VELOX_REQUIRE_LIVE_WORKERS")
	source("supervisor.critical_max_retries", "VELOX_CRITICAL_MAX_RETRIES")
	source("supervisor.critical_fail_after", "VELOX_CRITICAL_FAIL_AFTER")

	cache := CacheConfig{
		ProtectedAssetLookaheadJobs: intFromEnv("VELOX_CACHE_LOOKAHEAD_JOBS", 10, 1),
		SnapshotInterval:            durationFromEnv("VELOX_CACHE_SNAPSHOT_INTERVAL", 30*time.Second),
	}
	source("cache.protected_asset_lookahead_jobs", "VELOX_CACHE_LOOKAHEAD_JOBS")
	source("cache.snapshot_interval", "VELOX_CACHE_SNAPSHOT_INTERVAL")

	scheduler := SchedulerConfig{
		TaskGraphTick:             durationFromEnv("VELOX_TASKGRAPH_TICK", 2*time.Second),
		ArtifactReconcileInterval: durationFromEnv("VELOX_ARTIFACT_RECONCILE_INTERVAL", 15*time.Minute),
		MetricsSnapshotInterval:   durationFromEnv("VELOX_METRICS_SNAPSHOT_INTERVAL", 5*time.Minute),
		RestartableMaxRetries:     intFromEnv("VELOX_RESTARTABLE_MAX_RETRIES", 5, 1),
	}
	source("scheduler.taskgraph_tick", "VELOX_TASKGRAPH_TICK")
	source("scheduler.artifact_reconcile_interval", "VELOX_ARTIFACT_RECONCILE_INTERVAL")
	source("scheduler.metrics_snapshot_interval", "VELOX_METRICS_SNAPSHOT_INTERVAL")
	source("scheduler.restartable_max_retries", "VELOX_RESTARTABLE_MAX_RETRIES")

	metrics := MetricsConfig{
		Tick:           durationFromEnv("VELOX_METRICS_TICK", 15*time.Second),
		AttemptLimit:   intFromEnv("VELOX_METRICS_ATTEMPT_LIMIT", 1000, 1),
		SeenIDsCap:     intFromEnv("VELOX_METRICS_SEEN_IDS_CAP", 10000, 1),
		CPUCostEUR:     floatFromEnv("VELOX_CPU_CORE_SECOND_COST", 5e-6, 0),
		NetworkCostEUR: floatFromEnv("VELOX_NETWORK_GB_COST", 0.01, 0),
		StorageCostEUR: floatFromEnv("VELOX_STORAGE_GB_COST", 0.00012, 0),
	}
	source("metrics.tick", "VELOX_METRICS_TICK")
	source("metrics.attempt_limit", "VELOX_METRICS_ATTEMPT_LIMIT")
	source("metrics.seen_ids_cap", "VELOX_METRICS_SEEN_IDS_CAP")
	source("metrics.cpu_cost_eur", "VELOX_CPU_CORE_SECOND_COST")
	source("metrics.network_cost_eur", "VELOX_NETWORK_GB_COST")
	source("metrics.storage_cost_eur", "VELOX_STORAGE_GB_COST")

	alerts := loadAlertConfig()
	source("alerts.error_rate_pct", "VELOX_ALERT_ERROR_RATE_PCT")
	source("alerts.p95_wall_ms", "VELOX_ALERT_P95_WALL_MS")
	source("alerts.disk_free_gb", "VELOX_ALERT_DISK_FREE_GB")
	source("alerts.ffmpeg_min", "VELOX_ALERT_FFMPEG_MIN")
	source("alerts.webhook_url", "VELOX_ALERT_WEBHOOK_URL")
	source("alerts.webhook_type", "VELOX_ALERT_WEBHOOK_TYPE")
	source("alerts.evaluation_interval", "VELOX_ALERT_EVALUATION_INTERVAL")
	source("alerts.cooldown", "VELOX_ALERT_COOLDOWN")
	source("social.base_url", "SOCIAL_API_URL")
	source("social.api_key", "SOCIAL_API_TOKEN")
	source("social.callback_base_url", "SOCIAL_CALLBACK_BASE_URL")
	source("social.timeout", "SOCIAL_API_TIMEOUT_MS")
	source("social.max_retries", "SOCIAL_API_RETRIES")
	source("logging.quiet", "VELOX_QUIET_LOGS")
	source("logging.json_output", "VELOX_JSON_LOGS")
	source("logging.debug", "VELOX_DEBUG")
	source("telemetry.exporter", "VELOX_OTEL_EXPORTER")
	source("telemetry.endpoint", "VELOX_OTEL_ENDPOINT")
	source("telemetry.version", "VELOX_VERSION")
	source("telemetry.insecure", "VELOX_OTEL_INSECURE")
	source("telemetry.measure_enqueue_allocations", "VELOX_ENQUEUE_MEASURE_ALLOCATIONS")
	return supervisor, cache, scheduler, metrics, alerts, sources
}

func operationalParseErrors() []string {
	var errors []string
	checkDuration := func(key string) {
		if raw, ok := os.LookupEnv(key); ok && strings.TrimSpace(raw) != "" {
			value, err := time.ParseDuration(strings.TrimSpace(raw))
			if err != nil || value <= 0 {
				errors = append(errors, fmt.Sprintf("%s must be a positive duration, got %q", key, raw))
			}
		}
	}
	checkInt := func(key string, min int) {
		if raw, ok := os.LookupEnv(key); ok && strings.TrimSpace(raw) != "" {
			value, err := strconv.Atoi(strings.TrimSpace(raw))
			if err != nil || value < min {
				errors = append(errors, fmt.Sprintf("%s must be an integer >= %d, got %q", key, min, raw))
			}
		}
	}
	for _, key := range []string{"VELOX_CACHE_SNAPSHOT_INTERVAL", "VELOX_TASKGRAPH_TICK", "VELOX_ARTIFACT_RECONCILE_INTERVAL", "VELOX_METRICS_SNAPSHOT_INTERVAL", "VELOX_METRICS_TICK", "VELOX_ALERT_EVALUATION_INTERVAL", "VELOX_ALERT_COOLDOWN"} {
		checkDuration(key)
	}
	for _, item := range []struct {
		key string
		min int
	}{
		{"VELOX_CACHE_LOOKAHEAD_JOBS", 1},
		{"VELOX_RESTARTABLE_MAX_RETRIES", 1},
		{"VELOX_METRICS_ATTEMPT_LIMIT", 1},
		{"VELOX_METRICS_SEEN_IDS_CAP", 1},
		{"SOCIAL_API_TIMEOUT_MS", 0},
		{"SOCIAL_API_RETRIES", 0},
	} {
		checkInt(item.key, item.min)
	}
	return errors
}

func durationFromEnv(key string, fallback time.Duration) time.Duration {
	raw, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback
	}
	d, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}

// LoadCacheConfigFromEnv exposes strict parsing for cache consumers while
// keeping all environment access inside the config package.
func LoadCacheConfigFromEnv() (CacheConfig, error) {
	cache := CacheConfig{
		ProtectedAssetLookaheadJobs: intFromEnv("VELOX_CACHE_LOOKAHEAD_JOBS", 10, 1),
		SnapshotInterval:            durationFromEnv("VELOX_CACHE_SNAPSHOT_INTERVAL", 30*time.Second),
	}
	for _, errText := range operationalParseErrors() {
		if strings.HasPrefix(errText, "VELOX_CACHE_") {
			return cache, fmt.Errorf("config: %s", errText)
		}
	}
	return cache, nil
}

func loadSocialConfig() SocialConfig {
	return SocialConfig{
		BaseURL:         strings.TrimSpace(os.Getenv("SOCIAL_API_URL")),
		APIKey:          os.Getenv("SOCIAL_API_TOKEN"),
		CallbackBaseURL: strings.TrimSpace(os.Getenv("SOCIAL_CALLBACK_BASE_URL")),
		Timeout:         time.Duration(intFromEnv("SOCIAL_API_TIMEOUT_MS", 30000, 0)) * time.Millisecond,
		MaxRetries:      intFromEnv("SOCIAL_API_RETRIES", 0, 0),
	}
}

func loadLoggingConfig() LoggingConfig {
	return LoggingConfig{
		Quiet:      boolFromEnv("VELOX_QUIET_LOGS", false),
		JSONOutput: boolFromEnv("VELOX_JSON_LOGS", false),
		Debug:      boolFromEnv("VELOX_DEBUG", false),
	}
}

func loadTelemetryConfig() TelemetryConfig {
	return TelemetryConfig{
		Exporter:                  strings.ToLower(strings.TrimSpace(os.Getenv("VELOX_OTEL_EXPORTER"))),
		Endpoint:                  strings.TrimSpace(os.Getenv("VELOX_OTEL_ENDPOINT")),
		Version:                   strings.TrimSpace(os.Getenv("VELOX_VERSION")),
		Insecure:                  boolFromEnv("VELOX_OTEL_INSECURE", true),
		MeasureEnqueueAllocations: strings.TrimSpace(os.Getenv("VELOX_ENQUEUE_MEASURE_ALLOCATIONS")) == "1",
	}
}

func (c *Config) LoadOperationalRuntime() {
	supervisor, cache, scheduler, metrics, alerts, sources := loadOperationalRuntimeConfig()
	c.parseErrors = operationalParseErrors()
	c.Runtime.Supervisor = supervisor
	c.Runtime.Cache = cache
	c.Runtime.Scheduler = scheduler
	c.Runtime.Metrics = metrics
	c.Runtime.Alerts = alerts
	c.Runtime.Social = loadSocialConfig()
	c.Runtime.Logging = loadLoggingConfig()
	c.Runtime.Telemetry = loadTelemetryConfig()
	c.Runtime.Sources = sources
	// Keep the old top-level fields populated while callers migrate to
	// RuntimeConfig. New runtime wiring must use c.Runtime.*.
	c.Supervisor = supervisor
	c.Alerts = alerts
}

// LoadFromEnv is the canonical bootstrap pipeline: load, parse, validate,
// freeze. FromEnv remains available for tests and callers that need to inspect
// an invalid configuration before deciding how to report it.
func LoadFromEnv() (*Config, error) {
	c := FromEnv()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if err := c.Freeze(); err != nil {
		return nil, err
	}
	return c, nil
}

// Freeze marks a validated configuration as the immutable bootstrap snapshot.
// Go permits field assignment after this point, so consumers should retain the
// frozen pointer and never mutate it; IsFrozen is available to guard setters.
func (c *Config) Freeze() error {
	if c == nil {
		return fmt.Errorf("config: cannot freeze nil Config")
	}
	if err := c.Validate(); err != nil {
		return err
	}
	c.frozen = true
	return nil
}

func (c *Config) IsFrozen() bool { return c != nil && c.frozen }

// Snapshot returns a deterministic, redacted startup representation.
func (c *Config) Snapshot() RuntimeSnapshot {
	if c == nil {
		return RuntimeSnapshot{SchemaVersion: RuntimeConfigSchemaVersion}
	}
	values := map[string]string{
		"runtime.data_dir":                              c.Runtime.DataDir,
		"runtime.environment":                           c.Runtime.Environment,
		"runtime.supervisor.critical_max_retries":       fmt.Sprint(c.Runtime.Supervisor.CriticalMaxRetries),
		"runtime.cache.lookahead_jobs":                  fmt.Sprint(c.Runtime.Cache.ProtectedAssetLookaheadJobs),
		"runtime.cache.snapshot_interval":               c.Runtime.Cache.SnapshotInterval.String(),
		"runtime.scheduler.taskgraph_tick":              c.Runtime.Scheduler.TaskGraphTick.String(),
		"runtime.scheduler.artifact_reconcile_interval": c.Runtime.Scheduler.ArtifactReconcileInterval.String(),
		"runtime.scheduler.metrics_snapshot_interval":   c.Runtime.Scheduler.MetricsSnapshotInterval.String(),
		"runtime.scheduler.restartable_max_retries":     fmt.Sprint(c.Runtime.Scheduler.RestartableMaxRetries),
		"runtime.metrics.tick":                          c.Runtime.Metrics.Tick.String(),
		"runtime.metrics.attempt_limit":                 fmt.Sprint(c.Runtime.Metrics.AttemptLimit),
		"runtime.metrics.seen_ids_cap":                  fmt.Sprint(c.Runtime.Metrics.SeenIDsCap),
		"runtime.metrics.cpu_cost_eur":                  fmt.Sprint(c.Runtime.Metrics.CPUCostEUR),
		"runtime.metrics.network_cost_eur":              fmt.Sprint(c.Runtime.Metrics.NetworkCostEUR),
		"runtime.metrics.storage_cost_eur":              fmt.Sprint(c.Runtime.Metrics.StorageCostEUR),
		"runtime.supervisor.require_live_workers":       fmt.Sprint(c.Runtime.Supervisor.RequireLiveWorkers),
		"runtime.supervisor.critical_fail_after":        fmt.Sprint(c.Runtime.Supervisor.CriticalFailAfter),
		"runtime.alerts.error_rate_pct":                 fmt.Sprint(c.Runtime.Alerts.ErrorRatePct),
		"runtime.alerts.p95_wall_ms":                    fmt.Sprint(c.Runtime.Alerts.P95WallMS),
		"runtime.alerts.disk_free_gb":                   fmt.Sprint(c.Runtime.Alerts.DiskFreeGB),
		"runtime.alerts.ffmpeg_min":                     fmt.Sprint(c.Runtime.Alerts.FFmpegMin),
		"runtime.alerts.webhook_url":                    redactPresence(c.Runtime.Alerts.WebhookURL),
		"runtime.alerts.webhook_type":                   c.Runtime.Alerts.WebhookType,
		"runtime.alerts.evaluation_interval":            c.Runtime.Alerts.EvaluationInterval.String(),
		"runtime.alerts.cooldown":                       c.Runtime.Alerts.Cooldown.String(),
		"runtime.social.base_url":                       c.Runtime.Social.BaseURL,
		"runtime.social.api_key":                        redactPresence(c.Runtime.Social.APIKey),
		"runtime.social.callback_base_url":              c.Runtime.Social.CallbackBaseURL,
		"runtime.social.timeout":                        c.Runtime.Social.Timeout.String(),
		"runtime.social.max_retries":                    fmt.Sprint(c.Runtime.Social.MaxRetries),
		"runtime.logging.quiet":                         fmt.Sprint(c.Runtime.Logging.Quiet),
		"runtime.logging.json_output":                   fmt.Sprint(c.Runtime.Logging.JSONOutput),
		"runtime.logging.debug":                         fmt.Sprint(c.Runtime.Logging.Debug),
		"runtime.telemetry.exporter":                    c.Runtime.Telemetry.Exporter,
		"runtime.telemetry.endpoint":                    c.Runtime.Telemetry.Endpoint,
		"runtime.telemetry.version":                     c.Runtime.Telemetry.Version,
		"runtime.telemetry.insecure":                    fmt.Sprint(c.Runtime.Telemetry.Insecure),
		"runtime.telemetry.measure_enqueue_allocations": fmt.Sprint(c.Runtime.Telemetry.MeasureEnqueueAllocations),
		"database.driver":                               c.Database.Driver,
		"database.db_path":                              c.Database.DBPath,
		"secret.admin_token":                            redactPresence(c.Auth.AdminToken),
		"secret.commit_hmac_key":                        redactPresence(c.Runtime.CommitHMACKey),
		"secret.storage_access_key":                     redactPresence(c.Storage.AccessKeyID),
		"secret.storage_secret_key":                     redactPresence(c.Storage.SecretKey),
	}
	sources := make(map[string]string, len(c.Runtime.Sources))
	for k, v := range c.Runtime.Sources {
		sources[k] = string(v)
	}
	fingerprint := fingerprintSnapshot(values)
	return RuntimeSnapshot{SchemaVersion: RuntimeConfigSchemaVersion, Fingerprint: fingerprint, Sources: sources, Values: values}
}

func (c *Config) SnapshotJSON() ([]byte, error) {
	s := c.Snapshot()
	return json.Marshal(s)
}

func redactPresence(value string) string {
	if strings.TrimSpace(value) == "" {
		return "<unset>"
	}
	return "[REDACTED]"
}

func fingerprintSnapshot(values map[string]string) string {
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	canonical := make([]string, 0, len(keys))
	for _, k := range keys {
		canonical = append(canonical, k+"="+values[k])
	}
	sum := sha256.Sum256([]byte(strings.Join(canonical, "\n")))
	return hex.EncodeToString(sum[:])
}

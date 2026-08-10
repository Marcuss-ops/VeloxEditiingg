package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
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
	CalendarInterval          time.Duration
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

// RuntimeSnapshot is safe to emit at startup. Values containing credentials
// are represented by [REDACTED], never by their plaintext content.
type RuntimeSnapshot struct {
	SchemaVersion string            `json:"schema_version"`
	Fingerprint   string            `json:"fingerprint"`
	Values        map[string]string `json:"values"`
}

func loadOperationalRuntimeConfig(raw RawConfig) (SupervisorConfig, CacheConfig, SchedulerConfig, MetricsConfig, AlertConfig) {
	supervisor := loadSupervisorConfig(raw)

	cache := CacheConfig{
		ProtectedAssetLookaheadJobs: raw.Int("VELOX_CACHE_LOOKAHEAD_JOBS", 10, 1),
		SnapshotInterval:            raw.Duration("VELOX_CACHE_SNAPSHOT_INTERVAL", 30*time.Second),
	}
	scheduler := SchedulerConfig{
		TaskGraphTick:             raw.Duration("VELOX_TASKGRAPH_TICK", 2*time.Second),
		ArtifactReconcileInterval: raw.Duration("VELOX_ARTIFACT_RECONCILE_INTERVAL", 15*time.Minute),
		MetricsSnapshotInterval:   raw.Duration("VELOX_METRICS_SNAPSHOT_INTERVAL", 5*time.Minute),
		CalendarInterval:          time.Duration(raw.Int("VELOX_CALENDAR_SCHEDULER_INTERVAL_SECONDS", 30, 1)) * time.Second,
		RestartableMaxRetries:     raw.Int("VELOX_RESTARTABLE_MAX_RETRIES", 5, 1),
	}
	metrics := MetricsConfig{
		Tick:           raw.Duration("VELOX_METRICS_TICK", 15*time.Second),
		AttemptLimit:   raw.Int("VELOX_METRICS_ATTEMPT_LIMIT", 1000, 1),
		SeenIDsCap:     raw.Int("VELOX_METRICS_SEEN_IDS_CAP", 10000, 1),
		CPUCostEUR:     raw.Float("VELOX_CPU_CORE_SECOND_COST", 5e-6, 0),
		NetworkCostEUR: raw.Float("VELOX_NETWORK_GB_COST", 0.01, 0),
		StorageCostEUR: raw.Float("VELOX_STORAGE_GB_COST", 0.00012, 0),
	}
	alerts := loadAlertConfig(raw)
	return supervisor, cache, scheduler, metrics, alerts
}

func operationalParseErrors(raw RawConfig) []string {
	var errors []string
	checkDuration := func(key string) {
		if rawValue, ok := raw.Lookup(key); ok && strings.TrimSpace(rawValue) != "" {
			value, err := time.ParseDuration(strings.TrimSpace(rawValue))
			if err != nil || value <= 0 {
				errors = append(errors, fmt.Sprintf("%s must be a positive duration, got %q", key, rawValue))
			}
		}
	}
	checkInt := func(key string, min int) {
		if rawValue, ok := raw.Lookup(key); ok && strings.TrimSpace(rawValue) != "" {
			value, err := strconv.Atoi(strings.TrimSpace(rawValue))
			if err != nil || value < min {
				errors = append(errors, fmt.Sprintf("%s must be an integer >= %d, got %q", key, min, rawValue))
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
		{"VELOX_CALENDAR_SCHEDULER_INTERVAL_SECONDS", 1},
		{"SOCIAL_API_TIMEOUT_MS", 0},
	} {
		checkInt(item.key, item.min)
	}
	return errors
}

func loadSocialConfig(raw RawConfig) SocialConfig {
	return SocialConfig{
		BaseURL:         strings.TrimSpace(raw.Get("SOCIAL_API_URL")),
		APIKey:          raw.Get("SOCIAL_API_TOKEN"),
		CallbackBaseURL: strings.TrimSpace(raw.Get("SOCIAL_CALLBACK_BASE_URL")),
		Timeout:         time.Duration(raw.Int("SOCIAL_API_TIMEOUT_MS", 30000, 0)) * time.Millisecond,
	}
}

func loadLoggingConfig(raw RawConfig) LoggingConfig {
	return LoggingConfig{
		Quiet:      raw.Bool("VELOX_QUIET_LOGS", false),
		JSONOutput: raw.Bool("VELOX_JSON_LOGS", false),
		Debug:      raw.Bool("VELOX_DEBUG", false),
	}
}

func loadTelemetryConfig(raw RawConfig) TelemetryConfig {
	return TelemetryConfig{
		Exporter:                  strings.ToLower(strings.TrimSpace(raw.Get("VELOX_OTEL_EXPORTER"))),
		Endpoint:                  strings.TrimSpace(raw.Get("VELOX_OTEL_ENDPOINT")),
		Version:                   strings.TrimSpace(raw.Get("VELOX_VERSION")),
		Insecure:                  raw.Bool("VELOX_OTEL_INSECURE", true),
		MeasureEnqueueAllocations: strings.TrimSpace(raw.Get("VELOX_ENQUEUE_MEASURE_ALLOCATIONS")) == "1",
	}
}

func loadOperationalRuntimeConfigInto(c *Config, raw RawConfig) {
	supervisor, cache, scheduler, metrics, alerts := loadOperationalRuntimeConfig(raw)
	c.Runtime.Supervisor = supervisor
	c.Runtime.Cache = cache
	c.Runtime.Scheduler = scheduler
	c.Runtime.Metrics = metrics
	c.Runtime.Alerts = alerts
	c.Runtime.Social = loadSocialConfig(raw)
	c.Runtime.Logging = loadLoggingConfig(raw)
	c.Runtime.Telemetry = loadTelemetryConfig(raw)
}

func loadProcessConfig(c *Config, raw RawConfig) {
	c.Runtime.CosignSkipVerify = strings.TrimSpace(raw.Get("VELOX_SKIP_COSIGN_VERIFY")) == "1"
	c.Runtime.CosignOverrideReason = strings.TrimSpace(raw.Get("VELOX_COSIGN_OVERRIDE_REASON"))
	c.Runtime.AssetRewriteDevBypass = raw.Bool("VELOX_ASSET_REWRITE_DEV_BYPASS", false)
	c.Runtime.FFProbeVerifyMode = raw.Get("VELOX_FFPROBE_VERIFY_ON_FINALIZE")
	c.Runtime.SystemPath = raw.Get("PATH")
	c.Runtime.Credentials = loadCredentialsConfig(raw)
}

func loadCredentialsConfig(raw RawConfig) CredentialsConfig {
	cfg := CredentialsConfig{CurrentVersion: 1, Historical: make(map[int]CredentialKeyConfig)}
	if version := strings.TrimSpace(raw.Get("VELOX_CREDENTIAL_KEY_VERSION")); version != "" {
		if parsed, err := strconv.Atoi(version); err == nil && parsed > 0 {
			cfg.CurrentVersion = parsed
		}
	}
	cfg.Current = CredentialKeyConfig{Value: raw.Get("VELOX_CREDENTIAL_KEY"), File: raw.Get("VELOX_CREDENTIAL_KEY_FILE")}
	for version := 1; version <= 32; version++ {
		if version == cfg.CurrentVersion {
			continue
		}
		value := raw.Get(fmt.Sprintf("VELOX_CREDENTIAL_KEY_%d", version))
		file := raw.Get(fmt.Sprintf("VELOX_CREDENTIAL_KEY_%d_FILE", version))
		if value != "" || file != "" {
			cfg.Historical[version] = CredentialKeyConfig{Value: value, File: file}
		}
	}
	return cfg
}

// LoadFromEnv is the canonical bootstrap pipeline: load, parse, validate,
// freeze. FromEnv remains available for tests and callers that need to inspect
// an invalid configuration before deciding how to report it.
func LoadFromEnv() (*Config, error) {
	return LoadFromRaw(RawConfigFromEnv())
}

// LoadFromRaw validates and freezes one captured raw snapshot. The optional
// env-file source information must be attached to that snapshot before this
// boundary; runtime consumers never reload process environment state.
func LoadFromRaw(raw RawConfig) (*Config, error) {
	c := FromRaw(raw)
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if err := validateBootstrapEndpoints(c); err != nil {
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
		"compatibility.mode":                            c.Compatibility.Mode,
		"runtime.supervisor.critical_max_retries":       fmt.Sprint(c.Runtime.Supervisor.CriticalMaxRetries),
		"runtime.cache.lookahead_jobs":                  fmt.Sprint(c.Runtime.Cache.ProtectedAssetLookaheadJobs),
		"runtime.cache.snapshot_interval":               c.Runtime.Cache.SnapshotInterval.String(),
		"runtime.scheduler.taskgraph_tick":              c.Runtime.Scheduler.TaskGraphTick.String(),
		"runtime.scheduler.artifact_reconcile_interval": c.Runtime.Scheduler.ArtifactReconcileInterval.String(),
		"runtime.scheduler.metrics_snapshot_interval":   c.Runtime.Scheduler.MetricsSnapshotInterval.String(),
		"runtime.scheduler.calendar_interval":           c.Runtime.Scheduler.CalendarInterval.String(),
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
	fingerprint := fingerprintSnapshot(values)
	return RuntimeSnapshot{SchemaVersion: RuntimeConfigSchemaVersion, Fingerprint: fingerprint, Values: values}
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

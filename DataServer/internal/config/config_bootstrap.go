package config

import (
	"strings"
	"time"
)

func loadSupervisorConfig(raw RawConfig) SupervisorConfig {
	return SupervisorConfig{
		RequireLiveWorkers: raw.Bool("VELOX_REQUIRE_LIVE_WORKERS", false),
		CriticalMaxRetries: raw.Int("VELOX_CRITICAL_MAX_RETRIES", 0, 0),
		CriticalFailAfter:  raw.Int("VELOX_CRITICAL_FAIL_AFTER", 10, 1),
	}
}

func loadAlertConfig(raw RawConfig) AlertConfig {
	return AlertConfig{
		ErrorRatePct:       raw.Float("VELOX_ALERT_ERROR_RATE_PCT", 5.0, 0),
		P95WallMS:          int64(raw.Int("VELOX_ALERT_P95_WALL_MS", 300000, 0)),
		DiskFreeGB:         raw.Float("VELOX_ALERT_DISK_FREE_GB", 10.0, 0),
		FFmpegMin:          raw.Float("VELOX_ALERT_FFMPEG_MIN", 1.5, 0),
		WebhookURL:         strings.TrimSpace(raw.Get("VELOX_ALERT_WEBHOOK_URL")),
		WebhookType:        strings.ToLower(strings.TrimSpace(raw.Get("VELOX_ALERT_WEBHOOK_TYPE"))),
		EvaluationInterval: raw.Duration("VELOX_ALERT_EVALUATION_INTERVAL", 30*time.Second),
		Cooldown:           raw.Duration("VELOX_ALERT_COOLDOWN", 5*time.Minute),
	}
}

func loadFleetConfig(raw RawConfig) FleetConfig {
	return FleetConfig{
		SmokeMode:          strings.ToLower(strings.TrimSpace(raw.Get("VELOX_SMOKE_MODE"))),
		SmokeAssetID:       strings.TrimSpace(raw.Get("VELOX_SMOKE_ASSET_ID")),
		SmokeDriveFolderID: strings.TrimSpace(raw.Get("VELOX_SMOKE_DRIVE_FOLDER_ID")),
	}
}

func loadCompatibilityConfig(raw RawConfig) CompatibilityConfig {
	mode := strings.ToLower(strings.TrimSpace(raw.Get("VELOX_COMPATIBILITY_MODE")))
	// Strict is the canonical default: the master must reject registered
	// legacy aliases at the submission boundary unless an operator
	// explicitly opts back into VELOX_COMPATIBILITY_MODE=compat for the
	// legacy-producer drain window.
	if mode == "" {
		mode = "strict"
	}
	aliases := []RetiredEnvAlias{
		{Env: "SOCIAL_GATEWAY_URL", Canonical: "SOCIAL_API_URL"},
		{Env: "SOCIAL_GATEWAY_API_KEY", Canonical: "SOCIAL_API_TOKEN"},
		{Env: "SOCIAL_GATEWAY_CALLBACK_BASE_URL", Canonical: "SOCIAL_CALLBACK_BASE_URL"},
	}
	configured := make([]RetiredEnvAlias, 0, len(aliases))
	for _, alias := range aliases {
		if strings.TrimSpace(raw.Get(alias.Env)) != "" {
			configured = append(configured, alias)
		}
	}
	return CompatibilityConfig{Mode: mode, RetiredSocialAliases: configured}
}

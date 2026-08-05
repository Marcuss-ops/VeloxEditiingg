package config

import (
	"os"
	"strings"
)

func loadSupervisorConfig() SupervisorConfig {
	return SupervisorConfig{
		RequireLiveWorkers: boolFromEnv("VELOX_REQUIRE_LIVE_WORKERS", false),
		CriticalMaxRetries: intFromEnv("VELOX_CRITICAL_MAX_RETRIES", 0, 0),
		CriticalFailAfter:  intFromEnv("VELOX_CRITICAL_FAIL_AFTER", 10, 1),
	}
}

func loadAlertConfig() AlertConfig {
	return AlertConfig{
		ErrorRatePct: floatFromEnv("VELOX_ALERT_ERROR_RATE_PCT", 5.0, 0),
		P95WallMS:    int64(intFromEnv("VELOX_ALERT_P95_WALL_MS", 300000, 0)),
		DiskFreeGB:   floatFromEnv("VELOX_ALERT_DISK_FREE_GB", 10.0, 0),
		FFmpegMin:    floatFromEnv("VELOX_ALERT_FFMPEG_MIN", 1.5, 0),
	}
}

func loadFleetConfig() FleetConfig {
	return FleetConfig{
		SmokeMode:          strings.ToLower(strings.TrimSpace(os.Getenv("VELOX_SMOKE_MODE"))),
		SmokeDriveFolderID: strings.TrimSpace(os.Getenv("VELOX_SMOKE_DRIVE_FOLDER_ID")),
	}
}

func loadCompatibilityConfig() CompatibilityConfig {
	aliases := []RetiredEnvAlias{
		{Env: "SOCIAL_GATEWAY_URL", Canonical: "SOCIAL_API_URL"},
		{Env: "SOCIAL_GATEWAY_API_KEY", Canonical: "SOCIAL_API_TOKEN"},
		{Env: "SOCIAL_GATEWAY_CALLBACK_BASE_URL", Canonical: "SOCIAL_CALLBACK_BASE_URL"},
	}
	configured := make([]RetiredEnvAlias, 0, len(aliases))
	for _, alias := range aliases {
		if strings.TrimSpace(os.Getenv(alias.Env)) != "" {
			configured = append(configured, alias)
		}
	}
	return CompatibilityConfig{RetiredSocialAliases: configured}
}

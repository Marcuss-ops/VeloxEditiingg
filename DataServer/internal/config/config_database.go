package config

import (
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// loadDatabaseConfig reads the VELOX_DB_* env vars into a DatabaseConfig.
//
// What is required depends on the Driver. With no VELOX_DB_DRIVER set,
// Driver is left empty so config.Validate() can either default it to
// "sqlite" (backward compat) or reject a config that mistakenly dropped
// it entirely. The legacy SQLite-only "DBPath always required" rule
// is gone — the platform/database abstraction is what consumes this
// struct.
//
// Env vars mapped here:
//
//   VELOX_DB_DRIVER          → Driver          (sqlite|postgres)
//   VELOX_DATABASE_URL       → URL             (postgres DSN)
//   VELOX_DB_PATH            → DBPath          (sqlite file path; absolute)
//   VELOX_DB_MAX_OPEN_CONNS  → MaxOpenConns    (int ≥ 0)
//   VELOX_DB_MAX_IDLE_CONNS  → MaxIdleConns    (int ≥ 0)
//   VELOX_DB_CONN_MAX_LIFETIME → ConnMaxLifetime (duration string)
//   VELOX_DB_MIGRATE_ON_START → MigrateOnStart (bool)
func loadDatabaseConfig() DatabaseConfig {
	driver := strings.ToLower(strings.TrimSpace(os.Getenv("VELOX_DB_DRIVER")))

	cfg := DatabaseConfig{
		Driver: driver,
		URL:    os.Getenv("VELOX_DATABASE_URL"),
	}

	// SQLitePath keeps the historical symlink-resolution behaviour for
	// absolute paths so existing deployments that bind-mount a symlink
	// at VELOX_DB_PATH continue to see the resolved target downstream.
	if raw := os.Getenv("VELOX_DB_PATH"); raw != "" {
		resolved := raw
		if filepath.IsAbs(raw) {
			if r, err := filepath.EvalSymlinks(raw); err == nil {
				log.Printf("config: database path resolved: %s -> %s", raw, r)
				resolved = r
			} else {
				log.Printf("config: cannot resolve symlinks for %s: %v (using original path)", raw, err)
			}
		}
		cfg.DBPath = resolved
	}

	cfg.MaxOpenConns = parseNonNegativeInt("VELOX_DB_MAX_OPEN_CONNS")
	cfg.MaxIdleConns = parseNonNegativeInt("VELOX_DB_MAX_IDLE_CONNS")
	cfg.ConnMaxLifetime = parseDurationConfig("VELOX_DB_CONN_MAX_LIFETIME")
	// MigrateOnStart is the boot-time schema bootstrap gate. The
	// "opt-out" framing in the user-facing docstring means: a
	// deployment that has not set VELOX_DB_MIGRATE_ON_START DEFAULTS
	// TO LEGACY BEHAVIOUR (master owns schema, runs migrations +
	// post-migration adjustments on boot). The ONLY way to skip
	// schema bootstrap at boot is to explicitly set the env var to
	// `false` (or `0` / `off` / `no`). This avoids silently breaking
	// existing deployments during upgrade — previous masters always
	// ran migrations on boot, and a default of "skip" would land
	// existing operators on a half-stale schema without warning.
	cfg.MigrateOnStart = parseBoolConfigDefaultTrue("VELOX_DB_MIGRATE_ON_START")

	return cfg
}

// parseNonNegativeInt reads a non-negative integer env var. Returns 0
// for unset, malformed, or negative values so the platform/database
// Open() applies driver defaults.
func parseNonNegativeInt(envName string) int {
	raw := os.Getenv(envName)
	if raw == "" {
		return 0
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 0 {
		return 0
	}
	return v
}

// parseDurationConfig reads a time.Duration env var via time.ParseDuration.
// Returns 0 for unset, malformed, or negative durations so Open() applies
// driver defaults.
func parseDurationConfig(envName string) time.Duration {
	raw := os.Getenv(envName)
	if raw == "" {
		return 0
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < 0 {
		return 0
	}
	return d
}

// parseBoolConfig reads a boolean env var. Returns false for unset or
// malformed values so the bootstrap path stays opt-in by default.
// Used for one-shot opt-in flags where unset → no is the safe default
// (e.g. insecure transport toggles, dev-only bypasses).
func parseBoolConfig(envName string) bool {
	raw := os.Getenv(envName)
	if raw == "" {
		return false
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return false
	}
	return v
}

// parseBoolConfigDefaultTrue reads a boolean env var with a REVERSED
// safe default: unset / empty / malformed → true, ONLY an explicit
// "false" (or "0" / "off" / "no") opts out. Used for the schema-bootstrap
// gate where the legacy behaviour is "do migrate at boot" and the
// forward-only posture is an explicit operator opt-out — flipping the
// default-on-upgrade would silently break existing deployments whose
// schema state advances only through the embedded runner.
//
// Sets TO TRUE on unset (unlike parseBoolConfig which defaults false)
// to preserve backward compatibility across the master upgrade that
// landed this field.
func parseBoolConfigDefaultTrue(envName string) bool {
	raw := os.Getenv(envName)
	if raw == "" {
		return true
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		// Unparseable treated as NOT-set → legacy default. A typo
		// like VELOX_DB_MIGRATE_ON_START=yesss falls back to the
		// master-owns-schema behaviour rather than silently
		// disabling migrations.
		return true
	}
	return v
}

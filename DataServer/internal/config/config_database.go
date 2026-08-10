package config

import (
	"log"
	"path/filepath"
	"strings"
)

// loadDatabaseConfig reads the VELOX_DB_* env vars into a DatabaseConfig.
//
// The master runtime accepts an empty driver (which database.Open defaults
// to SQLite) or an explicit "sqlite". "postgres" is loaded only so the
// configuration boundary can reject it before bootstrap I/O; isolated
// repository contract tests construct database.Config directly.
//
// Env vars mapped here:
//
//	VELOX_DB_DRIVER          → Driver          (empty|sqlite at runtime; postgres rejected)
//	VELOX_DATABASE_URL       → URL             (contract tests only)
//	VELOX_DB_PATH            → DBPath          (SQLite file path; absolute)
//	VELOX_DB_MAX_OPEN_CONNS  → MaxOpenConns    (int ≥ 0)
//	VELOX_DB_MAX_IDLE_CONNS  → MaxIdleConns    (int ≥ 0)
//	VELOX_DB_CONN_MAX_LIFETIME → ConnMaxLifetime (duration string)
//	VELOX_DB_MIGRATE_ON_START → MigrateOnStart (bool)
func loadDatabaseConfig(raw RawConfig) DatabaseConfig {
	driver := strings.ToLower(strings.TrimSpace(raw.Get("VELOX_DB_DRIVER")))

	cfg := DatabaseConfig{
		Driver: driver,
		URL:    raw.Get("VELOX_DATABASE_URL"),
	}

	// SQLitePath keeps the historical symlink-resolution behaviour for
	// absolute paths so existing deployments that bind-mount a symlink
	// at VELOX_DB_PATH continue to see the resolved target downstream.
	// A Postgres config is rejected by Config.Validate before database
	// opening, so do not touch the filesystem while constructing that
	// invalid configuration.
	if raw := raw.Get("VELOX_DB_PATH"); raw != "" && driver != "postgres" {
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

	cfg.MaxOpenConns = raw.Int("VELOX_DB_MAX_OPEN_CONNS", 0, 0)
	cfg.MaxIdleConns = raw.Int("VELOX_DB_MAX_IDLE_CONNS", 0, 0)
	cfg.ConnMaxLifetime = raw.Duration("VELOX_DB_CONN_MAX_LIFETIME", 0)
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
	cfg.MigrateOnStart = raw.Bool("VELOX_DB_MIGRATE_ON_START", true)

	return cfg
}

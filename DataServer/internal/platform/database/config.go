// Package database — config.go
//
// Backend-neutral configuration types + env-binding helper for the
// platform/database abstraction. The Driver + URL + SQLitePath + pool
// defaults are the smallest possible surface an Open() caller has to
// reason about; anything more specific (TLS, search_path, statement
// timeout) stays in the per-driver concrete wrapper.
//
// This package is the only place VELOX_DB_DRIVER / VELOX_DATABASE_URL /
// VELOX_DB_PATH are resolved. Callers that already supply a Config
// struct do not need to call LoadFromEnv; callers that want default
// env-var semantics call database.LoadFromEnv() and pass the result.
package database

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Driver identifies which SQL backend the Handle speaks.
// Empty (DriverUnknown) is interpreted as DriverSQLite to preserve
// backward compatibility with the pre-platform era where
// VELOX_DB_PATH always implied SQLite.
type Driver string

const (
	DriverSQLite   Driver = "sqlite"
	DriverPostgres Driver = "postgres"

	// DriverUnknown is the zero value. Treat it as DriverSQLite when an
	// env var is unset so older boots that only set VELOX_DB_PATH keep
	// working without an explicit VELOX_DB_DRIVER=sqlite declaration.
	DriverUnknown Driver = ""
)

// Config is the connection-pool + driver selection knobs that drive
// Open(). Fields with zero values fall back to driver-specific defaults
// applied inside Open(): SQLite gets a single-writer pool, Postgres gets
// 16 open / 4 idle / 5-minute lifetime. Callers may pin the values
// explicitly when the workload pattern is known.
type Config struct {
	// Driver selects the SQL backend. DriverUnknown is treated as
	// DriverSQLite so an empty env binding still works.
	Driver Driver
	// URL is the Postgres DSN (libpq keyword form OR postgres:// URL form).
	// Required when Driver == DriverPostgres. Ignored otherwise.
	URL string
	// SQLitePath is the path to the SQLite database file. Required when
	// Driver == DriverSQLite (or DriverUnknown, which falls back to SQLite).
	// Absolute paths are preferred; Open() will pass relative paths
	// through to the driver unchanged.
	SQLitePath string
	// MaxOpenConns caps the connection pool. Zero = driver-specific default.
	MaxOpenConns int
	// MaxIdleConns caps idle connections kept warm. Zero = driver-specific default.
	MaxIdleConns int
	// ConnMaxLifetime caps how long a pooled connection may live. Zero = default.
	ConnMaxLifetime time.Duration
}

// envDefaultDriver is the driver used when VELOX_DB_DRIVER is empty.
// SQLite remains the historical default so existing deployments that
// only set VELOX_DB_PATH keep booting without an env change.
const envDefaultDriver = DriverSQLite

// loadEnv reads the VELOX_DB_* env vars into a Config WITHOUT applying
// the SQLite-default fallback. Callers that want the fall-back behaviour
// use LoadFromEnv; callers that want strict pre-validation (e.g. the
// existing config package Validate() path) use loadEnv directly so they
// can reject empty Driver explicitly.
func loadEnv() Config {
	driverRaw := strings.ToLower(strings.TrimSpace(os.Getenv("VELOX_DB_DRIVER")))
	c := Config{
		Driver:     Driver(driverRaw),
		URL:        os.Getenv("VELOX_DATABASE_URL"),
		SQLitePath: os.Getenv("VELOX_DB_PATH"),
	}

	c.MaxOpenConns = parsePositiveInt("VELOX_DB_MAX_OPEN_CONNS")
	c.MaxIdleConns = parsePositiveInt("VELOX_DB_MAX_IDLE_CONNS")
	c.ConnMaxLifetime = parseDurationEnv("VELOX_DB_CONN_MAX_LIFETIME")

	return c
}

// LoadFromEnv returns a Config populated from VELOX_DB_* env vars.
// Driver falls back to DriverSQLite when VELOX_DB_DRIVER is unset, so
// historical deployments that only set VELOX_DB_PATH continue to work.
// The Config returned by LoadFromEnv is suitable for direct Open() use;
// callers that want stricter pre-validation should read env themselves.
func LoadFromEnv() Config {
	c := loadEnv()
	if c.Driver == DriverUnknown {
		c.Driver = envDefaultDriver
	}
	return c
}

// ValidateRejected here is a doc-only type documenting the validation
// errors Open() may surface. Callers can match ErrUnsupportedDriver
// and ErrDatabaseNotConfigured via errors.Is.
// parsePositiveInt reads a positive integer env var. Returns 0 when
// the var is unset, unparseable, or negative. Open() interprets 0 as
// "apply driver default".
func parsePositiveInt(envName string) int {
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

// parseDurationEnv reads a time.Duration env var using time.ParseDuration.
// Returns 0 for unset, malformed, or negative durations so Open() can
// apply driver defaults.
func parseDurationEnv(envName string) time.Duration {
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

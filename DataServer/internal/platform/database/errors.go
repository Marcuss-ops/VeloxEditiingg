// Package database — errors.go
//
// Centralised error sentinels for the platform/database abstraction.
// Callers should match via errors.Is(err, ErrXxx) so test assertions
// stay decoupled from the formatted message text. Non-sentinel errors
// (e.g. fmt.Errorf-wrapped driver errors with %w) preserve the original
// underlying error for inspection.
package database

import "errors"

// ErrUnsupportedDriver is returned by Open when cfg.Driver is not
// DriverSQLite or DriverPostgres. Pre-existing factories used to
// silently fall back to SQLite on a bad DSN; the platform layer
// fails loud here instead.
var ErrUnsupportedDriver = errors.New("database: unsupported driver")

// ErrDatabaseNotConfigured is returned by Open when cfg is missing the
// connection target the selected Driver requires:
//   - DriverSQLite / DriverUnknown: SQLitePath must be set
//   - DriverPostgres: URL must be set
//
// The message callers will see is wrapped with this sentinel so the
// driver context is preserved.
var ErrDatabaseNotConfigured = errors.New("database: not configured")

// ErrPingFailed wraps a public Ping failure (driver unreachable,
// auth rejected, wrong database name). The original error is preserved
// via %w so callers can match the underlying driver sentinel.
var ErrPingFailed = errors.New("database: ping")

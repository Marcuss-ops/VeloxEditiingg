// Package database — database.go
//
// Handle + Open: backend-neutral connection factory. SQLite + Postgres
// drivers are registered via blank-import side effects (sqlite3 from
// mattn/go-sqlite3, pgx from jackc/pgx/v5/stdlib). Pool defaults are
// driver-specific because SQLite serialises writers through the file
// lock while Postgres handles concurrent writers natively.
//
// Open does NOT run migrations. Callers that want implicit migration
// (historical NewSQLiteStore behaviour) must invoke migrations.Run /
// migrations.RunMigrations explicitly after Open returns. This
// separation keeps platform/database free of migration-side concerns
// and avoids import cycles (the migrations package imports the
// store-level interfaces, not the platform abstraction).
package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	// Register the drivers database/sql resolves by name.
	// pgx v5 stdlib is the canonical Postgres driver for Velox (chosen
	// over lib/pq due to upstream maintenance status).
	_ "github.com/jackc/pgx/v5/stdlib"
	// mattn/go-sqlite3 is the SQLite driver. CGO required at build time.
	_ "github.com/mattn/go-sqlite3"
)

// Driver-specific connection pool defaults. SQLite is intentionally
// pinned to a single-writer pool because concurrent opens on the same
// file degrade quickly under write contention. Postgres handles
// concurrent writers natively, so the defaults are larger and the
// caller is free to grow them when the workload proves it safe.
const (
	sqliteDefaultMaxOpenConns   = 1
	sqliteDefaultMaxIdleConns   = 1
	sqliteDefaultConnMaxLifetime = time.Hour

	postgresDefaultMaxOpenConns   = 16
	postgresDefaultMaxIdleConns   = 4
	postgresDefaultConnMaxLifetime = 5 * time.Minute

	openPingTimeout = 10 * time.Second
)

// Handle is the resource-owning struct returned by Open. Callers MUST
// call h.DB.Close() (or h.Close() if/when added) on shutdown to release
// the underlying connection pool. Handle is intentionally tiny — the
// *sql.DB surface is the application's primary entry point.
type Handle struct {
	DB     *sql.DB
	Driver Driver
}

// Open resolves the configured driver, applies driver-specific pool
// defaults (or keeps caller-supplied values), opens the connection,
// and verifies reachability via PingContext. It does NOT run migrations.
//
// Returns:
//   - ErrUnsupportedDriver: cfg.Driver is not SQLite or Postgres
//   - ErrDatabaseNotConfigured: target field empty for the chosen driver
//   - ErrPingFailed (wrapped): driver-level error from PingContext
//
// Ping may legitimately fail for unreachable databases; Open does not
// retry. The caller is expected to surface the error to operators
// rather than retry-with-backoff inside Open.
func Open(ctx context.Context, cfg Config) (*Handle, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	driver := cfg.Driver
	if driver == DriverUnknown {
		driver = envDefaultDriver
	}

	switch driver {
	case DriverSQLite:
		return openSQLite(ctx, cfg)
	case DriverPostgres:
		return openPostgres(ctx, cfg)
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedDriver, cfg.Driver)
	}
}

// sqliteDSNParams are the connection-init parameters appended to the
// SQLite DSN. They MUST be on the DSN itself rather than runtime
// `db.Exec` PRAGMAs because mattn/go-sqlite3 applies DSN params to
// every connection the pool spawns; runtime PRAGMAs only affect the
// single connection that ran them. Skipping them would mean
// non-primary pooled connections default to busy_timeout=0 (instant
// SQLITE_BUSY under concurrent writes) and to journal_mode=DELETE
// (no WAL durability). Both regression vectors are documented in
// the engine-history ticket filed when this was extracted from the
// legacy NewSQLiteStore hard-coded URL string.
const sqliteDSNParams = "_busy_timeout=5000&_journal_mode=WAL"

// ensureSQLiteDSN normalises a SQLitePath into a fully-qualified DSN
// with the standard connection-init parameters appended. Empty paths
// pass through unchanged so the upstream ErrDatabaseNotConfigured
// sentinel still fires for the empty case. Paths that already carry a
// `?...` query are merged with `&` rather than `?` so existing
// caller-supplied params survive.
func ensureSQLiteDSN(path string) string {
	if path == "" {
		return path
	}
	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}
	return path + sep + sqliteDSNParams
}

// openSQLite opens the SQLite file via the mattn driver and pings. The
// pool is pinned to one connection so concurrent app writers serialise
// cleanly through the storage engine. Tunable runtime PRAGMAs
// (synchronous, cache_size, mmap_size, foreign_keys, ...) are applied
// by the store layer post-init via db.Exec — see
// store.NewSQLiteStoreFromHandle for the rationale and per-connection
// caveats.
func openSQLite(ctx context.Context, cfg Config) (*Handle, error) {
	if cfg.SQLitePath == "" {
		return nil, fmt.Errorf("%w: SQLitePath is required for Driver=sqlite", ErrDatabaseNotConfigured)
	}
	dsn := ensureSQLiteDSN(cfg.SQLitePath)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("database sqlite open: %w", err)
	}
	applyPoolDefaults(db, cfg, sqliteDefaultMaxOpenConns, sqliteDefaultMaxIdleConns, sqliteDefaultConnMaxLifetime)

	pingCtx, cancel := context.WithTimeout(ctx, openPingTimeout)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("%w (sqlite): %w", ErrPingFailed, err)
	}
	return &Handle{DB: db, Driver: DriverSQLite}, nil
}

// openPostgres opens via the pgx v5 stdlib driver and pings. Caller
// pool overrides always win over defaults; defaults are conservative
// until benchmarks prove higher is safe.
func openPostgres(ctx context.Context, cfg Config) (*Handle, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("%w: URL is required for Driver=postgres", ErrDatabaseNotConfigured)
	}
	db, err := sql.Open("pgx", cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("database postgres open: %w", err)
	}
	applyPoolDefaults(db, cfg, postgresDefaultMaxOpenConns, postgresDefaultMaxIdleConns, postgresDefaultConnMaxLifetime)

	pingCtx, cancel := context.WithTimeout(ctx, openPingTimeout)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("%w (postgres): %w", ErrPingFailed, err)
	}
	return &Handle{DB: db, Driver: DriverPostgres}, nil
}

// applyPoolDefaults fills in driver-specific defaults when cfg leaves
// the pool knobs at zero. Caller-supplied positive values always win
// — Open never narrows a pool the operator widened explicitly.
func applyPoolDefaults(db *sql.DB, cfg Config, defOpen, defIdle int, defLifetime time.Duration) {
	maxOpen := cfg.MaxOpenConns
	if maxOpen <= 0 {
		maxOpen = defOpen
	}
	maxIdle := cfg.MaxIdleConns
	if maxIdle <= 0 {
		maxIdle = defIdle
	}
	lifetime := cfg.ConnMaxLifetime
	if lifetime <= 0 {
		lifetime = defLifetime
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(lifetime)
}

// Package database is the canonical database entry point for Velox.
// It abstracts SQLite and Postgres connection setup, pool tuning, and
// driver dispatch behind a single Open call consumed by the composition
// root (cmd/server/bootstrap.go) and test suites.
package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/mattn/go-sqlite3"
)

// Driver identifies the SQL backend.
type Driver string

const (
	DriverSQLite  Driver = "sqlite"
	DriverPostgres Driver = "postgres"
)

// Config holds connection parameters for both supported backends.
type Config struct {
	Driver          Driver
	SQLitePath      string
	URL             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

// Handle is the result of Open. Callers retain Close ownership.
type Handle struct {
	DB     *sql.DB
	Driver Driver
}

// Open connects to the requested backend using the supplied config.
//
// Driver dispatch rules:
//   - empty Driver → treated as DriverSQLite (backward compat)
//   - DriverSQLite → opens SQLite via mattn/go-sqlite3
//   - DriverPostgres → opens Postgres via jackc/pgx/v5
//
// Pool tuning defaults are conservative (1 open / 1 idle / 1h lifetime)
// when zero so that production does not accidentally run unbounded.
// Callers that want different defaults (e.g. sqliteStorePoolSize's
// 4/2/5m) must set the fields explicitly.
func Open(ctx context.Context, cfg Config) (*Handle, error) {
	driver := cfg.Driver
	if driver == "" {
		driver = DriverSQLite
	}

	switch driver {
	case DriverSQLite:
		return openSQLite(ctx, cfg)
	case DriverPostgres:
		return openPostgres(ctx, cfg)
	default:
		return nil, fmt.Errorf("database: unsupported driver %q", driver)
	}
}

func openSQLite(ctx context.Context, cfg Config) (*Handle, error) {
	if cfg.SQLitePath == "" {
		return nil, fmt.Errorf("database: SQLitePath is required for driver=sqlite")
	}

	// Connection-init PRAGMAs must live on the DSN so every pooled
	// connection inherits them. Runtime db.Exec PRAGMAs only affect
	// the single connection that ran them.
	dsn := cfg.SQLitePath
	if strings.Contains(dsn, "?") {
		dsn += "&_busy_timeout=5000&_journal_mode=WAL"
	} else {
		dsn += "?_busy_timeout=5000&_journal_mode=WAL"
	}

	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("database: open sqlite: %w", err)
	}

	applyPoolDefaults(db, cfg)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("database: ping sqlite: %w", err)
	}

	return &Handle{DB: db, Driver: DriverSQLite}, nil
}

func openPostgres(ctx context.Context, cfg Config) (*Handle, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("database: URL is required for driver=postgres")
	}

	db, err := sql.Open("pgx", cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("database: open postgres: %w", err)
	}

	applyPoolDefaults(db, cfg)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("database: ping postgres: %w", err)
	}

	return &Handle{DB: db, Driver: DriverPostgres}, nil
}

// applyPoolDefaults sets MaxOpenConns, MaxIdleConns and ConnMaxLifetime.
// Zero fields are replaced with conservative defaults so the pool does
// not silently become unbounded in production.
func applyPoolDefaults(db *sql.DB, cfg Config) {
	maxOpen := cfg.MaxOpenConns
	if maxOpen <= 0 {
		maxOpen = 1
	}
	maxIdle := cfg.MaxIdleConns
	if maxIdle <= 0 {
		maxIdle = 1
	}
	lifetime := cfg.ConnMaxLifetime
	if lifetime <= 0 {
		lifetime = time.Hour
	}

	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(lifetime)
}

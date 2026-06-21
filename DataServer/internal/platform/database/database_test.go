// Package database_test — runtime tests for the platform/database
// abstraction. SQLite-only here so the package compiles on any host
// (CI/Windows/Mac). Postgres open+ping belongs in an integration test
// gated by VELOX_TEST_DATABASE_URL — Phase 2 contracts already cover
// that path via the env-gated factories.
package database_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"velox-server/internal/platform/database"
)

// TestOpen_SQLiteRoundTrip opens a tempdir SQLite, pings, runs a create +
// insert + select round-trip to verify the connection is live.
func TestOpen_SQLiteRoundTrip(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "velox.db")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	h, err := database.Open(ctx, database.Config{
		Driver:     database.DriverSQLite,
		SQLitePath: dbPath,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = h.DB.Close() })

	if h.Driver != database.DriverSQLite {
		t.Fatalf("Driver mismatch: got %q want %q", h.Driver, database.DriverSQLite)
	}
	if err := h.DB.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}

	if _, err := h.DB.ExecContext(ctx, "CREATE TABLE roundtrip (id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := h.DB.ExecContext(ctx, "INSERT INTO roundtrip (v) VALUES (?)", "hello"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	var v string
	if err := h.DB.QueryRowContext(ctx, "SELECT v FROM roundtrip WHERE id = 1").Scan(&v); err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if v != "hello" {
		t.Fatalf("v mismatch: got %q", v)
	}
}

// TestOpen_SQLiteEmptyPath_Rejected verifies that an empty SQLitePath
// surfaces ErrDatabaseNotConfigured rather than silently succeeding.
func TestOpen_SQLiteEmptyPath_Rejected(t *testing.T) {
	_, err := database.Open(context.Background(), database.Config{
		Driver: database.DriverSQLite,
		// SQLitePath intentionally omitted.
	})
	if err == nil {
		t.Fatal("expected error for empty SQLitePath, got nil")
	}
	if !errorContains(err, "SQLitePath is required") {
		t.Fatalf("error message must mention SQLitePath: %v", err)
	}
}

// TestOpen_PostgresEmptyURL_Rejected verifies that an empty URL for
// DriverPostgres surfaces ErrDatabaseNotConfigured.
func TestOpen_PostgresEmptyURL_Rejected(t *testing.T) {
	_, err := database.Open(context.Background(), database.Config{
		Driver: database.DriverPostgres,
		// URL intentionally omitted.
	})
	if err == nil {
		t.Fatal("expected error for empty URL, got nil")
	}
	if !errorContains(err, "URL is required") {
		t.Fatalf("error message must mention URL: %v", err)
	}
}

// TestOpen_UnsupportedDriver_Rejected verifies that an unknown driver
// name surfaces ErrUnsupportedDriver rather than silently defaulting to
// SQLite (the historical buggy behaviour of legacy Postgres store
// helpers that did not validate the driver name).
func TestOpen_UnsupportedDriver_Rejected(t *testing.T) {
	_, err := database.Open(context.Background(), database.Config{
		Driver: "mysql",
	})
	if err == nil {
		t.Fatal("expected error for unsupported driver, got nil")
	}
	if !errorContains(err, "unsupported driver") {
		t.Fatalf("error message must mention unsupported: %v", err)
	}
}

// TestLoadFromEnv_DefaultsToSQLite verifies that with no VELOX_DB_DRIVER
// set, LoadFromEnv returns DriverSQLite for backward compatibility.
func TestLoadFromEnv_DefaultsToSQLite(t *testing.T) {
	t.Setenv("VELOX_DB_DRIVER", "")
	t.Setenv("VELOX_DB_PATH", "")
	t.Setenv("VELOX_DATABASE_URL", "")

	cfg := database.LoadFromEnv()
	if cfg.Driver != database.DriverSQLite {
		t.Fatalf("default Driver must be sqlite, got %q", cfg.Driver)
	}
}

// TestLoadFromEnv_ReadsPostgres verifies that VELOX_DB_DRIVER=postgres
// + VELOX_DATABASE_URL get propagated into the returned Config verbatim.
func TestLoadFromEnv_ReadsPostgres(t *testing.T) {
	const wantURL = "postgres://u:p@host:5432/db?sslmode=disable"
	t.Setenv("VELOX_DB_DRIVER", "postgres")
	t.Setenv("VELOX_DATABASE_URL", wantURL)
	t.Setenv("VELOX_DB_PATH", "/should/be/ignored")
	t.Setenv("VELOX_DB_MAX_OPEN_CONNS", "32")
	t.Setenv("VELOX_DB_MAX_IDLE_CONNS", "8")
	t.Setenv("VELOX_DB_CONN_MAX_LIFETIME", "30s")

	cfg := database.LoadFromEnv()
	if cfg.Driver != database.DriverPostgres {
		t.Fatalf("Driver mismatch: got %q", cfg.Driver)
	}
	if cfg.URL != wantURL {
		t.Fatalf("URL mismatch: got %q want %q", cfg.URL, wantURL)
	}
	if cfg.MaxOpenConns != 32 {
		t.Fatalf("MaxOpenConns mismatch: got %d", cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns != 8 {
		t.Fatalf("MaxIdleConns mismatch: got %d", cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime != 30*time.Second {
		t.Fatalf("ConnMaxLifetime mismatch: got %v", cfg.ConnMaxLifetime)
	}
}

// TestExecutorInterfaceBackendNeutral verifies that *sql.DB satisfies
// platform/database.Executor without an adapter. This is a compile-time
// guarantee exercised at runtime via var assignment.
func TestExecutorInterfaceBackendNeutral(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "velox.db")
	h, err := database.Open(context.Background(), database.Config{
		Driver:     database.DriverSQLite,
		SQLitePath: dbPath,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = h.DB.Close() })

	// Compile-time assertion — if *sql.DB stopped satisfying Executor,
	// this line would fail to build.
	var _ database.Executor = h.DB

	// Runtime sanity that the interface methods actually work through
	// the interface (catches e.g. accidental shadowing).
	var ex database.Executor = h.DB
	if _, err := ex.ExecContext(context.Background(), "SELECT 1"); err != nil {
		t.Fatalf("ExecContext via Executor: %v", err)
	}
}

// errorContains is a tiny helper for substring assertions on error
// messages so the test body stays readable. Strings.Contains would do
// but the named helper makes intent obvious.
func errorContains(err error, want string) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), want)
}

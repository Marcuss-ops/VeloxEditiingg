// Package database_test — runtime tests for the platform/database
// abstraction. SQLite-only here so the package compiles on any host
// (CI/Windows/Mac). The Postgres backend was removed (2026-08-10);
// SQLite is the only supported runtime driver.
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

// TestOpen_UnsupportedDriver_Rejected verifies that an unknown driver
// name surfaces ErrUnsupportedDriver rather than silently defaulting to
// SQLite (the historical buggy behaviour of legacy store helpers that
// did not validate the driver name).
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

// TestConfigFromApplication_DefaultsToTypedValues verifies that with no
// VELOX_DB_DRIVER set, ConfigFromApplication leaves the driver unknown
// until Open defaults it to SQLite.
func TestConfigFromApplication_DefaultsToTypedValues(t *testing.T) {
	cfg := database.ConfigFromApplication("", "/tmp/velox.db", 0, 0, 0)
	if cfg.Driver != database.DriverUnknown {
		t.Fatalf("empty application driver must remain unknown until Open, got %q", cfg.Driver)
	}
	if cfg.SQLitePath != "/tmp/velox.db" {
		t.Fatalf("SQLitePath = %q", cfg.SQLitePath)
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

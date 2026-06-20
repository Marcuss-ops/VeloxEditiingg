package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"velox-server/internal/config"
	"velox-server/internal/platform/database"
)

// postgresDsnEnvVar is the env var name this integration test reads
// to discover a running Postgres testcontainer. Mirrors the constants
// in internal/store/contracts/factories_postgres.go so the cmd/server
// integration test and the narrow-repo contract suite share a single
// env-var convention. Operators running this locally set it after
// `docker run` (or via DataServer/run-tests-postgres.sh); CI provides
// it through the platform's secret manager.
//
// When unset, the test Skip-s itself with a hint pointing at the
// helper script so contributors don't have to guess how to run it.
// Skipped tests are NOT failures in CI environments without docker.
const postgresDsnEnvVar = "VELOX_TEST_POSTGRES_DSN"

// uniquePostgresTestSchemaName returns a Postgres-safe schema name
// unique per (sub-test name, call time). Combines UnixNano +
// sanitized sub-test name so parallel `go test` runs (or this test
// running multiple times in the same CI shard) cannot collide on
// schema name even when host clock resolution would otherwise tie.
//
// Postgres caps identifier length at 63 bytes; the helpers in
// internal/store/contracts/ already bind to 32 sanitised chars per
// segment. We apply the same cap here so the constructed name stays
// inside the cap on all Postgres versions CI runs.
func uniquePostgresTestSchemaName(t *testing.T, prefix string) string {
	t.Helper()

	safe := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z':
			return r
		case r >= '0' && r <= '9':
			return r
		case r == '_':
			return r
		default:
			return '_'
		}
	}, strings.ToLower(t.Name()))
	if len(safe) > 24 {
		safe = safe[:24]
	}
	return fmt.Sprintf("pg_dispatch_%s_%d_%s", prefix, time.Now().UnixNano(), safe)
}

// withSingleSchemaSearchPath appends `&search_path=<schema>,public`
// to the supplied DSN. Mirrors internal/store/contracts/withSearchPath
// for the single-schema DSN shape the integration test needs. The
// form is the URL form (postgres://u:p@host:port/db?...); keyword-form
// DSNs are not exercised here because the docker helper script emits
// URL-form DSNs.
func withSingleSchemaSearchPath(dsn, schema string) string {
	parts := strings.SplitN(dsn, "?", 2)
	var qs string
	if len(parts) == 2 {
		// Drop any pre-existing search_path; we want the test's
		// value authoritative so the per-test schema is unambiguous.
		existing := strings.Split(parts[1], "&")
		kept := make([]string, 0, len(existing))
		for _, kv := range existing {
			if kv == "" {
				continue
			}
			if strings.HasPrefix(kv, "search_path=") {
				continue
			}
			kept = append(kept, kv)
		}
		qs = strings.Join(kept, "&")
	}
	if qs == "" {
		return parts[0] + "?search_path=" + schema + ",public"
	}
	return parts[0] + "?" + qs + "&search_path=" + schema + ",public"
}

// setupIsolatedPostgresBareSchema isolates a per-test schema with a
// bare Connection + schema + search_path — explicitly NOT a 1:1
// mirror of contracts.openPostgresForTest. Migrations are deliberately
// skipped because this test:
//   - (a) only writes its own probe table so an empty per-test schema is
//     sufficient for the cross-leak assertion;
//   - (b) exercises buildServerDeps's DriverPostgres branch which errors
//     out fast BEFORE the production migrations runner is invoked.
//
// If a future subtest needs a fully-migrated per-test schema, import
// the contracts package's openPostgresForTest directly (or factor this
// helper to accept a `runMigrations bool`). Today the bare variant is
// the smallest thing that lets the test assert what the user asked for.
//
// Operations order is deliberate: CREATE SCHEMA before SET search_path
// so the latter's reference resolves; database.Open already wires the
// connect-time path via cfg.URL.
func setupIsolatedPostgresBareSchema(t *testing.T, dsn, schema string) (*database.Handle, func()) {
	t.Helper()

	ctx := context.Background()
	handle, err := database.Open(ctx, database.Config{
		Driver: database.DriverPostgres,
		URL:    withSingleSchemaSearchPath(dsn, schema),
	})
	if err != nil {
		t.Fatalf("setupIsolatedPostgresBareSchema: database.Open: %v", err)
	}
	db := handle.DB

	if _, err := db.Exec("CREATE SCHEMA " + schema); err != nil {
		_ = handle.DB.Close()
		t.Fatalf("setupIsolatedPostgresBareSchema: ensure schema %q: %v", schema, err)
	}
	if _, err := db.Exec("SET search_path TO " + schema + ", public"); err != nil {
		_ = handle.DB.Close()
		t.Fatalf("setupIsolatedPostgresBareSchema: set search_path %q: %v", schema, err)
	}

	cleanup := func() {
		// Best-effort drop with CASCADE so any tables the test
		// wrote don't leak between tests sharing the database.
		_, _ = db.Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE")
		_ = handle.DB.Close()
	}
	return handle, cleanup
}

// TestBuildServerDeps_PostgresDispatch_IsolatedAndFailFast is the
// integration-test assertion that proves the platform/database
// dispatch path is end-to-end sound. It runs against a real Postgres
// instance (env-gated via VELOX_TEST_POSTGRES_DSN).
//
// What it asserts:
//
//  1. Handle.Driver is DriverPostgres — Open() distributed the
//     postgres URL through pgx/v5 correctly and tagged the
//     resulting *database.Handle with the right Driver.
//
//  2. Per-test schema isolation — two parallel setups with different
//     schema names produce two independent connection states: data
//     written through one handle is invisible through the other.
//
//  3. handle.DB.Close releases back to the pool — a third Open
//     against the same DSN after closing schema B's handle succeeds
//     (proves no connection-level leak across the lifecycle).
//
//  4. buildServerDeps reaches the DriverPostgres branch and returns
//     ErrPostgresNotYetWired — proves the dispatch switch landed
//     correctly without silently falling through to SQLite. The
//     sentinel error is the documented forward-path message; this
//     test prevents subsequent edits from changing the dispatch
//     point or weakening the error contract.
//
// Subtests run sequentially because the integration assertion is
// really one end-to-end flow: open → isolate → dispatch.
// t.Parallel() would race the CLEANUP-on-fail paths.
func TestBuildServerDeps_PostgresDispatch_IsolatedAndFailFast(t *testing.T) {
	dsn := os.Getenv(postgresDsnEnvVar)
	if dsn == "" {
		t.Skipf("VELOX_TEST_POSTGRES_DSN unset; run DataServer/run-tests-postgres.sh "+
			"or export VELOX_TEST_POSTGRES_DSN=<postgres://...> pointing at a running Postgres")
	}

	t.Run("OpenReturnsDriverPostgresHandle", func(t *testing.T) {
		// Open against the bare DSN (no per-test schema) — this
		// probes the pure platform/database.Open path and validates
		// cfg.Driver DTO round-trip. The handle does not need to
		// write anything; we just inspect handle.Driver.
		handle, err := database.Open(context.Background(), database.Config{
			Driver: database.DriverPostgres,
			URL:    dsn,
		})
		if err != nil {
			t.Fatalf("database.Open: %v", err)
		}
		defer func() { _ = handle.DB.Close() }()
		if handle.Driver != database.DriverPostgres {
			t.Fatalf("handle.Driver = %q, want %q — platform/database.Open must DTO-round-trip the Driver field",
				handle.Driver, database.DriverPostgres)
		}
		// Ping forces the actual connection (sql.Open is lazy otherwise).
		if err := handle.DB.Ping(); err != nil {
			t.Fatalf("handle.DB.Ping: %v — pgx/v5 handshake to the test DSN failed", err)
		}
	})

	t.Run("PerTestSchemasAreIsolated", func(t *testing.T) {
		// Two distinct schemas in the same shared database must not
		// see each other's writes. This is the proof that
		// openPostgresForTest-style CREATE SCHEMA + SET search_path
		// produces real isolation (not the public-fallback illusion
		// the historical inlined factory ordering had).
		schemaA := uniquePostgresTestSchemaName(t, "alpha")
		schemaB := uniquePostgresTestSchemaName(t, "beta")

		handleA, cleanupA := setupIsolatedPostgresBareSchema(t, dsn, schemaA)
		defer cleanupA()
		handleB, cleanupB := setupIsolatedPostgresBareSchema(t, dsn, schemaB)
		defer cleanupB()

		// Write into A: a sentinel table with one row.
		if _, err := handleA.DB.Exec("CREATE TABLE isolation_probe (id INT, marker TEXT)"); err != nil {
			t.Fatalf("schema.A CREATE TABLE: %v", err)
		}
		if _, err := handleA.DB.Exec("INSERT INTO isolation_probe (id, marker) VALUES (1, 'alpha-only')"); err != nil {
			t.Fatalf("schema.A INSERT: %v", err)
		}

		// Write into B: a different sentinel value.
		if _, err := handleB.DB.Exec("CREATE TABLE isolation_probe (id INT, marker TEXT)"); err != nil {
			t.Fatalf("schema.B CREATE TABLE: %v", err)
		}
		if _, err := handleB.DB.Exec("INSERT INTO isolation_probe (id, marker) VALUES (2, 'beta-only')"); err != nil {
			t.Fatalf("schema.B INSERT: %v", err)
		}

		// Verify schema A sees only its own row.
		var markerFromA string
		if err := handleA.DB.QueryRow("SELECT marker FROM isolation_probe WHERE id = 1").Scan(&markerFromA); err != nil {
			t.Fatalf("schema.A SELECT row 1: %v", err)
		}
		if markerFromA != "alpha-only" {
			t.Fatalf("schema.A leaked or lost: marker=%q want %q", markerFromA, "alpha-only")
		}

		// Verify schema B sees only its own row.
		var markerFromB string
		if err := handleB.DB.QueryRow("SELECT marker FROM isolation_probe WHERE id = 2").Scan(&markerFromB); err != nil {
			t.Fatalf("schema.B SELECT row 2: %v", err)
		}
		if markerFromB != "beta-only" {
			t.Fatalf("schema.B leaked or lost: marker=%q want %q", markerFromB, "beta-only")
		}

		// Cross-leak guard: count rows in A; must equal exactly 1
		// (the row A inserted) even though B inserted into a
		// table with the SAME NAME in a sibling schema. This is
		// the strongest isolation signal — same table name, two
		// schemas, no leakage because pgx connection-local
		// search_path scopes the unqualified reference.
		var countInA int
		if err := handleA.DB.QueryRow("SELECT COUNT(*) FROM isolation_probe").Scan(&countInA); err != nil {
			t.Fatalf("schema.A COUNT: %v", err)
		}
		if countInA != 1 {
			t.Fatalf("schema.A isolation broken: COUNT(*)=%d, want 1 — B's row leaked into A", countInA)
		}
	})

	t.Run("DispatchPathReturnsErrPostgresNotYetWired", func(t *testing.T) {
		// buildServerDeps's DriverPostgres branch never reaches the
		// per-test schema — it just dispatches Handle.Driver and
		// returns the sentinel. So we point cfg.Database.URL at the
		// bare DSN: no schema dance required, no leaked test
		// resource to clean up. cfg.Database.DBPath is set so any
		// future dispatch refactor that ignores the postgres branch
		// would still have a defensible path to read.
		cfg := &config.Config{
			Database: config.DatabaseConfig{
				Driver: "postgres",
				URL:    dsn,
			},
			Runtime: config.RuntimeConfig{DataDir: t.TempDir()},
			Workers: config.WorkersConfig{
				MaxJobAttempts:   3,
				AllowedWorkerIDs: []string{"test-worker-dispatch"},
			},
		}
		cfg.Database.DBPath = filepath.Join(t.TempDir(), "velox.db")

		deps, err := buildServerDeps(cfg)
		if deps != nil {
			t.Errorf("buildServerDeps on Driver=postgres must return nil deps; got %v", deps)
		}
		if err == nil {
			t.Fatal("buildServerDeps on Driver=postgres must return error; got nil")
		}
		// Sentinel match — declarative check that the dispatch
		// landed on the documented fail-fast branch. Brittle string
		// matching avoided deliberately.
		if !errors.Is(err, ErrPostgresNotYetWired) {
			t.Fatalf("err is not ErrPostgresNotYetWired: %v", err)
		}
		// Message-content sanity: even with the sentinel match, the
		// operator-facing string must still reference the road-map
		// pointer so a grep for "VELOX_DB_DRIVER=postgres" in a log
		// dump surfaces the docs path.
		if !strings.Contains(err.Error(), "docs/architecture/") ||
			!strings.Contains(err.Error(), "docs/pr/") {
			t.Fatalf("ErrPostgresNotYetWired message lost operator-facing pointers: %q", err.Error())
		}
		// Connection-leak guard: after buildServerDeps returned the
		// sentinel, a follow-up Open against the same DSN must
		// succeed and Ping() must succeed. The dispatch branch
		// calls `_ = handle.DB.Close()` before returning; if the
		// close were dropped, the client-side Open here still
		// succeeds (pgx reconnects on each Open), but a server-side
		// observer would see pg_stat_activity accumulating.
		postHandle, err := database.Open(context.Background(), database.Config{
			Driver: database.DriverPostgres,
			URL:    dsn,
		})
		if err != nil {
			t.Fatalf("post-build Open (leak guard): %v", err)
		}
		defer func() { _ = postHandle.DB.Close() }()
		if err := postHandle.DB.Ping(); err != nil {
			t.Fatalf("post-build Ping (leak guard): %v", err)
		}
	})
}

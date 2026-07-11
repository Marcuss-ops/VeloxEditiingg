// Package contracts / factories_postgres.go
//
// Postgres-backed factory for the ArtifactRepository contract suite.
// Signature matches the pre-existing ArtifactRepositoryFactory declared
// in artifact_repository_contract_test.go:
//
//	type ArtifactRepositoryFactory func(t *testing.T) (store.ArtifactRepository, func())
//
// Env-var gating (VELOX_TEST_POSTGRES_DSN) lives in the test driver, not
// here — keeps the factory pure so it composes cleanly with the existing
// ArtifactRepositoryContract runner.
//
// Each test gets its own ephemeral Postgres *schema* (dropped via the
// returned cleanup func) so tests are isolated even when sharing a
// database. Schema-per-test is cheaper than db-per-test and avoids
// Postgres' create-database privilege trap. Schema name combines
// `time.Now().UnixNano()` + sanitized t.Name() so parallel runs cannot
// collide on the same nano.
//
// The factory delegates bootstrap (Open + CREATE SCHEMA + Migrate +
// SET search_path) to openPostgresForTest so the artifact and jobs
// factories stay in lockstep on the schema-isolation order. The
// Repo's constructor takes a *database.Handle directly — the previous
// *PostgresStore wrapper was dropped because platform/database now
// owns connection lifecycle end-to-end.
package contracts

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"velox-server/internal/store"
)

// PostgresDsnEnvVar is the env var name the Postgres test driver reads.
// Kept here as a single source of truth so the contract suite and any
// future ad-hoc Postgres tests stay in lockstep.
const PostgresDsnEnvVar = "VELOX_TEST_POSTGRES_DSN"

// NewPostgresArtifactRepositoryFactory opens a fresh Postgres connection
// through platform/database.Open against an isolated per-test schema,
// runs the embedded Postgres migrations, and returns the adapter +
// cleanup func.
//
// Caller is responsible for checking PostgresDsnEnvVar first and
// t.Skipf'ing when it is unset — see artifact_repository_postgres_contract_test.go.
func NewPostgresArtifactRepositoryFactory(t *testing.T) (store.ArtifactRepository, func()) {
	t.Helper()

	dsn := os.Getenv(PostgresDsnEnvVar)
	schema := uniqueSchemaName(t)
	handle, cleanup := openPostgresForTest(t, withSearchPath(dsn, schema), schema)

	return store.NewPostgresArtifactRepository(handle), cleanup
}

// pgSchemaSafeRune is the per-rune mapping used to normalise Go test
// names into Postgres-safe identifier characters (lowercase ASCII
// letters, digits, underscore; everything else collapses to '_').
// Inlined via strings.Map inside uniqueSchemaName so we don't keep
// a closure on the package init graph.
var pgSchemaSafeRune = func(r rune) rune {
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
}

// uniqueSchemaName returns a Postgres-safe schema name unique per test.
// Combines UnixNano + lowercase-[a-z0-9_]-sanitized t.Name() so parallel
// `go test` runs never share a schema even when the host clock resolution
// would otherwise collide. The suffix is bounded (~80 chars max) because
// Postgres caps identifier length at 63 bytes — the sanitizer keeps the
// result portable across all major versions.
func uniqueSchemaName(t *testing.T) string {
	t.Helper()
	safe := strings.Map(pgSchemaSafeRune, t.Name())
	if len(safe) > 32 {
		safe = safe[:32]
	}
	return fmt.Sprintf("pg_artifacts_%d_%s", time.Now().UnixNano(), safe)
}

// withSearchPath appends (or merges) the pgx/libpq option `search_path=`
// onto the given DSN. Handles both DSN forms:
//
//   - URL form:    postgres://u:p@host:port/db?...  → append &search_path=foo
//   - Keyword form: host=... user=...    → append keyword search_path=foo
//
// Idempotent: overrides any existing search_path. Special-cases the
// "trailing ? with empty query" form (postgres://host/db?) so the result
// is `?...?search_path=foo`, never `?...?&search_path=foo`.
func withSearchPath(dsn, schema string) string {
	if strings.Contains(dsn, "?") {
		parts := strings.SplitN(dsn, "?", 2)
		qs := stripQueryParam(parts[1], "search_path")
		if qs == "" {
			return parts[0] + "?search_path=" + schema
		}
		return parts[0] + "?" + qs + "&search_path=" + schema
	}
	stripped := stripKeywordParam(dsn, "search_path")
	return strings.TrimRight(stripped, " ") + " search_path=" + schema
}

func stripQueryParam(qs, key string) string {
	out := []string{}
	for _, kv := range strings.Split(qs, "&") {
		if kv == "" {
			continue
		}
		if strings.HasPrefix(kv, key+"=") {
			continue
		}
		out = append(out, kv)
	}
	return strings.Join(out, "&")
}

func stripKeywordParam(dsn, key string) string {
	out := []string{}
	for _, kv := range strings.Fields(dsn) {
		if strings.HasPrefix(kv, key+"=") {
			continue
		}
		out = append(out, kv)
	}
	return strings.Join(out, " ")
}

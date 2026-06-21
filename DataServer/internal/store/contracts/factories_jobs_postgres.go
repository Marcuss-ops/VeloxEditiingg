// Package contracts / factories_jobs_postgres.go
//
// Postgres factory for the jobs.Repository narrow interface (jobs.Reader +
// jobs.Writer combined). Mirrors factories_postgres.go (artifact factory)
// pattern: env-gated, per-test schema isolation, returns the interface type
// instead of a concrete struct so the cross-backend contract suite composes
// identically with the SQLite factory.
//
// The factory delegates bootstrap (Open + CREATE SCHEMA + Migrate +
// SET search_path) to openPostgresForTest so the artifact and jobs
// factories stay in lockstep on the schema-isolation order. The
// Repo's constructor takes a *database.Handle directly — the previous
// *PostgresStore wrapper was dropped because platform/database now
// owns connection lifecycle end-to-end.
package contracts

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"velox-server/internal/jobs"
	"velox-server/internal/store"
)

// NewPostgresJobRepositoryFactory returns a Postgres-backed jobs.Repository
// bound to a per-test schema. Env-var gating is in the test driver; here we
// just open the connection, run migrations, and return the contract.
func NewPostgresJobRepositoryFactory(t *testing.T) (jobs.Repository, func()) {
	t.Helper()

	dsn := os.Getenv(PostgresDsnEnvVar)
	schema := uniqueJobsSchemaName(t)
	handle, cleanup := openPostgresForTest(t, withSearchPath(dsn, schema), schema)

	return store.NewPostgresJobRepository(handle), cleanup
}

// uniqueJobsSchemaName mirrors uniqueSchemaName (artifact factory) but
// uses a `pg_jobs_` prefix so a future combined Postgres test running
// artifact + jobs factories cannot collide on schema name. Base-36 keeps
// the identifier short without exceeding Postgres' 63-byte identifier cap.
func uniqueJobsSchemaName(t *testing.T) string {
	t.Helper()
	safe := strings.Map(pgSchemaSafeRune, t.Name())
	if len(safe) > 32 {
		safe = safe[:32]
	}
	return "pg_jobs_" + strconv.FormatInt(time.Now().UnixNano(), 36) + "_" + safe
}

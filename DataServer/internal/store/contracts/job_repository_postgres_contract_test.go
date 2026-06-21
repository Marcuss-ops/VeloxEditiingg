// Package contracts / job_repository_postgres_contract_test.go
//
// Postgres driver of the cross-backend JobRepositoryContract suite. Same
// pattern as artifact_repository_postgres_contract_test.go: env-gated skip
// when VELOX_TEST_POSTGRES_DSN is unset, drives the suite against a
// Postgres-backed factory.
//
// Note: the PRE-EXISTING JobRepositoryContract_SQLite test in
// jobs_repository_contract_test.go is intentionally untouched — it uses
// concrete *store.SQLiteJobRepository methods (CreateJob / GetJob /
// ListByStatus / Transition) and the SQLite job_history / job_events
// audit rows that are outside the narrow jobs.Writer contract. The
// cross-backend contract here targets jobs.Repository (narrow) and
// proves backend parity for those 17 methods only — internal audit
// tables remain SQLite-only until a late Phase 2 port.
package contracts

import (
	"os"
	"testing"
)

// TestJobRepositoryContract_Postgres drives the new
// JobRepositoryContract suite (this file's companion) against the
// Postgres-backed factory. Skips when VELOX_TEST_POSTGRES_DSN is unset.
func TestJobRepositoryContract_Postgres(t *testing.T) {
	if os.Getenv(PostgresDsnEnvVar) == "" {
		t.Skipf("Postgres contract tests skipped: set %s to enable", PostgresDsnEnvVar)
	}
	JobRepositoryContractCrossBackend(t, NewPostgresJobRepositoryFactory)
}

// Package contracts / artifact_repository_postgres_contract_test.go
//
// Postgres driver of the cross-backend ArtifactRepositoryContract suite.
// Same factory signature as the SQLite version so the suite runner does
// not know which backend it is driving.
//
// Env gate: VELOX_TEST_POSTGRES_DSN must be set (to a libpq URL OR
// keyword DSN that has privilege to CREATE SCHEMA) for the Postgres
// subtest to run. Otherwise this test skips cleanly so default CI on
// machines without a Postgres available does not fail.
//
// One dedicated file keeps the env-var reach test-isolated: any future
// narrow-interface Postgres driver (jobs, workflow, etc.) follows the
// same pattern in its own *postgres*_contract_test.go.
package contracts

import (
	"os"
	"testing"
)

// TestArtifactRepositoryContract_Postgres drives the pre-existing
// ArtifactRepositoryContract suite (SQLite-tested today, shared contract)
// against NewPostgresArtifactRepositoryFactory.
func TestArtifactRepositoryContract_Postgres(t *testing.T) {
	if os.Getenv(PostgresDsnEnvVar) == "" {
		t.Skipf("Postgres contract tests skipped: set %s to enable", PostgresDsnEnvVar)
	}
	ArtifactRepositoryContract(t, NewPostgresArtifactRepositoryFactory)
}

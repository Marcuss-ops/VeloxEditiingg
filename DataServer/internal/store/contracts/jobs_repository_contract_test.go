// Package contracts / jobs_repository_contract_test.go (PR-REMOVE-JOB-LEASE-OPS stub)
//
// This file previously drove a SQLite-bounded contract suite that
// exercised ClaimNext / ClaimNextForProfile on a concrete
// *store.SQLiteJobRepository. Both methods were removed in
// fix/remove-job-lease-ops (claim semantics live in taskgraph.Repository
// after the canonical-attempt-identity cutover). The companion
// job_repository_contract_test.go (also in this package) preserves
// every test that survived the cutover; the SQL-bound fixtures here
// are deferred to the PR15.x jobs-writer cleanup that reintroduces
// any SQLite-specific coverage on the canonical surface.
//
// This stub keeps Package Name + package-level helpers so future test
// constraints that import this file by name still compile, but emits
// only a single Skip so the build stays green.
package contracts

import "testing"

// TestJobRepositoryContract_SQLiteBoundClaimsSkipped documents the
// removal. The real coverage migrated to:
//
//   - contract_test.go::JobRepositoryContractCrossBackend (canonical
//     jobs.Repository interface, both SQLite + Postgres).
//   - taskgraph/repository_test.go (canonical claim via
//     ClaimNextWithAttemptAtomic).
func TestJobRepositoryContract_SQLiteBoundClaimsSkipped(t *testing.T) {
	t.Skip("fix/remove-job-lease-ops: ClaimNext/ClaimNextForProfile " +
		"removed from SQLiteJobRepository. SQLite-bound contract suite " +
		"deferred to the PR15.x jobs-writer cleanup. See " +
		"job_repository_contract_test.go for the cross-backend canonical surface.")
}

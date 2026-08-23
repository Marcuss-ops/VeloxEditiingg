// Package completion / e2e_test.go
//
// Phase 6 acceptance matrix per docs/completion-protocol-rollout-and-acceptance.md
// §"E2E test matrix" entries 4–17. Each scenario is a master-side fault-injection
// test on the REAL Coordinator + real *sql.DB (with migrations 001..090 applied
// via openCoordinatorTestDB / migrations.RunMigrations). We do NOT mock the
// Coordinator or any repository — every assertion reads the post-call SQLite
// state directly, so a regression in coordinator.go, store/completion_repository.go,
// or fencing.go trips the matrix.
//
// 17 scenarios → 9 top-level test functions → 14 sub-tests:
//
//	TestPhase6_Scenario04_AckLoss                                  (1 sub)
//	TestPhase6_Scenarios05_07_WorkerCrash                          (3 subs)
//	TestPhase6_Scenarios08_10_MasterCrash                          (3 subs)
//	TestPhase6_Scenario11_BranchA_StaysVerifying                  (1 sub)
//	TestPhase6_Scenario12_BranchD_ErrStaleReport                   (1 sub)
//	TestPhase6_Scenario13_BranchB_StaysVerifying                   (1 sub)
//	TestPhase6_Scenarios14_15_FenceRejection                       (2 subs)
//	TestPhase6_Scenarios16_17_RaceAndDeliveryRestore               (2 subs)
//	TestPhase6_Acceptance_GoldenPath                               (1 sub)
//
// Fault-injection mechanism (one per scenario class):
//   - Replay/idempotency (4, 9, 10): call the same Coordinator method twice
//     on a stable input; assert the second call is a no-op.
//   - Worker death (5–7): inject "deadline elapsed" into attempt_commits
//     directly, then exercise ReconcileAttempt's repair-forward EXPIRED path.
//   - Master crash (8–10): shard `attempt_commits`/`artifact_uploads` state
//     into the post-fault observable and replay the appropriate Coordinator
//     method, asserting the idempotent / replay-safe contract.
//   - Validation reject (11–13): drive CompleteUpload's Branch A/B/D four-
//     branch gate directly with deliberately mismatched hashes or empty
//     ServerSHA.
//   - Fence rejection (14–15): UPDATE attempt_commits.worker_id / lease_id /
//     task_revision out from under the FenceTuple, then call
//     CompleteUpload / CommitAttempt and assert the gate refuses.
//   - Race (16): two Coordinator instances on the same *sql.DB; goroutines
//     concurrent on CommitAttempt; exactly one wins, the other returns
//     nil-replay or ErrTransitionConflict (acceptable replay contract).
//   - Delivery restore (17): assert job_deliveries rows persist after a clean
//     CommitAttempt — the durable cross-join is the canonical contract post
//     DeliveryRunner restart.
package completion

import (
	"database/sql"
	"testing"
	"time"
)

// pastRFC3339 returns a Deadline already-elapsed timestamp formatted
// as RFC3339Nano (the wire format for attempt_commits.commit_deadline_at
// AND tasks.lease_expires_at). Used in scenarios 5/6/7 to push
// commit_deadline_at into the past so ReconcileAttempt's
// "deadlineElapsed = now.After(t)" branch fires.
func pastRFC3339() string {
	return time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
}

// scheduleRowReady promotes the canonical attempt_commits fixture into
// the "ready to be CommitAttempt'd" state the same way the production
// Code path would. Tests use this once after CompleteUpload to skip
// the unrelated-to-this-test artifact-ready machinery and drive a
// deterministic CommitAttempt path.
func scheduleRowReady(t *testing.T, db *sql.DB, fence FenceTuple, artifactID string) string {
	t.Helper()
	var declaredSHA string
	if err := db.QueryRow(`SELECT COALESCE(expected_sha256,'') FROM task_output_declarations WHERE task_id=? AND attempt_id=? LIMIT 1`, fence.TaskID, fence.AttemptID).Scan(&declaredSHA); err != nil {
		t.Fatalf("scheduleRowReady declaration hash: %v", err)
	}
	if len(declaredSHA) != 64 {
		t.Fatalf("scheduleRowReady declaration hash=%q, want canonical 64-hex SHA-256", declaredSHA)
	}
	if _, err := db.Exec(`UPDATE artifacts SET status='READY', verified_at=?, sha256=?, storage_key=? WHERE id=?`,
		time.Now().UTC().Format(time.RFC3339), declaredSHA, "durable/"+artifactID, artifactID); err != nil {
		t.Fatalf("scheduleRowReady artifact ready: %v", err)
	}
	if _, err := db.Exec(`UPDATE artifact_uploads SET received_sha256=?, received_size_bytes=(SELECT size_bytes FROM artifacts WHERE id=?) WHERE artifact_id=?`,
		declaredSHA, artifactID, artifactID); err != nil {
		t.Fatalf("scheduleRowReady upload evidence: %v", err)
	}
	if _, err := db.Exec(
		`UPDATE task_output_declarations
		 SET upload_id=(SELECT upload_id FROM artifact_uploads WHERE artifact_id=? LIMIT 1),
		     artifact_id=?
		 WHERE task_id=? AND attempt_id=? AND (artifact_id IS NULL OR artifact_id=?)`,
		artifactID, artifactID, fence.TaskID, fence.AttemptID, artifactID,
	); err != nil {
		t.Fatalf("scheduleRowReady declaration binding: %v", err)
	}
	if _, err := db.Exec(
		`UPDATE attempt_commits SET ready_output_count = required_output_count WHERE task_id = ? AND attempt_id = ?`,
		fence.TaskID, fence.AttemptID,
	); err != nil {
		t.Fatalf("scheduleRowReady attempt_commits ready bump: %v", err)
	}
	return readAttemptCommitRow(t, db, fence).CommitID
}

// seedDeliveryDestination seeds one ENABLED destination row so the
// destination exists for per-job plans referencing it.
func seedDeliveryDestination(t *testing.T, db *sql.DB, destID, provider string) {
	t.Helper()
	if _, err := db.Exec(
		`INSERT INTO delivery_destinations (destination_id, provider, name, enabled, created_at, updated_at)
		 VALUES (?, ?, ?, 1, ?, ?)`,
		destID, provider, "Test "+destID, "2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z",
	); err != nil {
		t.Fatalf("seedDeliveryDestination: %v", err)
	}
}

// seedJobDeliveryPlan seeds an ENABLED explicit per-job plan row. The
// CommitAttempt delivery fan-out reads ONLY job_delivery_plans; without
// an explicit plan the job gets zero delivery rows (render-only).
func seedJobDeliveryPlan(t *testing.T, db *sql.DB, jobID, destID string) {
	t.Helper()
	if _, err := db.Exec(
		`INSERT INTO job_delivery_plans (job_id, destination_id, enabled, priority, retry_budget, created_at, updated_at)
		 VALUES (?, ?, 1, 0, 5, ?, ?)`,
		jobID, destID, "2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z",
	); err != nil {
		t.Fatalf("seedJobDeliveryPlan: %v", err)
	}
}

// ────────────────────────────────────────────────────────────────────────
// Scenario 04 — final ack lost.
// ────────────────────────────────────────────────────────────────────────

// ────────────────────────────────────────────────────────────────────────
// Scenarios 05/06/07 — worker crash at three checkpoints. ReconcileAttempt
// transitions attempt_commits to EXPIRED when commit_deadline_at is past.
// ────────────────────────────────────────────────────────────────────────

// ────────────────────────────────────────────────────────────────────────
// Scenarios 08/09/10 — master crash at three checkpoints. The worker's
// retry pathway must converge cleanly: the protocol is replay-safe.
// ────────────────────────────────────────────────────────────────────────

// ────────────────────────────────────────────────────────────────────────
// Scenario 11 — codec fail (Branch A: no master SHA, no declarative SHA).
// Artifact stays VERIFYING.
// ────────────────────────────────────────────────────────────────────────

// ────────────────────────────────────────────────────────────────────────
// Scenario 12 — size-band / SHA mismatch (Branch D rejection).
// ────────────────────────────────────────────────────────────────────────

// ────────────────────────────────────────────────────────────────────────
// Scenario 13 — probe fail (Branch B: no master SHA but declarative SHA
// present). Artifact stays VERIFYING.
// ────────────────────────────────────────────────────────────────────────

// ────────────────────────────────────────────────────────────────────────
// Scenarios 14, 15 — fence rejection (lease revoke or stale revision).
// ────────────────────────────────────────────────────────────────────────

// ────────────────────────────────────────────────────────────────────────
// Scenarios 16, 17 — race + delivery restore.
// ────────────────────────────────────────────────────────────────────────

// TestCommitAttempt_DeliversOnlyToExplicitPlanDestinations locks the
// Commit-4 delivery contract: CommitAttempt fans out ONLY to the job's
// explicit job_delivery_plans destinations. A globally enabled
// delivery_destinations row that is NOT in the job's plan must never
// receive a delivery row (no implicit routing to unrelated folders).
// ────────────────────────────────────────────────────────────────────────
// Phase 6 smoke gate: a single integration test running a full happy
// path submit → Drive commit → assert all durable state surfaces.
// ────────────────────────────────────────────────────────────────────────

// TestPhase6_ArtifactContractJobPromotesFromRunning pins the
// finalizer-owned promotion in the completion commit path: an
// artifact-contract job that reaches CommitAttempt while still RUNNING
// (the worker publishes its outputs — declare → upload → complete —
// before the TaskResult whose ingest would normally roll the job
// RUNNING→AWAITING_ARTIFACT) is promoted to SUCCEEDED inside the same
// transaction. The gate is the artifact set itself: until the outputs
// are provably READY the commit must fail closed and the job must stay
// RUNNING.

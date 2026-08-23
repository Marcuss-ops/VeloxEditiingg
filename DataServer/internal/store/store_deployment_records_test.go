package store

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// newDeploymentTestStore stands up a fresh on-disk SQLite store
// with the deployment_records table ready. Uses t.TempDir() so
// the file is auto-cleaned; the SQLite handle is closed via
// t.Cleanup. On-disk (not :memory:) so the same test file can
// be re-opened across sub-tests if needed.
//
// Seeds a couple of workers rows so the deployment_records FK on
// worker_id (PRAGMA foreign_keys=ON in sqliteTunePragmas) lands
// cleanly. The INSERT shape mirrors the canonical pattern in
// store_worker_runtime_test.go:79 — worker_id, worker_name,
// node_role (must be 'worker' per the migration 094 trigger),
// raw_json (a valid JSON object), migrated_at.
func newDeploymentTestStore(t *testing.T) *SQLiteStore {
	t.Helper()
	return newDeploymentTestStoreAt(t, filepath.Join(t.TempDir(), "deployment-test.db"))
}

// newDeploymentTestStoreAt stands up the canonical deployment test store on
// an EXPLICIT path, seeding the workers rows the FK references. Recovery tests
// use the same path twice (create → close → reopen) to simulate a Master
// restart: the SQLite file is the only state that survives.
func newDeploymentTestStoreAt(t *testing.T, path string) *SQLiteStore {
	t.Helper()
	s, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("NewSQLiteStore(%s): %v", path, err)
	}
	if err := s.CreateDeploymentRecordsTableIfNotExists(); err != nil {
		t.Fatalf("CreateDeploymentRecordsTableIfNotExists: %v", err)
	}
	seeds := []struct{ id, name string }{
		{"wicket", "wicket-vps"},
		{"velox-worker-523925eb", "velox-worker-523925eb-vps"},
	}
	for _, sd := range seeds {
		if _, err := s.db.Exec(
			`INSERT INTO workers (worker_id, worker_name, node_role, raw_json, migrated_at) VALUES (?, ?, 'worker', '{}', datetime('now'))`,
			sd.id, sd.name,
		); err != nil {
			t.Fatalf("seed workers %s: %v", sd.id, err)
		}
	}
	t.Cleanup(func() {
		_ = s.Close()
	})
	return s
}

// reopenDeploymentTestStore re-opens an EXISTING deployment test DB after the
// original handle was closed — the Master-restart boundary. Table bootstrap is
// idempotent; the workers seed is NOT repeated (it persisted in the file).
func reopenDeploymentTestStore(t *testing.T, path string) *SQLiteStore {
	t.Helper()
	s, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("reopen NewSQLiteStore(%s): %v", path, err)
	}
	if err := s.CreateDeploymentRecordsTableIfNotExists(); err != nil {
		t.Fatalf("reopen CreateDeploymentRecordsTableIfNotExists: %v", err)
	}
	t.Cleanup(func() {
		_ = s.Close()
	})
	return s
}

func deploymentTestDigest(c rune) string {
	return "sha256:" + strings.Repeat(string(c), 64)
}

func deploymentTimePtr(t time.Time) *time.Time { return &t }

// TestDeploymentStore_InsertAndGetLatest verifies the basic
// round-trip: insert a PENDING row, fetch the latest by
// worker_id, all fields preserved.
// TestDeploymentStore_UpdateTerminalStatus asserts the
// PENDING → SUCCEEDED transition writes finished_at and reflects
// the new status on the next read.
// TestDeploymentStore_UpdateRejectsNonTerminal pins the API
// contract: UpdateDeploymentStatus ONLY accepts terminal
// statuses (SUCCEEDED/FAILED/ROLLED_BACK), not PENDING (which
// would be a meaningless transition — already initial state).
// TestDeploymentStore_ListOrderAndLimit inserts three rows with
// monotonically increasing started_at and verifies the list
// ordering (DESC) and the limit parameter.
// TestDeploymentStore_NotFound pins the ErrDeploymentNotFound
// sentinel for the "no deploys yet" case.
// TestDeploymentStore_RejectNonPendingInitial asserts the
// insert-side contract: initial status MUST be PENDING. A
// caller trying to record an already-terminal status gets
// rejected at the API boundary, not the SQL CHECK.
// TestDeploymentStore_BootstrapIdempotent asserts the DDL is
// idempotent: calling the bootstrap twice in a row does NOT
// fail (production setups apply the migration once via the
// runner, but the test's helper calls CreateDeploymentRecords-
// TableIfNotExists multiple times — particularly across
// sub-test boundaries — and must remain silent).
// TestDeploymentStore_TerminalStatusIsImmutable pins the canonical machine's
// no-resurrection rule at the store boundary: once a row is SUCCEEDED it can
// never be moved to a different terminal status (SUCCEEDED → FAILED is the
// classic clobber). The rejected transition must not touch the ledger row NOR
// the worker_deployment_state projection.
// TestDeploymentStore_TransitionUpdatesRecordAndProjectionAtomically is the
// POSITIVE twin of TestDeploymentStore_ProjectionFailureRollsBackTransition:
// one store call (PENDING → SUCCEEDED) must leave BOTH the journal row and
// the worker_deployment_state read model updated — the read model is a
// projection written inside the same transaction, not something the API
// reconstructs from history afterwards.
// TestDeploymentStore_FailedCannotResurrectToSucceeded pins the mirror case:
// a FAILED rollout is terminal and cannot be flipped to SUCCEEDED by a late
// or duplicate completion report.
// TestDeploymentStore_RolledBackIsTerminal pins the third terminal state:
// ROLLED_BACK rows are immutable like SUCCEEDED/FAILED — a rollback cascade
// that completed can never be flipped back to SUCCEEDED.
// TestDeploymentStore_RollbackFailedIsTerminal pins the rollback-also-
// failed terminal: MarkDeploymentRolledBack(rollbackOK=false) lands on
// PENDING → FAILED with is_rollback=1, and that row is then immutable like
// every other terminal row.
// TestDeploymentStore_ProjectionFailureRollsBackTransition pins the atomic
// journal + read-model contract: if the worker_deployment_state projection
// write fails inside the transition transaction, the deployment_records
// UPDATE must roll back too — never a torn (ledger=SUCCEEDED,
// projection=stale) state. The failure is forced with a SQLite trigger that
// aborts any write to the read model.
// ============================================================
// worker_deployment_state read model (migration 151)
// ============================================================

// TestWorkerDeploymentState_RunningDigestNullUntilHeartbeat pins the
// central read-model invariant: running_digest is written ONLY by an
// authenticated heartbeat. A deployment record (control-plane intent) must
// never fabricate it — the fresh state row created by
// InsertDeploymentRecord must carry an empty/NULL running digest, and only
// upsertWorkerRunningDigest (the heartbeat path) fills it in.
// TestWorkerDeploymentState_FailedRolloutPreservesLastSuccessfulDigest is
// the store-level twin of the migration backfill test: after a SUCCEEDED
// rollout to A, a newer FAILED rollout to B must leave the read model with
// last_successful=A, desired=B, running untouched, and the FAILED operation
// (with its error) visible.
// TestWorkerDeploymentState_StaleHeartbeatCannotEraseObservedDigest pins
// the heartbeat guard: an empty/absent image_digest must NOT blank out a
// previously observed running digest (spec §2: heartbeat metadata is
// authoritative only when it carries a value).
// TestWorkerDeploymentState_NotFound pins the absent-row sentinel so the
// admin API can distinguish "no state row" (pre-151 worker) from "state row
// with empty fields".
// TestWorkerDeploymentState_TerminalTransitionPreservesRunningDigest pins the
// read-model invariant that deployment transitions (control-plane intent) can
// never clobber the heartbeat-observed running digest. The upsert's ON
// CONFLICT DO UPDATE must keep running_digest out of the SET clause; if a
// future change adds it, this test fails.
// TestWorkerDeploymentState_GenericSucceededDoesNotAdvance pins the
// VERIFYING_DIGEST enforcement: the generic UpdateDeploymentStatus(SUCCEEDED)
// path — which carries NO digest verification — must NOT advance
// last_successful_digest. Only MarkVerifiedSucceeded (after an authenticated
// digest match) can make a new digest the last-known-good one.
// TestWorkerDeploymentState_VerifiedMismatchRejected pins the digest gate in
// MarkVerifiedSucceeded: an observed digest != target is rejected with
// ErrDeploymentDigestMismatch, the row stays PENDING, last_successful_digest
// is untouched, and running_digest is untouched (the mismatch must not be
// "fixed" by copying observed into the read model).
// TestWorkerDeploymentState_PhaseRecordedAndPreserved pins migration 152:
// RecordDeploymentPhase writes the in-flight phase into the read model, the
// phase survives subsequent record transitions (never blanked), and it is
// orthogonal to digest state.
// ============================================================
// error_code / error_message separation (migration 153)
// ============================================================

// TestWorkerDeploymentState_ErrorCodeAndMessagePersisted pins migration 153
// end-to-end through the repository adapter (the path the fleet executor
// uses): MarkFailed(code, msg) writes BOTH the stable code and the
// human-readable message to the journal row AND projects them into the read
// model's last_operation_error_code / last_operation_error.
// TestWorkerDeploymentState_NewOperationClearsErrorPreservesHistory pins the
// "new operation clears the current error but preserves history" contract:
// after op#1 FAILED with DIGEST_MISMATCH, inserting op#2 (PENDING) blanks
// last_operation_error_code / last_operation_error in the read model, while
// the journal row of op#1 keeps its code+message forever (audit history is
// never rewritten).
// TestWorkerDeploymentState_VerifiedSuccessClearsErrorCode pins the
// successful-terminal write: MarkVerifiedSucceeded clears both the code and
// the message from the journal row AND the read model — a later rollout that
// succeeds must not leave the previous DIGEST_MISMATCH visible as the
// current error.
// ============================================================
// Recovery after Master restart (acceptance conditions §29-30)
// ============================================================

// TestDeploymentRecovery_RestartDuringWaitingReadyResumesOnNewSessionHeartbeat
// is the durable-layer twin of the spec's §29 acceptance condition: the
// Master crashes while a rollout is parked in WAITING_READY; the PENDING
// deployment row + the read model (last_phase=WAITING_READY, desired=B) are
// the ONLY state that survives. After the restart the rollout must NOT
// complete on the pre-restart session's stale observation — it resumes ONLY
// when the NEW session's authenticated heartbeat advertises the target
// digest, and only then does MarkVerifiedSucceeded close the row.
// TestDeploymentRecovery_RestartDuringDeployingNeverAssumesSuccess pins the
// §30 acceptance condition: the Master crashes while a rollout to B is in
// DEPLOYING and the worker comes back advertising digest C. The reconciler
// must compare operation/session/running digest BEFORE deciding: running C !=
// target B → the rollout is NOT assumed successful. MarkVerifiedSucceeded is
// refused, the row stays PENDING, last-known-good A survives, and the drift
// (desired=B, running=C) stays visible in the read model.

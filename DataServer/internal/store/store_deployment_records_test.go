package store

import (
	"context"
	"errors"
	"fmt"
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
	path := filepath.Join(t.TempDir(), "deployment-test.db")
	s, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
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

func deploymentTestDigest(c rune) string {
	return "sha256:" + strings.Repeat(string(c), 64)
}

func deploymentTimePtr(t time.Time) *time.Time { return &t }

// TestDeploymentStore_InsertAndGetLatest verifies the basic
// round-trip: insert a PENDING row, fetch the latest by
// worker_id, all fields preserved.
func TestDeploymentStore_InsertAndGetLatest(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()

	rec := DeploymentRecord{
		DeploymentID:   "deploy-2026-07-28-001",
		WorkerID:       "wicket",
		PreviousDigest: deploymentTestDigest('a'),
		TargetDigest:   deploymentTestDigest('b'),
		StartedAt:      time.Now().UTC().Truncate(time.Second),
		Status:         DeployStatusPending,
		AppliedBy:      "admin@example.com",
	}
	if err := s.InsertDeploymentRecord(ctx, rec); err != nil {
		t.Fatalf("InsertDeploymentRecord: %v", err)
	}

	got, err := s.GetLatestDeploymentForWorker(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetLatestDeploymentForWorker: %v", err)
	}
	if got.DeploymentID != rec.DeploymentID {
		t.Errorf("DeploymentID = %q, want %q", got.DeploymentID, rec.DeploymentID)
	}
	if got.PreviousDigest != rec.PreviousDigest {
		t.Errorf("PreviousDigest = %q, want %q", got.PreviousDigest, rec.PreviousDigest)
	}
	if got.TargetDigest != rec.TargetDigest {
		t.Errorf("TargetDigest = %q, want %q", got.TargetDigest, rec.TargetDigest)
	}
	if got.Status != DeployStatusPending {
		t.Errorf("Status = %q, want PENDING", got.Status)
	}
	if got.IsRollback {
		t.Errorf("IsRollback = true, want false")
	}
}

func TestDeploymentStore_LastSuccessfulIgnoresNewerFailedRollout(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	oldDigest := deploymentTestDigest('a')
	newDigest := deploymentTestDigest('b')

	if err := s.InsertBaselineDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID: "deploy-success-old",
		WorkerID:     "wicket",
		TargetDigest: oldDigest,
		StartedAt:    base,
		FinishedAt:   deploymentTimePtr(base),
		Status:       DeployStatusSucceeded,
		AppliedBy:    "bootstrap",
	}); err != nil {
		t.Fatalf("InsertBaselineDeploymentRecord: %v", err)
	}
	if err := s.InsertDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID:   "deploy-failed-new",
		WorkerID:       "wicket",
		PreviousDigest: oldDigest,
		TargetDigest:   newDigest,
		StartedAt:      base.Add(time.Minute),
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}); err != nil {
		t.Fatalf("InsertDeploymentRecord: %v", err)
	}
	if err := s.UpdateDeploymentStatus(ctx, "deploy-failed-new", DeployStatusFailed, base.Add(2*time.Minute)); err != nil {
		t.Fatalf("UpdateDeploymentStatus: %v", err)
	}

	got, err := s.GetLatestSuccessfulDeploymentForWorker(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetLatestSuccessfulDeploymentForWorker: %v", err)
	}
	if got.TargetDigest != oldDigest || got.DeploymentID != "deploy-success-old" {
		t.Fatalf("last successful deployment = %#v, want old successful digest", got)
	}
}

// TestDeploymentStore_UpdateTerminalStatus asserts the
// PENDING → SUCCEEDED transition writes finished_at and reflects
// the new status on the next read.
func TestDeploymentStore_UpdateTerminalStatus(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()

	rec := DeploymentRecord{
		DeploymentID:   "deploy-success-1",
		WorkerID:       "velox-worker-523925eb",
		PreviousDigest: deploymentTestDigest('a'),
		TargetDigest:   deploymentTestDigest('b'),
		StartedAt:      time.Now().UTC().Truncate(time.Second),
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}
	if err := s.InsertDeploymentRecord(ctx, rec); err != nil {
		t.Fatalf("insert: %v", err)
	}

	finish := time.Now().UTC().Truncate(time.Second)
	if err := s.UpdateDeploymentStatus(ctx, rec.DeploymentID, DeployStatusSucceeded, finish); err != nil {
		t.Fatalf("UpdateDeploymentStatus: %v", err)
	}

	got, err := s.GetLatestDeploymentForWorker(ctx, rec.WorkerID)
	if err != nil {
		t.Fatalf("get latest: %v", err)
	}
	if got.Status != DeployStatusSucceeded {
		t.Errorf("Status = %q, want SUCCEEDED", got.Status)
	}
	if got.FinishedAt == nil {
		t.Errorf("FinishedAt = nil, want set after terminal transition")
	}
	if got.FinishedAt != nil && !got.FinishedAt.Equal(finish) {
		t.Errorf("FinishedAt = %v, want %v", got.FinishedAt, finish)
	}
}

// TestDeploymentStore_UpdateRejectsNonTerminal pins the API
// contract: UpdateDeploymentStatus ONLY accepts terminal
// statuses (SUCCEEDED/FAILED/ROLLED_BACK), not PENDING (which
// would be a meaningless transition — already initial state).
func TestDeploymentStore_UpdateRejectsNonTerminal(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()

	rec := DeploymentRecord{
		DeploymentID:   "deploy-update-bad",
		WorkerID:       "wicket",
		PreviousDigest: deploymentTestDigest('a'),
		TargetDigest:   deploymentTestDigest('b'),
		StartedAt:      time.Now().UTC().Truncate(time.Second),
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}
	if err := s.InsertDeploymentRecord(ctx, rec); err != nil {
		t.Fatalf("insert: %v", err)
	}

	err := s.UpdateDeploymentStatus(ctx, rec.DeploymentID, DeployStatusPending, time.Now().UTC())
	if err == nil {
		t.Errorf("UpdateDeploymentStatus(PENDING) should fail, got nil")
	}
}

func TestDeploymentStore_UpdatesFailClosedWhenRowIsMissing(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	finished := time.Now().UTC()

	if err := s.UpdateDeploymentStatus(ctx, "missing", DeployStatusFailed, finished); !errors.Is(err, ErrDeploymentNotFound) {
		t.Fatalf("UpdateDeploymentStatus error = %v, want ErrDeploymentNotFound", err)
	}
	if err := s.UpdateDeploymentRollbackFlag(ctx, "missing", true); !errors.Is(err, ErrDeploymentNotFound) {
		t.Fatalf("UpdateDeploymentRollbackFlag error = %v, want ErrDeploymentNotFound", err)
	}
	if err := s.MarkDeploymentRolledBack(ctx, "missing", finished, true); !errors.Is(err, ErrDeploymentNotFound) {
		t.Fatalf("MarkDeploymentRolledBack error = %v, want ErrDeploymentNotFound", err)
	}
}

func TestDeploymentStore_RejectsCorruptTimestamps(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	rec := DeploymentRecord{
		DeploymentID:   "deploy-corrupt-time",
		WorkerID:       "wicket",
		PreviousDigest: deploymentTestDigest('a'),
		TargetDigest:   deploymentTestDigest('b'),
		StartedAt:      time.Now().UTC(),
		Status:         DeployStatusPending,
		AppliedBy:      "test",
	}
	if err := s.InsertDeploymentRecord(ctx, rec); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE deployment_records SET started_at = 'not-a-timestamp' WHERE deployment_id = ?`, rec.DeploymentID); err != nil {
		t.Fatalf("corrupt row: %v", err)
	}
	if _, err := s.GetLatestDeploymentForWorker(ctx, rec.WorkerID); err == nil {
		t.Fatal("GetLatestDeploymentForWorker returned nil error for corrupt timestamp")
	}
}

// TestDeploymentStore_ListOrderAndLimit inserts three rows with
// monotonically increasing started_at and verifies the list
// ordering (DESC) and the limit parameter.
func TestDeploymentStore_ListOrderAndLimit(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()

	base := time.Now().UTC().Truncate(time.Second)
	for i := 0; i < 3; i++ {
		dig := "sha256:" + strings.Repeat(string(rune('a'+i)), 64)
		rec := DeploymentRecord{
			DeploymentID:   fmt.Sprintf("deploy-list-%d", i),
			WorkerID:       "wicket",
			PreviousDigest: deploymentTestDigest('a'),
			TargetDigest:   dig,
			StartedAt:      base.Add(time.Duration(i) * time.Minute),
			Status:         DeployStatusPending,
			AppliedBy:      "fleetctl",
		}
		if err := s.InsertDeploymentRecord(ctx, rec); err != nil {
			t.Fatalf("insert [%d]: %v", i, err)
		}
	}

	all, err := s.ListDeploymentsForWorker(ctx, "wicket", 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("len(all) = %d, want 3", len(all))
	}
	// Most recent first.
	if all[0].DeploymentID != "deploy-list-2" {
		t.Errorf("latest = %q, want deploy-list-2", all[0].DeploymentID)
	}
	if all[2].DeploymentID != "deploy-list-0" {
		t.Errorf("oldest = %q, want deploy-list-0", all[2].DeploymentID)
	}

	limited, err := s.ListDeploymentsForWorker(ctx, "wicket", 2)
	if err != nil {
		t.Fatalf("limited: %v", err)
	}
	if len(limited) != 2 {
		t.Errorf("len(limited) = %d, want 2", len(limited))
	}
	if limited[0].DeploymentID != "deploy-list-2" {
		t.Errorf("limited[0] = %q, want deploy-list-2", limited[0].DeploymentID)
	}
}

// TestDeploymentStore_NotFound pins the ErrDeploymentNotFound
// sentinel for the "no deploys yet" case.
func TestDeploymentStore_NotFound(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	_, err := s.GetLatestDeploymentForWorker(ctx, "no-such-worker")
	if !errors.Is(err, ErrDeploymentNotFound) {
		t.Errorf("err = %v, want ErrDeploymentNotFound", err)
	}
}

// TestDeploymentStore_RejectNonPendingInitial asserts the
// insert-side contract: initial status MUST be PENDING. A
// caller trying to record an already-terminal status gets
// rejected at the API boundary, not the SQL CHECK.
func TestDeploymentStore_RejectNonPendingInitial(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	rec := DeploymentRecord{
		DeploymentID:   "deploy-bad-1",
		WorkerID:       "wicket",
		PreviousDigest: deploymentTestDigest('a'),
		TargetDigest:   deploymentTestDigest('b'),
		StartedAt:      time.Now().UTC().Truncate(time.Second),
		Status:         DeployStatusSucceeded, // already terminal
		AppliedBy:      "fleetctl",
	}
	if err := s.InsertDeploymentRecord(ctx, rec); err == nil {
		t.Errorf("InsertDeploymentRecord with terminal Status should fail, got nil")
	}
}

func TestDeploymentStore_BaselineAllowsMissingRollbackProvenance(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	started := time.Now().UTC().Truncate(time.Second)
	if err := s.InsertBaselineDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID: "bootstrap-wicket-1",
		WorkerID:     "wicket",
		TargetDigest: deploymentTestDigest('b'),
		StartedAt:    started,
		FinishedAt:   &started,
		Status:       DeployStatusSucceeded,
		AppliedBy:    "fleetctl",
	}); err != nil {
		t.Fatalf("InsertBaselineDeploymentRecord: %v", err)
	}

	got, err := s.GetLatestDeploymentForWorker(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetLatestDeploymentForWorker: %v", err)
	}
	if got.Status != DeployStatusSucceeded || got.TargetDigest != deploymentTestDigest('b') {
		t.Fatalf("baseline row = %+v, want SUCCEEDED with target digest", got)
	}
	if got.PreviousDigest != "" {
		t.Errorf("PreviousDigest = %q, want empty/missing provenance", got.PreviousDigest)
	}
	if got.FinishedAt == nil {
		t.Fatal("FinishedAt = nil, want terminal baseline timestamp")
	}
}

// TestDeploymentStore_RollbackFlagToggle asserts
// UpdateDeploymentRollbackFlag flips is_rollback on an existing
// row and the next read reflects the change.
func TestDeploymentStore_RollbackFlagToggle(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()

	rec := DeploymentRecord{
		DeploymentID:   "deploy-rollback-1",
		WorkerID:       "wicket",
		PreviousDigest: deploymentTestDigest('a'),
		TargetDigest:   deploymentTestDigest('b'),
		StartedAt:      time.Now().UTC().Truncate(time.Second),
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}
	if err := s.InsertDeploymentRecord(ctx, rec); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if err := s.UpdateDeploymentRollbackFlag(ctx, rec.DeploymentID, true); err != nil {
		t.Fatalf("rollback flag toggle: %v", err)
	}

	got, err := s.GetLatestDeploymentForWorker(ctx, rec.WorkerID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !got.IsRollback {
		t.Errorf("IsRollback = false, want true after toggle")
	}
}

// TestDeploymentStore_BootstrapIdempotent asserts the DDL is
// idempotent: calling the bootstrap twice in a row does NOT
// fail (production setups apply the migration once via the
// runner, but the test's helper calls CreateDeploymentRecords-
// TableIfNotExists multiple times — particularly across
// sub-test boundaries — and must remain silent).
func TestDeploymentStore_BootstrapIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "deployment-bootstrap.db")
	s, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	for i := 0; i < 3; i++ {
		if err := s.CreateDeploymentRecordsTableIfNotExists(); err != nil {
			t.Errorf("bootstrap call %d: %v", i, err)
		}
	}
}

// ============================================================
// worker_deployment_state read model (migration 151)
// ============================================================

// TestWorkerDeploymentState_RunningDigestNullUntilHeartbeat pins the
// central read-model invariant: running_digest is written ONLY by an
// authenticated heartbeat. A deployment record (control-plane intent) must
// never fabricate it — the fresh state row created by
// InsertDeploymentRecord must carry an empty/NULL running digest, and only
// upsertWorkerRunningDigest (the heartbeat path) fills it in.
func TestWorkerDeploymentState_RunningDigestNullUntilHeartbeat(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()

	rec := DeploymentRecord{
		DeploymentID:   "deploy-state-1",
		WorkerID:       "wicket",
		PreviousDigest: deploymentTestDigest('a'),
		TargetDigest:   deploymentTestDigest('b'),
		StartedAt:      time.Now().UTC().Truncate(time.Second),
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}
	if err := s.InsertDeploymentRecord(ctx, rec); err != nil {
		t.Fatalf("InsertDeploymentRecord: %v", err)
	}

	state, err := s.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.RunningDigest != "" {
		t.Errorf("RunningDigest = %q, want empty (NULL) until first authenticated heartbeat", state.RunningDigest)
	}
	if state.DesiredDigest != rec.TargetDigest {
		t.Errorf("DesiredDigest = %q, want %q (control-plane intent from record)", state.DesiredDigest, rec.TargetDigest)
	}
	if state.LastOperationID != rec.DeploymentID || state.LastOperationStatus != DeployStatusPending {
		t.Errorf("last operation = %s/%s, want %s/PENDING", state.LastOperationID, state.LastOperationStatus, rec.DeploymentID)
	}

	// Authenticated heartbeat observes digest 'c' (drift from desired 'b').
	heartbeatDigest := deploymentTestDigest('c')
	if err := upsertWorkerRunningDigest(ctx, s.db, "wicket", heartbeatDigest, time.Now().UTC()); err != nil {
		t.Fatalf("upsertWorkerRunningDigest: %v", err)
	}
	state, err = s.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState after heartbeat: %v", err)
	}
	if state.RunningDigest != heartbeatDigest {
		t.Errorf("RunningDigest after heartbeat = %q, want %q", state.RunningDigest, heartbeatDigest)
	}
	// The heartbeat must not touch intent.
	if state.DesiredDigest != rec.TargetDigest {
		t.Errorf("DesiredDigest after heartbeat = %q, want %q (heartbeat never writes intent)", state.DesiredDigest, rec.TargetDigest)
	}
}

// TestWorkerDeploymentState_FailedRolloutPreservesLastSuccessfulDigest is
// the store-level twin of the migration backfill test: after a SUCCEEDED
// rollout to A, a newer FAILED rollout to B must leave the read model with
// last_successful=A, desired=B, running untouched, and the FAILED operation
// (with its error) visible.
func TestWorkerDeploymentState_FailedRolloutPreservesLastSuccessfulDigest(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	digestA := deploymentTestDigest('a')
	digestB := deploymentTestDigest('b')

	if err := s.InsertBaselineDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID: "deploy-state-success",
		WorkerID:     "wicket",
		TargetDigest: digestA,
		StartedAt:    base,
		FinishedAt:   deploymentTimePtr(base),
		Status:       DeployStatusSucceeded,
		AppliedBy:    "bootstrap",
	}); err != nil {
		t.Fatalf("InsertBaselineDeploymentRecord: %v", err)
	}
	if err := s.InsertDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID:   "deploy-state-failed",
		WorkerID:       "wicket",
		PreviousDigest: digestA,
		TargetDigest:   digestB,
		StartedAt:      base.Add(time.Minute),
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}); err != nil {
		t.Fatalf("InsertDeploymentRecord: %v", err)
	}
	if err := s.updateDeploymentTerminal(ctx, "deploy-state-failed", DeployStatusFailed, base.Add(2*time.Minute), "cosign verify failed", false); err != nil {
		t.Fatalf("updateDeploymentTerminal(FAILED): %v", err)
	}

	state, err := s.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.DesiredDigest != digestB {
		t.Errorf("DesiredDigest = %q, want %q (failed target remains the intent)", state.DesiredDigest, digestB)
	}
	if state.LastSuccessfulDigest != digestA {
		t.Errorf("LastSuccessfulDigest = %q, want %q (failed rollout must not erase last-known-good)", state.LastSuccessfulDigest, digestA)
	}
	if state.RunningDigest != "" {
		t.Errorf("RunningDigest = %q, want empty (no heartbeat yet)", state.RunningDigest)
	}
	if state.LastOperationID != "deploy-state-failed" || state.LastOperationStatus != DeployStatusFailed {
		t.Errorf("last operation = %s/%s, want deploy-state-failed/FAILED", state.LastOperationID, state.LastOperationStatus)
	}
	if state.LastOperationError != "cosign verify failed" {
		t.Errorf("LastOperationError = %q, want the FAILED transition error", state.LastOperationError)
	}
}

// TestWorkerDeploymentState_StaleHeartbeatCannotEraseObservedDigest pins
// the heartbeat guard: an empty/absent image_digest must NOT blank out a
// previously observed running digest (spec §2: heartbeat metadata is
// authoritative only when it carries a value).
func TestWorkerDeploymentState_StaleHeartbeatCannotEraseObservedDigest(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()

	observed := deploymentTestDigest('b')
	if err := upsertWorkerRunningDigest(ctx, s.db, "wicket", observed, time.Now().UTC()); err != nil {
		t.Fatalf("upsertWorkerRunningDigest: %v", err)
	}
	// A heartbeat without a digest is a no-op — it must not erase 'b'.
	if err := upsertWorkerRunningDigest(ctx, s.db, "wicket", "", time.Now().UTC()); err != nil {
		t.Fatalf("upsertWorkerRunningDigest(empty): %v", err)
	}

	state, err := s.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.RunningDigest != observed {
		t.Errorf("RunningDigest = %q, want %q (empty heartbeat must not erase observation)", state.RunningDigest, observed)
	}
}

// TestWorkerDeploymentState_NotFound pins the absent-row sentinel so the
// admin API can distinguish "no state row" (pre-151 worker) from "state row
// with empty fields".
func TestWorkerDeploymentState_NotFound(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	_, err := s.GetWorkerDeploymentState(ctx, "no-such-worker")
	if !errors.Is(err, ErrWorkerDeploymentStateNotFound) {
		t.Errorf("err = %v, want ErrWorkerDeploymentStateNotFound", err)
	}
}

// TestWorkerDeploymentState_TerminalTransitionPreservesRunningDigest pins the
// read-model invariant that deployment transitions (control-plane intent) can
// never clobber the heartbeat-observed running digest. The upsert's ON
// CONFLICT DO UPDATE must keep running_digest out of the SET clause; if a
// future change adds it, this test fails.
func TestWorkerDeploymentState_TerminalTransitionPreservesRunningDigest(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)
	digestB := deploymentTestDigest('b')

	// Heartbeat observes the worker actually running digest 'b'.
	if err := upsertWorkerRunningDigest(ctx, s.db, "wicket", digestB, base); err != nil {
		t.Fatalf("upsertWorkerRunningDigest: %v", err)
	}

	// A rollout to digest 'c' then SUCCEEDED — the terminal transition must
	// not rewrite running_digest (still 'b' until the next heartbeat).
	if err := s.InsertDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID:   "deploy-state-c",
		WorkerID:       "wicket",
		PreviousDigest: digestB,
		TargetDigest:   deploymentTestDigest('c'),
		StartedAt:      base.Add(time.Minute),
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}); err != nil {
		t.Fatalf("InsertDeploymentRecord: %v", err)
	}
	if err := s.updateDeploymentTerminal(ctx, "deploy-state-c", DeployStatusSucceeded, base.Add(2*time.Minute), "", false); err != nil {
		t.Fatalf("updateDeploymentTerminal(SUCCEEDED): %v", err)
	}

	state, err := s.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.RunningDigest != digestB {
		t.Errorf("RunningDigest after SUCCEEDED transition = %q, want %q (transition must not clobber observation)", state.RunningDigest, digestB)
	}
	if state.LastSuccessfulDigest != deploymentTestDigest('c') {
		t.Errorf("LastSuccessfulDigest = %q, want %q (verification advances last-known-good)", state.LastSuccessfulDigest, deploymentTestDigest('c'))
	}
	if state.LastOperationStatus != DeployStatusSucceeded {
		t.Errorf("LastOperationStatus = %q, want SUCCEEDED", state.LastOperationStatus)
	}
}

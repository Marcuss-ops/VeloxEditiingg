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
	if err := s.MarkDeploymentRolledBack(ctx, "missing", finished, true, ""); !errors.Is(err, ErrDeploymentNotFound) {
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

// TestDeploymentStore_TerminalStatusIsImmutable pins the canonical machine's
// no-resurrection rule at the store boundary: once a row is SUCCEEDED it can
// never be moved to a different terminal status (SUCCEEDED → FAILED is the
// classic clobber). The rejected transition must not touch the ledger row NOR
// the worker_deployment_state projection.
func TestDeploymentStore_TerminalStatusIsImmutable(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)

	rec := DeploymentRecord{
		DeploymentID:   "deploy-immutable",
		WorkerID:       "wicket",
		PreviousDigest: deploymentTestDigest('a'),
		TargetDigest:   deploymentTestDigest('b'),
		StartedAt:      base,
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}
	if err := s.InsertDeploymentRecord(ctx, rec); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := s.UpdateDeploymentStatus(ctx, rec.DeploymentID, DeployStatusSucceeded, base.Add(time.Minute)); err != nil {
		t.Fatalf("PENDING -> SUCCEEDED: %v", err)
	}

	// Resurrection attempt: SUCCEEDED -> FAILED must be rejected.
	err := s.UpdateDeploymentStatus(ctx, rec.DeploymentID, DeployStatusFailed, base.Add(2*time.Minute))
	if !errors.Is(err, ErrIllegalDeploymentTransition) {
		t.Fatalf("SUCCEEDED -> FAILED error = %v, want ErrIllegalDeploymentTransition", err)
	}

	got, err := s.GetLatestDeploymentForWorker(ctx, rec.WorkerID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != DeployStatusSucceeded {
		t.Errorf("Status after rejected transition = %q, want SUCCEEDED (row must stay terminal)", got.Status)
	}
	// "Writes nothing": the rejected FAILED attempt must not restamp
	// finished_at, must not leak an error onto the SUCCEEDED row, and the
	// read-model projection must stay byte-identical to the pre-attempt
	// state. The transition API validates BEFORE it writes, so a rejected
	// transition is a complete no-op.
	if got.FinishedAt == nil || !got.FinishedAt.Equal(base.Add(time.Minute)) {
		t.Errorf("FinishedAt after rejected transition = %v, want %v (rejected attempt must not restamp)", got.FinishedAt, base.Add(time.Minute))
	}
	if got.ErrorCode != "" || got.ErrorMessage != "" {
		t.Errorf("journal error after rejected transition = code=%q msg=%q, want empty (rejected attempt must not write error)", got.ErrorCode, got.ErrorMessage)
	}

	state, err := s.GetWorkerDeploymentState(ctx, rec.WorkerID)
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.LastOperationStatus != DeployStatusSucceeded {
		t.Errorf("LastOperationStatus after rejected transition = %q, want SUCCEEDED (projection must stay in sync)", state.LastOperationStatus)
	}
	if state.LastOperationErrorCode != "" || state.LastOperationError != "" {
		t.Errorf("projection error after rejected transition = code=%q msg=%q, want empty (rejected attempt must not write error)", state.LastOperationErrorCode, state.LastOperationError)
	}
}

// TestDeploymentStore_TransitionUpdatesRecordAndProjectionAtomically is the
// POSITIVE twin of TestDeploymentStore_ProjectionFailureRollsBackTransition:
// one store call (PENDING → SUCCEEDED) must leave BOTH the journal row and
// the worker_deployment_state read model updated — the read model is a
// projection written inside the same transaction, not something the API
// reconstructs from history afterwards.
func TestDeploymentStore_TransitionUpdatesRecordAndProjectionAtomically(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)

	rec := DeploymentRecord{
		DeploymentID:   "deploy-atomic-pos",
		WorkerID:       "wicket",
		PreviousDigest: deploymentTestDigest('a'),
		TargetDigest:   deploymentTestDigest('b'),
		StartedAt:      base,
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}
	if err := s.InsertDeploymentRecord(ctx, rec); err != nil {
		t.Fatalf("insert: %v", err)
	}

	finished := base.Add(time.Minute)
	if err := s.UpdateDeploymentStatus(ctx, rec.DeploymentID, DeployStatusSucceeded, finished); err != nil {
		t.Fatalf("UpdateDeploymentStatus(SUCCEEDED): %v", err)
	}

	// Journal: the single call updated the record.
	got, err := s.GetLatestDeploymentForWorker(ctx, rec.WorkerID)
	if err != nil {
		t.Fatalf("GetLatestDeploymentForWorker: %v", err)
	}
	if got.Status != DeployStatusSucceeded {
		t.Errorf("journal Status = %q, want SUCCEEDED", got.Status)
	}
	if got.FinishedAt == nil || !got.FinishedAt.Equal(finished) {
		t.Errorf("journal FinishedAt = %v, want %v", got.FinishedAt, finished)
	}

	// Read model: the SAME call projected it, with no reconstruction needed.
	state, err := s.GetWorkerDeploymentState(ctx, rec.WorkerID)
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.LastOperationID != rec.DeploymentID {
		t.Errorf("projection LastOperationID = %q, want %q", state.LastOperationID, rec.DeploymentID)
	}
	if state.LastOperationStatus != DeployStatusSucceeded {
		t.Errorf("projection LastOperationStatus = %q, want SUCCEEDED", state.LastOperationStatus)
	}
	if state.LastOperationErrorCode != "" || state.LastOperationError != "" {
		t.Errorf("projection error = code=%q msg=%q, want empty on a clean success", state.LastOperationErrorCode, state.LastOperationError)
	}
}

// TestDeploymentStore_FailedCannotResurrectToSucceeded pins the mirror case:
// a FAILED rollout is terminal and cannot be flipped to SUCCEEDED by a late
// or duplicate completion report.
func TestDeploymentStore_FailedCannotResurrectToSucceeded(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)

	rec := DeploymentRecord{
		DeploymentID:   "deploy-failed-term",
		WorkerID:       "wicket",
		PreviousDigest: deploymentTestDigest('a'),
		TargetDigest:   deploymentTestDigest('b'),
		StartedAt:      base,
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}
	if err := s.InsertDeploymentRecord(ctx, rec); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := s.UpdateDeploymentStatus(ctx, rec.DeploymentID, DeployStatusFailed, base.Add(time.Minute)); err != nil {
		t.Fatalf("PENDING -> FAILED: %v", err)
	}

	err := s.UpdateDeploymentStatus(ctx, rec.DeploymentID, DeployStatusSucceeded, base.Add(2*time.Minute))
	if !errors.Is(err, ErrIllegalDeploymentTransition) {
		t.Fatalf("FAILED -> SUCCEEDED error = %v, want ErrIllegalDeploymentTransition", err)
	}
	// The rollback marker path is equally barred: a FAILED forward row must
	// not be re-labelled ROLLED_BACK.
	if err := s.MarkDeploymentRolledBack(ctx, rec.DeploymentID, base.Add(2*time.Minute), true, ""); !errors.Is(err, ErrIllegalDeploymentTransition) {
		t.Fatalf("FAILED -> ROLLED_BACK error = %v, want ErrIllegalDeploymentTransition", err)
	}

	got, err := s.GetLatestDeploymentForWorker(ctx, rec.WorkerID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != DeployStatusFailed {
		t.Errorf("Status after rejected resurrections = %q, want FAILED", got.Status)
	}
}

// TestDeploymentStore_RolledBackIsTerminal pins the third terminal state:
// ROLLED_BACK rows are immutable like SUCCEEDED/FAILED — a rollback cascade
// that completed can never be flipped back to SUCCEEDED.
func TestDeploymentStore_RolledBackIsTerminal(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)

	rec := DeploymentRecord{
		DeploymentID:   "deploy-rollback-term",
		WorkerID:       "wicket",
		PreviousDigest: deploymentTestDigest('a'),
		TargetDigest:   deploymentTestDigest('a'), // rollback restores previous
		StartedAt:      base,
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
		IsRollback:     true,
	}
	if err := s.InsertDeploymentRecord(ctx, rec); err != nil {
		t.Fatalf("insert rollback row: %v", err)
	}
	if err := s.MarkDeploymentRolledBack(ctx, rec.DeploymentID, base.Add(time.Minute), true, ""); err != nil {
		t.Fatalf("PENDING -> ROLLED_BACK: %v", err)
	}

	err := s.UpdateDeploymentStatus(ctx, rec.DeploymentID, DeployStatusSucceeded, base.Add(2*time.Minute))
	if !errors.Is(err, ErrIllegalDeploymentTransition) {
		t.Fatalf("ROLLED_BACK -> SUCCEEDED error = %v, want ErrIllegalDeploymentTransition", err)
	}

	got, err := s.GetLatestDeploymentForWorker(ctx, rec.WorkerID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != DeployStatusRolledBack || !got.IsRollback {
		t.Errorf("row after rejected transition = %s/is_rollback=%v, want ROLLED_BACK/is_rollback=true", got.Status, got.IsRollback)
	}
}

// TestDeploymentStore_RollbackFailedIsTerminal pins the rollback-also-
// failed terminal: MarkDeploymentRolledBack(rollbackOK=false) lands on
// PENDING → FAILED with is_rollback=1, and that row is then immutable like
// every other terminal row.
func TestDeploymentStore_RollbackFailedIsTerminal(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)

	rec := DeploymentRecord{
		DeploymentID:   "deploy-rollback-failed",
		WorkerID:       "wicket",
		PreviousDigest: deploymentTestDigest('a'),
		TargetDigest:   deploymentTestDigest('a'), // rollback restores previous
		StartedAt:      base,
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
		IsRollback:     true,
	}
	if err := s.InsertDeploymentRecord(ctx, rec); err != nil {
		t.Fatalf("insert rollback row: %v", err)
	}
	if err := s.MarkDeploymentRolledBack(ctx, rec.DeploymentID, base.Add(time.Minute), false, "ROLLBACK_FAILED"); err != nil {
		t.Fatalf("MarkDeploymentRolledBack(false): %v", err)
	}

	got, err := s.GetLatestDeploymentForWorker(ctx, rec.WorkerID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != DeployStatusFailed || !got.IsRollback {
		t.Errorf("row = %s/is_rollback=%v, want FAILED/is_rollback=true (rollback also failed)", got.Status, got.IsRollback)
	}

	// The rollback-failed row is terminal: it can be neither revived to
	// SUCCEEDED nor re-labelled ROLLED_BACK.
	err = s.UpdateDeploymentStatus(ctx, rec.DeploymentID, DeployStatusSucceeded, base.Add(2*time.Minute))
	if !errors.Is(err, ErrIllegalDeploymentTransition) {
		t.Fatalf("FAILED -> SUCCEEDED error = %v, want ErrIllegalDeploymentTransition", err)
	}
	err = s.MarkDeploymentRolledBack(ctx, rec.DeploymentID, base.Add(2*time.Minute), true, "")
	if !errors.Is(err, ErrIllegalDeploymentTransition) {
		t.Fatalf("FAILED -> ROLLED_BACK error = %v, want ErrIllegalDeploymentTransition", err)
	}

	state, err := s.GetWorkerDeploymentState(ctx, rec.WorkerID)
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.LastOperationStatus != DeployStatusFailed {
		t.Errorf("LastOperationStatus = %q, want FAILED (projection stays with the terminal row)", state.LastOperationStatus)
	}
}

// TestDeploymentStore_ProjectionFailureRollsBackTransition pins the atomic
// journal + read-model contract: if the worker_deployment_state projection
// write fails inside the transition transaction, the deployment_records
// UPDATE must roll back too — never a torn (ledger=SUCCEEDED,
// projection=stale) state. The failure is forced with a SQLite trigger that
// aborts any write to the read model.
func TestDeploymentStore_ProjectionFailureRollsBackTransition(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)

	rec := DeploymentRecord{
		DeploymentID:   "deploy-atomic-tx",
		WorkerID:       "wicket",
		PreviousDigest: deploymentTestDigest('a'),
		TargetDigest:   deploymentTestDigest('b'),
		StartedAt:      base,
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}
	if err := s.InsertDeploymentRecord(ctx, rec); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Force the projection write to fail AFTER the journal UPDATE: the
	// BEFORE INSERT trigger fires for the upsert's candidate row (including
	// the ON CONFLICT DO UPDATE path) and aborts the whole statement.
	if _, err := s.db.ExecContext(ctx, `
CREATE TRIGGER fail_projection
BEFORE INSERT ON worker_deployment_state
BEGIN
  SELECT RAISE(ABORT, 'forced projection failure');
END;`); err != nil {
		t.Fatalf("create projection-failure trigger: %v", err)
	}

	err := s.UpdateDeploymentStatus(ctx, rec.DeploymentID, DeployStatusSucceeded, base.Add(time.Minute))
	if err == nil {
		t.Fatal("UpdateDeploymentStatus = nil, want projection failure to abort the transition")
	}

	got, err := s.GetLatestDeploymentForWorker(ctx, rec.WorkerID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != DeployStatusPending {
		t.Errorf("Status = %q, want PENDING (journal UPDATE must roll back with the failed projection)", got.Status)
	}

	// The read model must be untouched by the rolled-back transition.
	state, err := s.GetWorkerDeploymentState(ctx, rec.WorkerID)
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.LastOperationStatus != DeployStatusPending {
		t.Errorf("LastOperationStatus = %q, want PENDING (projection must stay with the pre-transition row)", state.LastOperationStatus)
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
	if err := s.updateDeploymentTerminal(ctx, "deploy-state-failed", DeployStatusFailed, base.Add(2*time.Minute), "COSIGN_FAILED", "cosign verify failed", false); err != nil {
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

	// A rollout to digest 'c' verified through MarkVerifiedSucceeded — the
	// VERIFYING_DIGEST path: it must NOT rewrite running_digest (still 'b'
	// until the next heartbeat) but MUST advance last_successful_digest.
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
	if err := s.MarkVerifiedSucceeded(ctx, "deploy-state-c", deploymentTestDigest('c'), base.Add(2*time.Minute)); err != nil {
		t.Fatalf("MarkVerifiedSucceeded: %v", err)
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

// TestWorkerDeploymentState_GenericSucceededDoesNotAdvance pins the
// VERIFYING_DIGEST enforcement: the generic UpdateDeploymentStatus(SUCCEEDED)
// path — which carries NO digest verification — must NOT advance
// last_successful_digest. Only MarkVerifiedSucceeded (after an authenticated
// digest match) can make a new digest the last-known-good one.
func TestWorkerDeploymentState_GenericSucceededDoesNotAdvance(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)
	digestA := deploymentTestDigest('a')

	// Baseline verified success to A establishes last-known-good.
	if err := s.InsertBaselineDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID: "deploy-generic-base",
		WorkerID:     "wicket",
		TargetDigest: digestA,
		StartedAt:    base,
		FinishedAt:   deploymentTimePtr(base),
		Status:       DeployStatusSucceeded,
		AppliedBy:    "bootstrap",
	}); err != nil {
		t.Fatalf("InsertBaselineDeploymentRecord: %v", err)
	}

	// Unverified generic SUCCEEDED to B must leave last-known-good at A.
	if err := s.InsertDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID:   "deploy-generic-b",
		WorkerID:       "wicket",
		PreviousDigest: digestA,
		TargetDigest:   deploymentTestDigest('b'),
		StartedAt:      base.Add(time.Minute),
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}); err != nil {
		t.Fatalf("InsertDeploymentRecord: %v", err)
	}
	if err := s.UpdateDeploymentStatus(ctx, "deploy-generic-b", DeployStatusSucceeded, base.Add(2*time.Minute)); err != nil {
		t.Fatalf("UpdateDeploymentStatus(SUCCEEDED): %v", err)
	}

	state, err := s.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.LastSuccessfulDigest != digestA {
		t.Errorf("LastSuccessfulDigest = %q, want %q (unverified SUCCEEDED must not advance last-known-good)", state.LastSuccessfulDigest, digestA)
	}
	if state.LastOperationStatus != DeployStatusSucceeded {
		t.Errorf("LastOperationStatus = %q, want SUCCEEDED (the row IS succeeded, only last-known-good is gated)", state.LastOperationStatus)
	}
}

// TestWorkerDeploymentState_VerifiedMismatchRejected pins the digest gate in
// MarkVerifiedSucceeded: an observed digest != target is rejected with
// ErrDeploymentDigestMismatch, the row stays PENDING, last_successful_digest
// is untouched, and running_digest is untouched (the mismatch must not be
// "fixed" by copying observed into the read model).
func TestWorkerDeploymentState_VerifiedMismatchRejected(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)
	digestA := deploymentTestDigest('a')
	digestB := deploymentTestDigest('b')

	if err := s.InsertBaselineDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID: "deploy-mismatch-base",
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
		DeploymentID:   "deploy-mismatch-b",
		WorkerID:       "wicket",
		PreviousDigest: digestA,
		TargetDigest:   digestB,
		StartedAt:      base.Add(time.Minute),
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}); err != nil {
		t.Fatalf("InsertDeploymentRecord: %v", err)
	}
	// Heartbeat observed the worker actually running C (drift).
	if err := upsertWorkerRunningDigest(ctx, s.db, "wicket", deploymentTestDigest('c'), base.Add(90*time.Second)); err != nil {
		t.Fatalf("upsertWorkerRunningDigest: %v", err)
	}

	err := s.MarkVerifiedSucceeded(ctx, "deploy-mismatch-b", deploymentTestDigest('c'), base.Add(2*time.Minute))
	if !errors.Is(err, ErrDeploymentDigestMismatch) {
		t.Fatalf("MarkVerifiedSucceeded(C) err = %v, want ErrDeploymentDigestMismatch", err)
	}
	if !strings.Contains(err.Error(), "expected=") || !strings.Contains(err.Error(), "observed=") {
		t.Errorf("mismatch error must carry expected/observed; got %v", err)
	}

	rec, err := s.GetLatestDeploymentForWorker(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetLatestDeploymentForWorker: %v", err)
	}
	if rec.Status != DeployStatusPending {
		t.Fatalf("row status = %q, want PENDING (mismatch applies no transition)", rec.Status)
	}
	state, err := s.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.LastSuccessfulDigest != digestA {
		t.Errorf("LastSuccessfulDigest = %q, want %q (mismatch must not advance last-known-good)", state.LastSuccessfulDigest, digestA)
	}
	if state.RunningDigest != deploymentTestDigest('c') {
		t.Errorf("RunningDigest = %q, want %q (the observation stays exactly as the heartbeat wrote it)", state.RunningDigest, deploymentTestDigest('c'))
	}
}

// TestWorkerDeploymentState_PhaseRecordedAndPreserved pins migration 152:
// RecordDeploymentPhase writes the in-flight phase into the read model, the
// phase survives subsequent record transitions (never blanked), and it is
// orthogonal to digest state.
func TestWorkerDeploymentState_PhaseRecordedAndPreserved(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)

	if err := s.RecordDeploymentPhase(ctx, "wicket", "DRAINING"); err != nil {
		t.Fatalf("RecordDeploymentPhase(DRAINING): %v", err)
	}
	state, err := s.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.LastPhase != "DRAINING" {
		t.Fatalf("LastPhase = %q, want DRAINING", state.LastPhase)
	}

	// A PENDING insert (control-plane intent) must preserve the recorded
	// phase and fill desired without touching it.
	if err := s.InsertDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID:   "deploy-phase-1",
		WorkerID:       "wicket",
		PreviousDigest: deploymentTestDigest('a'),
		TargetDigest:   deploymentTestDigest('b'),
		StartedAt:      base,
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}); err != nil {
		t.Fatalf("InsertDeploymentRecord: %v", err)
	}
	if err := s.RecordDeploymentPhase(ctx, "wicket", "VERIFYING_DIGEST"); err != nil {
		t.Fatalf("RecordDeploymentPhase(VERIFYING_DIGEST): %v", err)
	}
	// Terminal transition (FAILED) preserves the last phase: the operator can
	// see WHERE the rollout stopped.
	if err := s.updateDeploymentTerminal(ctx, "deploy-phase-1", DeployStatusFailed, base.Add(time.Minute), "DIGEST_MISMATCH", "digest_mismatch: expected=B observed=C", false); err != nil {
		t.Fatalf("updateDeploymentTerminal(FAILED): %v", err)
	}

	state, err = s.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.LastPhase != "VERIFYING_DIGEST" {
		t.Errorf("LastPhase after FAILED transition = %q, want VERIFYING_DIGEST (where the rollout stopped)", state.LastPhase)
	}
	if state.LastOperationStatus != DeployStatusFailed || state.LastOperationError != "digest_mismatch: expected=B observed=C" {
		t.Errorf("last operation = %s/%q, want FAILED/digest_mismatch...", state.LastOperationStatus, state.LastOperationError)
	}
	if state.DesiredDigest != deploymentTestDigest('b') {
		t.Errorf("DesiredDigest = %q, want %q (phase recording must not touch intent)", state.DesiredDigest, deploymentTestDigest('b'))
	}
}

// ============================================================
// error_code / error_message separation (migration 153)
// ============================================================

// TestWorkerDeploymentState_ErrorCodeAndMessagePersisted pins migration 153
// end-to-end through the repository adapter (the path the fleet executor
// uses): MarkFailed(code, msg) writes BOTH the stable code and the
// human-readable message to the journal row AND projects them into the read
// model's last_operation_error_code / last_operation_error.
func TestWorkerDeploymentState_ErrorCodeAndMessagePersisted(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)

	if err := s.InsertDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID:   "deploy-code-fail",
		WorkerID:       "wicket",
		PreviousDigest: deploymentTestDigest('a'),
		TargetDigest:   deploymentTestDigest('b'),
		StartedAt:      base,
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}); err != nil {
		t.Fatalf("InsertDeploymentRecord: %v", err)
	}
	repo := NewDeploymentRecordRepository(s)
	if err := repo.MarkFailed(ctx, "deploy-code-fail", base.Add(time.Minute), "DIGEST_MISMATCH", "digest_mismatch: expected=sha256:b observed=sha256:c"); err != nil {
		t.Fatalf("repo.MarkFailed: %v", err)
	}

	// Journal row carries both, in separate columns.
	rec, err := s.GetLatestDeploymentForWorker(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetLatestDeploymentForWorker: %v", err)
	}
	if rec.ErrorCode != "DIGEST_MISMATCH" {
		t.Errorf("journal ErrorCode = %q, want DIGEST_MISMATCH", rec.ErrorCode)
	}
	if rec.ErrorMessage != "digest_mismatch: expected=sha256:b observed=sha256:c" {
		t.Errorf("journal ErrorMessage = %q, want the full message", rec.ErrorMessage)
	}

	// Read model projects both, still separate.
	state, err := s.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.LastOperationErrorCode != "DIGEST_MISMATCH" {
		t.Errorf("read model LastOperationErrorCode = %q, want DIGEST_MISMATCH", state.LastOperationErrorCode)
	}
	if state.LastOperationError != "digest_mismatch: expected=sha256:b observed=sha256:c" {
		t.Errorf("read model LastOperationError = %q, want the full message", state.LastOperationError)
	}
}

// TestWorkerDeploymentState_NewOperationClearsErrorPreservesHistory pins the
// "new operation clears the current error but preserves history" contract:
// after op#1 FAILED with DIGEST_MISMATCH, inserting op#2 (PENDING) blanks
// last_operation_error_code / last_operation_error in the read model, while
// the journal row of op#1 keeps its code+message forever (audit history is
// never rewritten).
func TestWorkerDeploymentState_NewOperationClearsErrorPreservesHistory(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)
	digestA := deploymentTestDigest('a')
	digestB := deploymentTestDigest('b')
	repo := NewDeploymentRecordRepository(s)

	if err := s.InsertBaselineDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID: "deploy-clear-base",
		WorkerID:     "wicket",
		TargetDigest: digestA,
		StartedAt:    base,
		FinishedAt:   deploymentTimePtr(base),
		Status:       DeployStatusSucceeded,
		AppliedBy:    "bootstrap",
	}); err != nil {
		t.Fatalf("InsertBaselineDeploymentRecord: %v", err)
	}
	// op#1: FAILED with DIGEST_MISMATCH.
	if err := s.InsertDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID:   "deploy-clear-op1",
		WorkerID:       "wicket",
		PreviousDigest: digestA,
		TargetDigest:   digestB,
		StartedAt:      base.Add(time.Minute),
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}); err != nil {
		t.Fatalf("insert op1: %v", err)
	}
	if err := repo.MarkFailed(ctx, "deploy-clear-op1", base.Add(2*time.Minute), "DIGEST_MISMATCH", "digest_mismatch: expected=sha256:b observed=sha256:c"); err != nil {
		t.Fatalf("MarkFailed op1: %v", err)
	}

	state, err := s.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.LastOperationErrorCode != "DIGEST_MISMATCH" || state.LastOperationError == "" {
		t.Fatalf("pre-condition: read model error = %q/%q, want DIGEST_MISMATCH/msg", state.LastOperationErrorCode, state.LastOperationError)
	}

	// op#2: a NEW operation starts (PENDING). The read model's current error
	// must be cleared — the failure of op#1 is no longer the CURRENT error.
	if err := s.InsertDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID:   "deploy-clear-op2",
		WorkerID:       "wicket",
		PreviousDigest: digestB,
		TargetDigest:   deploymentTestDigest('c'),
		StartedAt:      base.Add(3 * time.Minute),
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}); err != nil {
		t.Fatalf("insert op2: %v", err)
	}

	state, err = s.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.LastOperationErrorCode != "" {
		t.Errorf("LastOperationErrorCode after new op = %q, want empty (current error cleared)", state.LastOperationErrorCode)
	}
	if state.LastOperationError != "" {
		t.Errorf("LastOperationError after new op = %q, want empty (current error cleared)", state.LastOperationError)
	}
	if state.LastOperationID != "deploy-clear-op2" {
		t.Errorf("LastOperationID = %q, want deploy-clear-op2", state.LastOperationID)
	}

	// History is preserved: op#1's journal row still carries code+message.
	op1, err := s.getDeploymentRecord(ctx, "deploy-clear-op1")
	if err != nil {
		t.Fatalf("get op1 journal row: %v", err)
	}
	if op1.ErrorCode != "DIGEST_MISMATCH" || op1.ErrorMessage != "digest_mismatch: expected=sha256:b observed=sha256:c" {
		t.Errorf("op1 history = code=%q msg=%q, want preserved DIGEST_MISMATCH/msg", op1.ErrorCode, op1.ErrorMessage)
	}
	if op1.Status != DeployStatusFailed {
		t.Errorf("op1 history status = %q, want FAILED (history is immutable)", op1.Status)
	}
}

// TestWorkerDeploymentState_VerifiedSuccessClearsErrorCode pins the
// successful-terminal write: MarkVerifiedSucceeded clears both the code and
// the message from the journal row AND the read model — a later rollout that
// succeeds must not leave the previous DIGEST_MISMATCH visible as the
// current error.
func TestWorkerDeploymentState_VerifiedSuccessClearsErrorCode(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)
	digestA := deploymentTestDigest('a')
	digestB := deploymentTestDigest('b')
	repo := NewDeploymentRecordRepository(s)

	if err := s.InsertBaselineDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID: "deploy-clear-ok-base",
		WorkerID:     "wicket",
		TargetDigest: digestA,
		StartedAt:    base,
		FinishedAt:   deploymentTimePtr(base),
		Status:       DeployStatusSucceeded,
		AppliedBy:    "bootstrap",
	}); err != nil {
		t.Fatalf("InsertBaselineDeploymentRecord: %v", err)
	}
	// Failed op#1 leaves DIGEST_MISMATCH on the read model.
	if err := s.InsertDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID:   "deploy-clear-ok-fail",
		WorkerID:       "wicket",
		PreviousDigest: digestA,
		TargetDigest:   digestB,
		StartedAt:      base.Add(time.Minute),
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}); err != nil {
		t.Fatalf("insert failed op: %v", err)
	}
	if err := repo.MarkFailed(ctx, "deploy-clear-ok-fail", base.Add(2*time.Minute), "DIGEST_MISMATCH", "digest_mismatch: expected=sha256:b observed=sha256:c"); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	// Retry op#2 now verifies successfully.
	if err := s.InsertDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID:   "deploy-clear-ok-success",
		WorkerID:       "wicket",
		PreviousDigest: digestB,
		TargetDigest:   digestB,
		StartedAt:      base.Add(3 * time.Minute),
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}); err != nil {
		t.Fatalf("insert success op: %v", err)
	}
	if err := s.MarkVerifiedSucceeded(ctx, "deploy-clear-ok-success", digestB, base.Add(4*time.Minute)); err != nil {
		t.Fatalf("MarkVerifiedSucceeded: %v", err)
	}

	state, err := s.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.LastOperationErrorCode != "" || state.LastOperationError != "" {
		t.Errorf("read model error after verified success = %q/%q, want cleared", state.LastOperationErrorCode, state.LastOperationError)
	}
	if state.LastOperationStatus != DeployStatusSucceeded {
		t.Errorf("LastOperationStatus = %q, want SUCCEEDED", state.LastOperationStatus)
	}
	// The successful journal row carries no error; the FAILED op#1 row keeps
	// its DIGEST_MISMATCH history.
	succ, err := s.getDeploymentRecord(ctx, "deploy-clear-ok-success")
	if err != nil {
		t.Fatalf("get success row: %v", err)
	}
	if succ.ErrorCode != "" || succ.ErrorMessage != "" {
		t.Errorf("success journal row error = %q/%q, want cleared", succ.ErrorCode, succ.ErrorMessage)
	}
	fail, err := s.getDeploymentRecord(ctx, "deploy-clear-ok-fail")
	if err != nil {
		t.Fatalf("get failed row: %v", err)
	}
	if fail.ErrorCode != "DIGEST_MISMATCH" {
		t.Errorf("failed journal row ErrorCode = %q, want DIGEST_MISMATCH preserved in history", fail.ErrorCode)
	}
}

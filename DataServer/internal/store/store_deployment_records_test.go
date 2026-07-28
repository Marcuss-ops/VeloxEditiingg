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

package store

import (
	"context"
	"errors"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

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
func TestDeploymentStore_NotFound(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	_, err := s.GetLatestDeploymentForWorker(ctx, "no-such-worker")
	if !errors.Is(err, ErrDeploymentNotFound) {
		t.Errorf("err = %v, want ErrDeploymentNotFound", err)
	}
}
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

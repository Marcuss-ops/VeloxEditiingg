package main

// bootstrap_modules_observability_test.go — anti-reconstruction gate for
// the observability worker reader.
//
// workerRegistryAdapter (bootstrap_modules.go) is the WorkerReader that
// feeds the observability REST API's worker view. Its target_digest field
// ("what the fleet wants") MUST come from the worker_deployment_state read
// model (desired_digest), NEVER from a reconstruction over the
// deployment_records journal. A journal reconstruction would LIE about
// current state after a newer FAILED rollout: the latest journal row still
// carries the FAILED target while the read model shows the true desired
// digest as drift.
//
// The fixture is deliberately divergent:
//
//	deployment_records (journal)        → latest row target=A SUCCEEDED
//	worker_deployment_state (read model) → desired=C running=B last_successful=B
//
// The adapter MUST return C. If a future change starts rebuilding
// target_digest from the journal, this test fails with A leaking through.

import (
	"context"
	"strings"
	"testing"
	"time"

	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

// seedDivergentDeploymentState writes the journal row (target A, SUCCEEDED)
// and then forces the read model to diverge (desired C / running B /
// last_successful B). Returns the workerID used.
func seedDivergentDeploymentState(t *testing.T, s *store.SQLiteStore, workerID string) {
	t.Helper()
	ctx := context.Background()
	digestA := "sha256:" + strings.Repeat("a", 64)
	digestB := "sha256:" + strings.Repeat("b", 64)
	digestC := "sha256:" + strings.Repeat("c", 64)

	// Journal: a SUCCEEDED baseline to A. This also seeds the read model
	// with desired=A — we force the divergence right after.
	started := time.Now().UTC().Truncate(time.Second)
	if err := s.InsertBaselineDeploymentRecord(ctx, store.DeploymentRecord{
		DeploymentID: "deploy-history-a",
		WorkerID:     workerID,
		TargetDigest: digestA,
		StartedAt:    started,
		FinishedAt:   &started,
		Status:       store.DeployStatusSucceeded,
		AppliedBy:    "bootstrap",
	}); err != nil {
		t.Fatalf("InsertBaselineDeploymentRecord: %v", err)
	}

	// Force the read model to the drift story: desired=C running=B
	// last_successful=B — nothing the journal ever said.
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := s.DB().Exec(`UPDATE worker_deployment_state
SET desired_digest=?, running_digest=?, last_successful_digest=?, updated_at=?
WHERE worker_id=?`,
		digestC, digestB, digestB, now, workerID); err != nil {
		t.Fatalf("force read-model divergence: %v", err)
	}
}

func TestObservabilityAdapter_TargetDigestFromReadModel(t *testing.T) {
	cfg := newTestConfig(t)
	p, err := buildPersistence(cfg)
	if err != nil {
		t.Fatalf("buildPersistence: %v", err)
	}
	t.Cleanup(func() { _ = p.SQLite.Close() })

	workerID := "velox-worker-13197"
	reg := workersreg.New(p.SQLite)
	if err := reg.RegisterWorker(context.Background(), workerID, "vps-velox-worker-13197", "10.0.0.7", nil); err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}
	seedDivergentDeploymentState(t, p.SQLite, workerID)

	adapter := &workerRegistryAdapter{reg: reg, store: p.SQLite}

	// GetWorker path.
	row, err := adapter.GetWorker(workerID)
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if row == nil {
		t.Fatal("GetWorker returned nil row")
	}
	if got := row["target_digest"].(string); got != "sha256:"+strings.Repeat("c", 64) {
		t.Fatalf("GetWorker target_digest = %q, want sha256:c (read model desired; journal says A)", got)
	}

	// ListWorkers path.
	rows, err := adapter.ListWorkers()
	if err != nil {
		t.Fatalf("ListWorkers: %v", err)
	}
	found := false
	for _, r := range rows {
		if r["worker_id"] == workerID {
			found = true
			if got := r["target_digest"].(string); got != "sha256:"+strings.Repeat("c", 64) {
				t.Fatalf("ListWorkers target_digest = %q, want sha256:c (read model desired; journal says A)", got)
			}
		}
	}
	if !found {
		t.Fatalf("ListWorkers did not return worker %s: %v", workerID, rows)
	}
}

func TestObservabilityAdapter_MissingReadModelLeavesTargetEmpty(t *testing.T) {
	cfg := newTestConfig(t)
	p, err := buildPersistence(cfg)
	if err != nil {
		t.Fatalf("buildPersistence: %v", err)
	}
	t.Cleanup(func() { _ = p.SQLite.Close() })

	workerID := "velox-worker-523925eb"
	reg := workersreg.New(p.SQLite)
	if err := reg.RegisterWorker(context.Background(), workerID, "vps-velox-worker-523925eb", "10.0.0.8", nil); err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}
	// Journal has history (SUCCEEDED to A) but NO read model row (the
	// worker predates migration 151). The adapter must leave target_digest
	// empty — UNKNOWN is honest, reconstruction from history is not.
	ctx := context.Background()
	started := time.Now().UTC().Truncate(time.Second)
	digestA := "sha256:" + strings.Repeat("a", 64)
	if err := p.SQLite.InsertBaselineDeploymentRecord(ctx, store.DeploymentRecord{
		DeploymentID: "deploy-history-a2",
		WorkerID:     workerID,
		TargetDigest: digestA,
		StartedAt:    started,
		FinishedAt:   &started,
		Status:       store.DeployStatusSucceeded,
		AppliedBy:    "bootstrap",
	}); err != nil {
		t.Fatalf("InsertBaselineDeploymentRecord: %v", err)
	}
	// Remove the read model row the insert created, simulating a
	// pre-migration-151 worker whose journal survived but whose projection
	// was never built.
	if _, err := p.SQLite.DB().Exec(`DELETE FROM worker_deployment_state WHERE worker_id=?`, workerID); err != nil {
		t.Fatalf("delete read model row: %v", err)
	}

	adapter := &workerRegistryAdapter{reg: reg, store: p.SQLite}
	row, err := adapter.GetWorker(workerID)
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if got := row["target_digest"].(string); got != "" {
		t.Fatalf("target_digest = %q, want empty (journal SUCCEEDED=A must NOT be backfilled into target)\n", got)
	}
}

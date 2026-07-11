package store

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/placement"
	"velox-server/internal/taskgraph"
)

func TestSQLiteTaskRepository_ListReadyCandidates_NormalizesVersionedExecutorID(t *testing.T) {
	r, db := openCandidatesTestDB(t)
	ctx := context.Background()

	seedCandidateTask(t, db, "T-versioned", "J-versioned", 10, "READY", false, "",
		"scene.composite.v1@1", 1, time.Date(2026, 7, 2, 18, 0, 0, 0, time.UTC))

	candidates, err := r.ListReadyCandidates(ctx, 10)
	if err != nil {
		t.Fatalf("ListReadyCandidates: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("len(candidates) = %d, want 1", len(candidates))
	}
	if got := candidates[0].Executor; got != (placement.ExecutorKey{ID: "scene.composite.v1", Version: 1}) {
		t.Fatalf("normalized executor = %+v, want {ID:scene.composite.v1 Version:1}", got)
	}
}

func TestClaimTaskForWorkerAtomic_AcceptsLegacyVersionedExecutorIDStorage(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()

	taskID := "T-legacy-versioned"
	seedReadyTaskWithExecutor(t, s.db, taskID, "scene.composite.v1@1", 1, 0)

	tws, att, err := r.ClaimTaskForWorkerAtomic(ctx, taskgraph.ClaimTaskForWorkerCommand{
		TaskID:               taskID,
		ExpectedTaskRevision: 0,
		WorkerID:             "worker-legacy-1",
		SessionID:            "sess-legacy-1",
		LeaseID:              "lease-legacy-1",
		ExecutorID:           "scene.composite.v1",
		ExecutorVersion:      1,
		CapabilityRevision:   1,
	})
	if err != nil {
		t.Fatalf("ClaimTaskForWorkerAtomic legacy executor id: %v", err)
	}
	if tws == nil || att == nil {
		t.Fatal("expected claimed task and attempt, got nil")
	}
	if got := placement.NormalizeExecutorKey(tws.ExecutorID, tws.ExecutorVersion); got != (placement.ExecutorKey{ID: "scene.composite.v1", Version: 1}) {
		t.Fatalf("claimed task executor = %+v, want canonical scene.composite.v1@1", got)
	}
}

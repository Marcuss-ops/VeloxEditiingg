package api

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

type digestDeploymentReader struct {
	record *store.DeploymentRecord
}

func (r digestDeploymentReader) GetLatestDeploymentForWorker(context.Context, string) (*store.DeploymentRecord, error) {
	return r.record, nil
}

func TestAdminWorkersCard_DigestStateUsesRuntimeMatch(t *testing.T) {
	finished := time.Date(2026, time.August, 11, 12, 0, 0, 0, time.UTC)
	info := makeCardInfo("velox-worker-13197", func(w *workersreg.Worker) {
		w.ImageDigest = "sha256:running"
	})
	h := NewAdminWorkersHandler(nil)
	h.SetDeploymentReader(digestDeploymentReader{record: &store.DeploymentRecord{
		DeploymentID: "deploy-old-failed",
		WorkerID:     "velox-worker-13197",
		TargetDigest: "sha256:running",
		Status:       store.DeployStatusFailed,
		StartedAt:    finished.Add(-time.Minute),
		FinishedAt:   &finished,
	}})

	card := h.card(context.Background(), &info)
	if card.RunningDigest != "sha256:running" {
		t.Fatalf("RunningDigest = %q, want sha256:running", card.RunningDigest)
	}
	if card.TargetDigest != "sha256:running" {
		t.Fatalf("TargetDigest = %q, want sha256:running", card.TargetDigest)
	}
	if card.DigestState != "MATCH" {
		t.Fatalf("DigestState = %q, want MATCH despite old failed operation", card.DigestState)
	}
	if card.DigestMatch == nil || !*card.DigestMatch {
		t.Fatalf("DigestMatch = %#v, want true", card.DigestMatch)
	}
	if card.LastUpdateOperation == nil || card.LastUpdateOperation.Status != store.DeployStatusFailed {
		t.Fatalf("LastUpdateOperation = %#v, want FAILED history", card.LastUpdateOperation)
	}
}

func TestAdminWorkersCard_DigestStateSeparatesMismatchFromOperation(t *testing.T) {
	info := makeCardInfo("velox-worker-523925eb", func(w *workersreg.Worker) {
		w.ImageDigest = "sha256:old"
	})
	h := NewAdminWorkersHandler(nil)
	h.SetDeploymentReader(digestDeploymentReader{record: &store.DeploymentRecord{
		DeploymentID: "deploy-pending",
		WorkerID:     "velox-worker-523925eb",
		TargetDigest: "sha256:new",
		Status:       store.DeployStatusPending,
		StartedAt:    time.Date(2026, time.August, 11, 12, 0, 0, 0, time.UTC),
	}})

	card := h.card(context.Background(), &info)
	if card.DigestState != "MISMATCH" {
		t.Fatalf("DigestState = %q, want MISMATCH", card.DigestState)
	}
	if card.DigestMatch == nil || *card.DigestMatch {
		t.Fatalf("DigestMatch = %#v, want false", card.DigestMatch)
	}
	if card.LastUpdateOperation == nil || card.LastUpdateOperation.Status != store.DeployStatusPending {
		t.Fatalf("LastUpdateOperation = %#v, want PENDING history", card.LastUpdateOperation)
	}
}

package api

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

type digestDeploymentReader struct {
	record     *store.DeploymentRecord
	successful *store.DeploymentRecord
}

func (r digestDeploymentReader) GetLatestDeploymentForWorker(context.Context, string) (*store.DeploymentRecord, error) {
	return r.record, nil
}

func (r digestDeploymentReader) GetLatestSuccessfulDeploymentForWorker(context.Context, string) (*store.DeploymentRecord, error) {
	return r.successful, nil
}

func TestAdminWorkersCard_SeparatesDesiredRunningAndLastSuccessfulDigest(t *testing.T) {
	info := makeCardInfo("velox-worker-13197", func(w *workersreg.Worker) {
		w.ImageDigest = "sha256:old"
	})
	h := NewAdminWorkersHandler(nil)
	h.SetDeploymentReader(digestDeploymentReader{
		record: &store.DeploymentRecord{
			DeploymentID: "deploy-failed-new",
			WorkerID:     "velox-worker-13197",
			TargetDigest: "sha256:new",
			Status:       store.DeployStatusFailed,
			StartedAt:    time.Date(2026, time.August, 11, 12, 1, 0, 0, time.UTC),
		},
		successful: &store.DeploymentRecord{
			DeploymentID: "deploy-success-old",
			WorkerID:     "velox-worker-13197",
			TargetDigest: "sha256:old",
			Status:       store.DeployStatusSucceeded,
			StartedAt:    time.Date(2026, time.August, 11, 12, 0, 0, 0, time.UTC),
		},
	})

	card := h.card(context.Background(), &info)
	if card.RunningDigest != "sha256:old" || card.DesiredDigest != "sha256:new" || card.LastSuccessfulDigest != "sha256:old" {
		t.Fatalf("digest state = running=%q desired=%q last_successful=%q", card.RunningDigest, card.DesiredDigest, card.LastSuccessfulDigest)
	}
}

func TestAdminWorkersCard_ImageStateUsesRuntimeMatch(t *testing.T) {
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
	// IMAGE is the real-time state: running == target despite the old
	// failed operation → Match=true. The old FAILED rollout must not leak
	// into the image state.
	if card.ImageState == nil {
		t.Fatal("ImageState = nil, want populated")
	}
	if card.ImageState.RunningDigest != "sha256:running" || card.ImageState.TargetDigest != "sha256:running" {
		t.Fatalf("ImageState = %#v, want running==target==sha256:running", card.ImageState)
	}
	if !card.ImageState.Match {
		t.Fatalf("ImageState.Match = false, want true despite old failed operation")
	}
	// LAST UPDATE OPERATION keeps the history separate: FAILED + rollback
	// provenance, while the worker itself is on the target digest.
	if card.OperationState == nil {
		t.Fatal("OperationState = nil, want FAILED history")
	}
	if card.OperationState.Status != store.DeployStatusFailed {
		t.Fatalf("OperationState.Status = %q, want FAILED history", card.OperationState.Status)
	}
	if card.OperationState.OperationID != "deploy-old-failed" {
		t.Fatalf("OperationState.OperationID = %q, want deploy-old-failed", card.OperationState.OperationID)
	}
	if card.OperationState.Type != "update" {
		t.Fatalf("OperationState.Type = %q, want update", card.OperationState.Type)
	}
	if card.OperationState.StartedAt == "" || card.OperationState.FinishedAt == nil {
		t.Fatalf("OperationState timestamps missing: %#v", card.OperationState)
	}
}

func TestAdminWorkersCard_ImageStateSeparatesMismatchFromOperation(t *testing.T) {
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
	if card.ImageState == nil {
		t.Fatal("ImageState = nil, want populated")
	}
	if card.ImageState.Match {
		t.Fatalf("ImageState.Match = true, want false (running=old, target=new)")
	}
	if card.ImageState.RunningDigest != "sha256:old" || card.ImageState.TargetDigest != "sha256:new" {
		t.Fatalf("ImageState = %#v, want running=old target=new", card.ImageState)
	}
	if card.OperationState == nil || card.OperationState.Status != store.DeployStatusPending {
		t.Fatalf("OperationState = %#v, want PENDING history", card.OperationState)
	}
}

func TestAdminWorkersCard_OperationStateCarriesLedgerErrorReason(t *testing.T) {
	info := makeCardInfo("velox-worker-13197", func(w *workersreg.Worker) {
		w.ImageDigest = "sha256:running"
	})
	started := time.Date(2026, time.August, 11, 12, 0, 0, 0, time.UTC)
	h := NewAdminWorkersHandler(nil)
	h.SetDeploymentReader(digestDeploymentReader{record: &store.DeploymentRecord{
		DeploymentID: "deploy-failed",
		WorkerID:     "velox-worker-13197",
		TargetDigest: "sha256:running",
		Status:       store.DeployStatusFailed,
		StartedAt:    started,
	}})
	h.SetOperationLedgerReader(operationLedgerStub{rows: []store.Operation{{
		OperationID:  "op-1",
		WorkerID:     "velox-worker-13197",
		Op:           "update",
		Status:       store.OperationStatusFailed,
		ErrorMessage: "connection reset by peer",
	}}})

	card := h.card(context.Background(), &info)
	if card.OperationState == nil {
		t.Fatal("OperationState = nil, want failed operation history")
	}
	if card.OperationState.Error != "connection reset by peer" {
		t.Fatalf("OperationState.Error = %q, want connection reset by peer", card.OperationState.Error)
	}
	// The IMAGE view stays clean: running digest matches target digest.
	if card.ImageState == nil || !card.ImageState.Match {
		t.Fatalf("ImageState = %#v, want Match=true (worker is on target despite failed op)", card.ImageState)
	}
}

func TestAdminWorkersCard_OperationErrorSkipsNonUpdateRows(t *testing.T) {
	info := makeCardInfo("velox-worker-13197", func(w *workersreg.Worker) {
		w.ImageDigest = "sha256:running"
	})
	started := time.Date(2026, time.August, 11, 12, 0, 0, 0, time.UTC)
	h := NewAdminWorkersHandler(nil)
	h.SetDeploymentReader(digestDeploymentReader{record: &store.DeploymentRecord{
		DeploymentID: "deploy-failed",
		WorkerID:     "velox-worker-13197",
		TargetDigest: "sha256:running",
		Status:       store.DeployStatusFailed,
		StartedAt:    started,
	}})
	// Newest ledger rows are a FAILED smoke and a FAILED resume — neither
	// may leak under LAST UPDATE OPERATION; the older update row carries
	// the real reason.
	h.SetOperationLedgerReader(operationLedgerStub{rows: []store.Operation{
		{OperationID: "op-smoke", WorkerID: "velox-worker-13197", Op: "smoke", Status: store.OperationStatusFailed, ErrorMessage: "smoke probe failed"},
		{OperationID: "op-resume", WorkerID: "velox-worker-13197", Op: "resume", Status: store.OperationStatusFailed, ErrorMessage: "resume failed"},
		{OperationID: "op-update", WorkerID: "velox-worker-13197", Op: "update", Status: store.OperationStatusFailed, ErrorMessage: "connection reset by peer"},
	}})

	card := h.card(context.Background(), &info)
	if card.OperationState == nil {
		t.Fatal("OperationState = nil, want failed update history")
	}
	if card.OperationState.Error != "connection reset by peer" {
		t.Fatalf("OperationState.Error = %q, want connection reset by peer (non-update rows must be skipped)", card.OperationState.Error)
	}
}

func TestAdminWorkersCard_SucceededDeploymentOmitsError(t *testing.T) {
	info := makeCardInfo("velox-worker-13197", func(w *workersreg.Worker) {
		w.ImageDigest = "sha256:running"
	})
	started := time.Date(2026, time.August, 11, 12, 0, 0, 0, time.UTC)
	h := NewAdminWorkersHandler(nil)
	h.SetDeploymentReader(digestDeploymentReader{record: &store.DeploymentRecord{
		DeploymentID: "deploy-ok",
		WorkerID:     "velox-worker-13197",
		TargetDigest: "sha256:running",
		Status:       store.DeployStatusSucceeded,
		StartedAt:    started,
	}})
	// A stale FAILED smoke in the ledger must not attach an error to a
	// SUCCEEDED update.
	h.SetOperationLedgerReader(operationLedgerStub{rows: []store.Operation{
		{OperationID: "op-smoke", WorkerID: "velox-worker-13197", Op: "smoke", Status: store.OperationStatusFailed, ErrorMessage: "smoke probe failed"},
	}})

	card := h.card(context.Background(), &info)
	if card.OperationState == nil || card.OperationState.Status != store.DeployStatusSucceeded {
		t.Fatalf("OperationState = %#v, want SUCCEEDED", card.OperationState)
	}
	if card.OperationState.Error != "" {
		t.Fatalf("OperationState.Error = %q, want empty for a SUCCEEDED update", card.OperationState.Error)
	}
}

func TestAdminWorkersCard_NoLedgerOmitsErrorWithoutBreakingImage(t *testing.T) {
	info := makeCardInfo("velox-worker-13197", func(w *workersreg.Worker) {
		w.ImageDigest = "sha256:running"
	})
	h := NewAdminWorkersHandler(nil)
	h.SetDeploymentReader(digestDeploymentReader{record: &store.DeploymentRecord{
		DeploymentID: "deploy-failed",
		WorkerID:     "velox-worker-13197",
		TargetDigest: "sha256:running",
		Status:       store.DeployStatusFailed,
		StartedAt:    time.Date(2026, time.August, 11, 12, 0, 0, 0, time.UTC),
	}})
	// No operations reader wired: Error must be omitted, never fabricated.
	card := h.card(context.Background(), &info)
	if card.OperationState == nil || card.OperationState.Error != "" {
		t.Fatalf("OperationState = %#v, want Error omitted", card.OperationState)
	}
	if card.ImageState == nil || !card.ImageState.Match {
		t.Fatalf("ImageState = %#v, want Match=true", card.ImageState)
	}
}

// operationLedgerStub is an in-memory OperationLedgerReader for the
// error-reason enrichment test.
type operationLedgerStub struct {
	rows []store.Operation
}

func (s operationLedgerStub) ListOperations(context.Context, string, string, int) ([]store.Operation, error) {
	return s.rows, nil
}

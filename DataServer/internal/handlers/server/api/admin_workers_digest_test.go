package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

type digestDeploymentReader struct {
	record     *store.DeploymentRecord
	successful *store.DeploymentRecord
}

type failingDeploymentReader struct{}

func (failingDeploymentReader) GetLatestDeploymentForWorker(context.Context, string) (*store.DeploymentRecord, error) {
	return nil, errors.New("sqlite unavailable")
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

func TestAdminWorkersCard_DeploymentProjectionErrorFailsClosed(t *testing.T) {
	info := makeCardInfo("velox-worker-13197")
	h := NewAdminWorkersHandler(nil)
	h.SetDeploymentReader(failingDeploymentReader{})

	if _, err := h.cardWithError(context.Background(), &info); err == nil {
		t.Fatal("deployment projection error was swallowed")
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

// stateReaderStub is an in-memory WorkerDeploymentStateReader for the
// read-model vs journal tests.
type stateReaderStub struct {
	state *store.WorkerDeploymentState
	err   error
}

func (s stateReaderStub) GetWorkerDeploymentState(context.Context, string) (*store.WorkerDeploymentState, error) {
	return s.state, s.err
}

// divergentReadModelFixture returns the worker_deployment_state row the
// test DB is built to hold: desired=C, running=B, last_successful=B —
// deliberately different from anything the deployment_records journal
// suggests. This is the exact drift scenario (DESIRED C / ACTUAL B) the
// read model exists to keep visible.
func divergentReadModelFixture() *store.WorkerDeploymentState {
	return &store.WorkerDeploymentState{
		WorkerID:             "velox-worker-13197",
		DesiredDigest:        "sha256:C",
		RunningDigest:        "sha256:B",
		LastSuccessfulDigest: "sha256:B",
		LastOperationID:      "deploy-read-model",
		LastOperationKind:    "update",
		LastOperationStatus:  store.DeployStatusFailed,
		UpdatedAt:            time.Date(2026, time.August, 12, 9, 0, 0, 0, time.UTC),
	}
}

// journalSuggestingDigestA returns a deployment_records view that, if the
// API ever reconstructed current state from history, would claim digest A
// (latest row target=A SUCCEEDED and no other journal evidence).
func journalSuggestingDigestA() digestDeploymentReader {
	started := time.Date(2026, time.August, 12, 8, 0, 0, 0, time.UTC)
	return digestDeploymentReader{
		record: &store.DeploymentRecord{
			DeploymentID: "deploy-history-a",
			WorkerID:     "velox-worker-13197",
			TargetDigest: "sha256:A",
			Status:       store.DeployStatusSucceeded,
			StartedAt:    started,
			FinishedAt:   &started,
		},
		successful: &store.DeploymentRecord{
			DeploymentID: "deploy-history-a",
			WorkerID:     "velox-worker-13197",
			TargetDigest: "sha256:A",
			Status:       store.DeployStatusSucceeded,
			StartedAt:    started,
		},
	}
}

// TestAdminWorkersCard_ReadModelWinsOverReconstructedHistory is the
// anti-reconstruction gate for the Fleet persistent-state block.
//
// The DB intentionally holds a divergent story:
//
//	deployment_records (journal)   → latest row target=A SUCCEEDED
//	worker_deployment_state (read model) → desired=C running=B last_successful=B
//
// The API MUST return exactly the read model. If a future change starts
// rebuilding current state from the journal, this test fails with digest
// A leaking into desired/last_successful.
func TestAdminWorkersCard_ReadModelWinsOverReconstructedHistory(t *testing.T) {
	info := makeCardInfo("velox-worker-13197", func(w *workersreg.Worker) {
		w.ImageDigest = "sha256:registry-stale"
	})
	h := NewAdminWorkersHandler(nil)
	h.SetWorkerDeploymentStateReader(stateReaderStub{state: divergentReadModelFixture()})
	// History that would suggest digest A if the API reconstructed current
	// state from deployment_records.
	h.SetDeploymentReader(journalSuggestingDigestA())

	card := h.card(context.Background(), &info)

	if card.DesiredDigest != "sha256:C" || card.TargetDigest != "sha256:C" {
		t.Fatalf("desired/target reconstructed from history: desired=%q target=%q, want sha256:C (read model)", card.DesiredDigest, card.TargetDigest)
	}
	if card.RunningDigest != "sha256:B" {
		t.Fatalf("RunningDigest = %q, want sha256:B (read model, not the stale registry heartbeat)", card.RunningDigest)
	}
	if card.LastSuccessfulDigest != "sha256:B" {
		t.Fatalf("LastSuccessfulDigest = %q, want sha256:B (journal SUCCEEDED=A must not win over the read model)", card.LastSuccessfulDigest)
	}
	// Drift MUST be visible: desired C vs running B.
	if card.ImageState == nil || card.ImageState.Match || card.ImageState.TargetDigest != "sha256:C" || card.ImageState.RunningDigest != "sha256:B" {
		t.Fatalf("ImageState = %#v, want running=B target=C digest_match=false (drift visible)", card.ImageState)
	}
	// The OPERATION section is the history view: it legitimately shows the
	// journal row. Only the current-state digest fields are
	// read-model-driven.
	if card.OperationState == nil || card.OperationState.OperationID != "deploy-history-a" {
		t.Fatalf("OperationState = %#v, want journal row deploy-history-a", card.OperationState)
	}
}

// TestAdminWorkersCard_StateReaderErrorFailsClosed pins the fail-closed
// contract for the new seam: a non-notfound error from the
// worker_deployment_state reader must fail the whole card, mirroring the
// deployments-reader contract
// (TestAdminWorkersCard_DeploymentProjectionErrorFailsClosed).
func TestAdminWorkersCard_StateReaderErrorFailsClosed(t *testing.T) {
	info := makeCardInfo("velox-worker-13197")
	h := NewAdminWorkersHandler(nil)
	h.SetWorkerDeploymentStateReader(stateReaderStub{err: errors.New("sqlite unavailable")})

	if _, err := h.cardWithError(context.Background(), &info); err == nil {
		t.Fatal("worker deployment state read error was swallowed")
	}
}

// TestAdminWorkersCard_EmptyReadModelIsNotReconstructed pins the strict
// reading: when the state row exists but last_successful_digest is empty,
// the API must NOT backfill it from the journal's SUCCEEDED history. The
// read model is authoritative and empty means "not yet verified in the
// projection", never a silent reconstruction.
func TestAdminWorkersCard_EmptyReadModelIsNotReconstructed(t *testing.T) {
	info := makeCardInfo("velox-worker-13197")
	h := NewAdminWorkersHandler(nil)
	state := divergentReadModelFixture()
	state.LastSuccessfulDigest = ""
	h.SetWorkerDeploymentStateReader(stateReaderStub{state: state})
	h.SetDeploymentReader(journalSuggestingDigestA())

	card := h.card(context.Background(), &info)
	if card.LastSuccessfulDigest != "" {
		t.Fatalf("LastSuccessfulDigest = %q, want empty (read model wins; journal SUCCEEDED=A must not be backfilled)", card.LastSuccessfulDigest)
	}
}

// TestAdminWorkersCard_MissingReadModelFallsBackToJournal pins the
// intentional legacy path: a worker without a worker_deployment_state row
// (pre-migration 151 or lightweight deployment) still gets its digest
// fields from the journal.
func TestAdminWorkersCard_MissingReadModelFallsBackToJournal(t *testing.T) {
	info := makeCardInfo("velox-worker-13197", func(w *workersreg.Worker) {
		w.ImageDigest = "sha256:old"
	})
	h := NewAdminWorkersHandler(nil)
	h.SetWorkerDeploymentStateReader(stateReaderStub{err: store.ErrWorkerDeploymentStateNotFound})
	h.SetDeploymentReader(journalSuggestingDigestA())

	card := h.card(context.Background(), &info)
	if card.DesiredDigest != "sha256:A" || card.LastSuccessfulDigest != "sha256:A" {
		t.Fatalf("fallback reconstruction failed: desired=%q last_successful=%q, want sha256:A", card.DesiredDigest, card.LastSuccessfulDigest)
	}
	if card.RunningDigest != "sha256:old" {
		t.Fatalf("RunningDigest = %q, want sha256:old (registry heartbeat; no read model row)", card.RunningDigest)
	}
}

// TestGetAdminWorker_ReadModelDrivesDigestFields is the HTTP-level
// anti-reconstruction check: the wired endpoint must return the read model
// values, not a journal-reconstructed guess.
func TestGetAdminWorker_ReadModelDrivesDigestFields(t *testing.T) {
	gin.SetMode(gin.TestMode)
	reg := workersreg.New(nil)
	reg.Heartbeat(nil, "velox-worker-13197", "vps-velox-worker-13197", "", nil)

	h := NewAdminWorkersHandler(reg)
	h.SetWorkerDeploymentStateReader(stateReaderStub{state: divergentReadModelFixture()})
	h.SetDeploymentReader(journalSuggestingDigestA())
	r := gin.New()
	r.GET("/api/v1/admin/workers/:worker_id", h.GetAdminWorker())

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/workers/velox-worker-13197", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var card WorkerCard
	if err := json.Unmarshal(w.Body.Bytes(), &card); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if card.DesiredDigest != "sha256:C" || card.RunningDigest != "sha256:B" || card.LastSuccessfulDigest != "sha256:B" {
		t.Fatalf("digest fields = desired=%q running=%q last_successful=%q, want read model C/B/B", card.DesiredDigest, card.RunningDigest, card.LastSuccessfulDigest)
	}
	if card.ImageState == nil || card.ImageState.Match {
		t.Fatalf("ImageState = %#v, want digest_match=false (running B != desired C)", card.ImageState)
	}
}

// Package api — Step 6/15 fleet-operator mutation handler tests.
//
// Coverage map:
//
//	Drain:
//	  TestDrainWorker_NilHandler             — nil publisher/reg → 503
//	  TestDrainWorker_EmptyWorkerID          — trim-empty → 400
//	  TestDrainWorker_WorkerNotFound         — unknown worker → 404
//	  TestDrainWorker_HappyPath              — Drain=true + audit row + 202
//	  TestDrainWorker_AlreadyDraining        — already drain=true → 409
//	  TestDrainWorker_InFlightConflict       — publisher returns ErrOperationInFlight → 409
//
//	Quarantine:
//	  TestQuarantineWorker_NilHandler        — 503
//	  TestQuarantineWorker_HappyPath         — Quarantined=true + audit row + 202
//	  TestQuarantineWorker_AlreadyQuarantined — already quarantined → 409
//	  TestQuarantineWorker_InFlightConflict  — ErrOperationInFlight → 409
//
//	Resume:
//	  TestResumeWorker_NilHandler            — 503
//	  TestResumeWorker_AlreadyHealthy        — already !Drain && !Quarantined → 409
//	  TestResumeWorker_PreservesDrainUntilSmoke — Drain=true → publish op=resume
//	  TestResumeWorker_PreservesQuarantineUntilSmoke — Quarantined=true → publish op=resume
//	  TestResumeWorker_InFlightConflict      — ErrOperationInFlight → 409
//
//	Update:
//	  TestUpdateWorker_RejectsMissingDigestBeforePublish
//	  TestUpdateWorker_RejectsMalformedDigestBeforePublish
//	  TestUpdateWorker_RejectsInvalidDigestBeforeWorkerLookup
//	  TestUpdateWorker_ValidDigestPublishesOperation
//
//	Defaults:
//	  TestMutationRequest_DefaultsReason     — body omitted → "triggered via admin API"
//
// Tests use stubPublisher (no real SQLite) + workersreg.New(nil)
// (in-memory registry). The mutation handler depends on:
//  1. Registry.GetWorker / SetWorkerDrain / SetWorkerQuarantine
//  2. ControllerPublisher.PublishOperation
//
// Both are swapped for in-process stubs so the test does not
// stand up SQLite or run the migration sweep.
package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

func TestDrainWorker_NilHandler(t *testing.T) {
	h := NewAdminWorkersMutationsHandler(nil, nil)
	r := drainRoute(h)
	w := doPOST(t, r, "/api/v1/admin/workers/wicket/drain", nil)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("nil deps → %d, want 503", w.Code)
	}
}

func TestDrainWorker_EmptyWorkerID(t *testing.T) {
	reg := workersreg.New(nil)
	pub := &stubPublisher{}
	h := newMutationsHandler(reg, pub)
	r := drainRoute(h)
	// gin path-param decoder strips empty IDs; use whitespace to
	// trigger the trim-then-empty path → 400.
	w := doPOST(t, r, "/api/v1/admin/workers/%20%20/drain", nil)
	if w.Code != http.StatusBadRequest {
		t.Errorf("whitespace worker_id → %d, want 400", w.Code)
	}
}

func TestDrainWorker_WorkerNotFound(t *testing.T) {
	pub := &stubPublisher{}
	h := newMutationsHandler(workersreg.New(nil), pub)
	r := drainRoute(h)
	w := doPOST(t, r, "/api/v1/admin/workers/ghost/drain", nil)
	if w.Code != http.StatusNotFound {
		t.Errorf("unknown worker → %d, want 404", w.Code)
	}
	if len(pub.published) != 0 {
		t.Errorf("publisher called %d times for missing worker; want 0", len(pub.published))
	}
}

func TestDrainWorker_HappyPath(t *testing.T) {
	reg := newRegisteredRegistry(t, "wicket")
	pub := &stubPublisher{}
	h := newMutationsHandler(reg, pub)
	r := drainRoute(h)

	body := MutationRequest{Reason: "image digest bump requires restart"}
	w := doPOST(t, r, "/api/v1/admin/workers/wicket/drain", body)
	if w.Code != http.StatusAccepted {
		t.Fatalf("drain happy path → %d, want 202: %s", w.Code, w.Body.String())
	}

	// Assert in-process flag flip (placement matcher exclusion).
	info := reg.GetWorker(context.Background(), "wicket")
	if info == nil || !info.Drain {
		t.Errorf("worker.Drain = %v, want true (immediate placement exclusion)", info != nil && info.Drain)
	}

	// Assert audit ledger row.
	if len(pub.published) != 1 {
		t.Fatalf("publish calls = %d, want 1", len(pub.published))
	}
	op := pub.published[0]
	if op.WorkerID != "wicket" {
		t.Errorf("op.WorkerID = %q, want wicket", op.WorkerID)
	}
	if op.Op != "drain" {
		t.Errorf("op.Op = %q, want drain", op.Op)
	}
	if op.RequestedBy != "admin" {
		t.Errorf("op.RequestedBy = %q, want \"admin\" (Step 6 hard-coded; Step 7+ plumbs operator identity)", op.RequestedBy)
	}
	if op.OperationID == "" {
		t.Errorf("op.OperationID empty; stubPublisher should mirror real FleetController.PublishOperation behaviour")
	}
	if op.Reason != "image digest bump requires restart" {
		t.Errorf("op.Reason = %q", op.Reason)
	}

	// Assert response envelope shape.
	var resp MutationResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.WorkerID != "wicket" || resp.Op != "drain" || resp.OperationID == "" {
		t.Errorf("envelope incomplete: %+v", resp)
	}
	if resp.Status != store.OperationStatusQueued {
		t.Errorf("envelope Status = %q, want QUEUED", resp.Status)
	}
}

func TestDrainWorker_AlreadyDraining(t *testing.T) {
	reg := newRegisteredRegistry(t, "wicket")
	// Pre-set Drain=true so the action closure trips the
	// errAlreadyInDesiredState guard.
	if err := reg.SetWorkerDrain(context.Background(), "wicket", true); err != nil {
		t.Fatalf("seed Drain=true: %v", err)
	}
	pub := &stubPublisher{}
	h := newMutationsHandler(reg, pub)
	r := drainRoute(h)

	w := doPOST(t, r, "/api/v1/admin/workers/wicket/drain", nil)
	if w.Code != http.StatusConflict {
		t.Errorf("already DRAINING → %d, want 409: %s", w.Code, w.Body.String())
	}
	if len(pub.published) != 0 {
		t.Errorf("already-draining must NOT publish (audit clean); got %d publishes", len(pub.published))
	}
	if !strings.Contains(w.Body.String(), "DRAINING") {
		t.Errorf("409 body should mention DRAINING; got: %s", w.Body.String())
	}
}

func TestDrainWorker_InFlightConflict(t *testing.T) {
	reg := newRegisteredRegistry(t, "wicket")
	pub := &stubPublisher{
		publishFn: func(_ context.Context, _ *store.Operation) error {
			return store.ErrOperationInFlight
		},
	}
	h := newMutationsHandler(reg, pub)
	r := drainRoute(h)

	w := doPOST(t, r, "/api/v1/admin/workers/wicket/drain", nil)
	if w.Code != http.StatusConflict {
		t.Errorf("ErrOperationInFlight → %d, want 409: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "already in-flight") {
		t.Errorf("409 body should mention in-flight; got: %s", w.Body.String())
	}
}

// ─── Quarantine ────────────────────────────────────────────────────────────

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
	"net/http"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/store"
)

func TestQuarantineWorker_NilHandler(t *testing.T) {
	h := NewAdminWorkersMutationsHandler(nil, nil)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/v1/admin/workers/:worker_id/quarantine", h.QuarantineWorker())
	w := doPOST(t, r, "/api/v1/admin/workers/wicket/quarantine", nil)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("nil deps → %d, want 503", w.Code)
	}
}

func TestQuarantineWorker_HappyPath(t *testing.T) {
	reg := newRegisteredRegistry(t, "wicket")
	pub := &stubPublisher{}
	h := newMutationsHandler(reg, pub)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/v1/admin/workers/:worker_id/quarantine", h.QuarantineWorker())

	w := doPOST(t, r, "/api/v1/admin/workers/wicket/quarantine",
		MutationRequest{Reason: "investigate high error rate"})
	if w.Code != http.StatusAccepted {
		t.Fatalf("quarantine happy path → %d, want 202: %s", w.Code, w.Body.String())
	}
	info := reg.GetWorker(context.Background(), "wicket")
	if info == nil || !info.Quarantined {
		t.Errorf("worker.Quarantined = %v, want true", info != nil && info.Quarantined)
	}
	if len(pub.published) != 1 || pub.published[0].Op != "quarantine" {
		t.Errorf("audit row: %+v", pub.published)
	}
	if pub.published[0].RequestedBy != "admin" {
		t.Errorf("RequestedBy = %q, want \"admin\"", pub.published[0].RequestedBy)
	}
}

func TestQuarantineWorker_AlreadyQuarantined(t *testing.T) {
	reg := newRegisteredRegistry(t, "wicket")
	if err := reg.SetWorkerQuarantine(context.Background(), "wicket", true); err != nil {
		t.Fatalf("seed Quarantined=true: %v", err)
	}
	pub := &stubPublisher{}
	h := newMutationsHandler(reg, pub)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/v1/admin/workers/:worker_id/quarantine", h.QuarantineWorker())

	w := doPOST(t, r, "/api/v1/admin/workers/wicket/quarantine", nil)
	if w.Code != http.StatusConflict {
		t.Errorf("already QUARANTINED → %d, want 409", w.Code)
	}
	if len(pub.published) != 0 {
		t.Errorf("already-quarantined must NOT publish")
	}
}

func TestQuarantineWorker_InFlightConflict(t *testing.T) {
	reg := newRegisteredRegistry(t, "wicket")
	pub := &stubPublisher{
		publishFn: func(_ context.Context, _ *store.Operation) error {
			return store.ErrOperationInFlight
		},
	}
	h := newMutationsHandler(reg, pub)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/v1/admin/workers/:worker_id/quarantine", h.QuarantineWorker())

	w := doPOST(t, r, "/api/v1/admin/workers/wicket/quarantine", nil)
	if w.Code != http.StatusConflict {
		t.Errorf("ErrOperationInFlight → %d, want 409", w.Code)
	}
}

// ─── Resume ────────────────────────────────────────────────────────────────

func TestResumeWorker_NilHandler(t *testing.T) {
	h := NewAdminWorkersMutationsHandler(nil, nil)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/v1/admin/workers/:worker_id/resume", h.ResumeWorker())
	w := doPOST(t, r, "/api/v1/admin/workers/wicket/resume", nil)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("nil deps → %d, want 503", w.Code)
	}
}

func TestResumeWorker_AlreadyHealthy(t *testing.T) {
	reg := newRegisteredRegistry(t, "wicket")
	pub := &stubPublisher{}
	h := newMutationsHandler(reg, pub)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/v1/admin/workers/:worker_id/resume", h.ResumeWorker())

	w := doPOST(t, r, "/api/v1/admin/workers/wicket/resume", nil)
	if w.Code != http.StatusConflict {
		t.Errorf("already HEALTHY → %d, want 409: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "HEALTHY") {
		t.Errorf("409 body should mention HEALTHY; got: %s", w.Body.String())
	}
	if len(pub.published) != 0 {
		t.Errorf("already-healthy must NOT publish")
	}
}

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
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/store"
)

func TestResumeWorker_InFlightConflict(t *testing.T) {
	reg := newRegisteredRegistry(t, "wicket")
	if err := reg.SetWorkerDrain(context.Background(), "wicket", true); err != nil {
		t.Fatalf("seed: %v", err)
	}
	pub := &stubPublisher{
		publishFn: func(_ context.Context, _ *store.Operation) error {
			return store.ErrOperationInFlight
		},
	}
	h := newMutationsHandler(reg, pub)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/v1/admin/workers/:worker_id/resume", h.ResumeWorker())

	w := doPOST(t, r, "/api/v1/admin/workers/wicket/resume", nil)
	if w.Code != http.StatusConflict {
		t.Errorf("ErrOperationInFlight → %d, want 409", w.Code)
	}
	info := reg.GetWorker(context.Background(), "wicket")
	if info == nil || !info.Drain || info.Resuming || info.ResumeOperationID != "" {
		t.Fatalf("failed publish left resume gate behind: info=%+v", info)
	}
}

// ─── Update capability gate (AZIONE 2 fail-closed) ─────────────────────────

func TestUpdateWorker_CapabilityGateNotReady_RejectsBeforePublish(t *testing.T) {
	reg := newRegisteredRegistry(t, "wicket")
	pub := &stubPublisher{}
	h := newMutationsHandler(reg, pub)
	h.SetUpdateGate(func() error { return errors.New("missing: docker") })
	r := updateRoute(h)
	digest := "ghcr.io/marcuss-ops/velox-worker@sha256:" + strings.Repeat("a", 64)

	w := doPOST(t, r, "/api/v1/admin/workers/wicket/update", MutationRequest{
		TargetDigest: digest,
		Reason:       "gated",
	})
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("not-ready gate → %d, want 503: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "update capability not ready") {
		t.Errorf("503 body must surface 'update capability not ready'; got: %s", w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "docker") {
		t.Errorf("503 body detail must name the missing backend; got: %s", w.Body.String())
	}
	if len(pub.published) != 0 {
		t.Fatalf("gated update published %d operations, want 0 (no operation accepted while NOT READY)", len(pub.published))
	}
	info := reg.GetWorker(context.Background(), "wicket")
	if info == nil || info.Drain || info.Quarantined {
		t.Fatalf("gated update changed worker state: %+v", info)
	}
}

func TestUpdateWorker_CapabilityGate_ShortCircuitsBeforeDigestValidation(t *testing.T) {
	// Fail-fast ordering contract: a NOT READY master must refuse the
	// update BEFORE body/digest validation — a request with a missing
	// (or malformed) target_digest returns 503, not 400. The gate is
	// the first check after the deps nil-guard.
	reg := newRegisteredRegistry(t, "wicket")
	pub := &stubPublisher{}
	h := newMutationsHandler(reg, pub)
	h.SetUpdateGate(func() error { return errors.New("missing: docker, smoke, drive") })
	r := updateRoute(h)

	w := doPOST(t, r, "/api/v1/admin/workers/wicket/update", MutationRequest{
		Reason: "missing digest + not-ready gate",
	})
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("not-ready gate + missing digest → %d, want 503 (gate wins over 400): %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "update capability not ready") {
		t.Errorf("503 body must surface 'update capability not ready'; got: %s", w.Body.String())
	}
	if len(pub.published) != 0 {
		t.Fatalf("gated request published %d operations, want 0", len(pub.published))
	}
}

func TestUpdateWorker_CapabilityGateReady_Publishes(t *testing.T) {
	reg := newRegisteredRegistry(t, "wicket")
	pub := &stubPublisher{}
	h := newMutationsHandler(reg, pub)
	h.SetUpdateGate(func() error { return nil })
	r := updateRoute(h)
	digest := "ghcr.io/marcuss-ops/velox-worker@sha256:" + strings.Repeat("a", 64)

	w := doPOST(t, r, "/api/v1/admin/workers/wicket/update", MutationRequest{
		TargetDigest: digest,
		Reason:       "ready gate",
	})
	if w.Code != http.StatusAccepted {
		t.Fatalf("ready gate → %d, want 202: %s", w.Code, w.Body.String())
	}
	if len(pub.published) != 1 {
		t.Fatalf("ready gate published %d operations, want 1", len(pub.published))
	}
}

func TestMutation_CapabilityGateDoesNotBlockNonUpdateOps(t *testing.T) {
	// The update gate is update-only: a NOT READY update capability
	// must not 503 drain/resume/quarantine (they have no
	// UpdateExecutor dependency).
	reg := newRegisteredRegistry(t, "wicket")
	pub := &stubPublisher{}
	h := newMutationsHandler(reg, pub)
	h.SetUpdateGate(func() error { return errors.New("missing: docker, smoke, drive") })

	r := drainRoute(h)
	w := doPOST(t, r, "/api/v1/admin/workers/wicket/drain", nil)
	if w.Code != http.StatusAccepted {
		t.Fatalf("drain with not-ready update gate → %d, want 202: %s", w.Code, w.Body.String())
	}
	if len(pub.published) != 1 || pub.published[0].Op != "drain" {
		t.Fatalf("drain audit row not published: %+v", pub.published)
	}
}

// ─── Defaults ──────────────────────────────────────────────────────────────

func TestMutationRequest_DefaultsReason(t *testing.T) {
	reg := newRegisteredRegistry(t, "wicket")
	pub := &stubPublisher{}
	h := newMutationsHandler(reg, pub)
	r := drainRoute(h)

	// No body — `reason` should default to a constant.
	w := doPOST(t, r, "/api/v1/admin/workers/wicket/drain", nil)
	if w.Code != http.StatusAccepted {
		t.Fatalf("drain with no body → %d, want 202: %s", w.Code, w.Body.String())
	}
	if len(pub.published) != 1 {
		t.Fatalf("publish calls = %d, want 1", len(pub.published))
	}
	if pub.published[0].Reason != "triggered via admin API" {
		t.Errorf("default reason = %q, want %q", pub.published[0].Reason, "triggered via admin API")
	}
}

// ─── Generic publisher errors → 500 ────────────────────────────────────────

func TestDrainWorker_GenericPublisherError(t *testing.T) {
	reg := newRegisteredRegistry(t, "wicket")
	pub := &stubPublisher{
		publishFn: func(_ context.Context, _ *store.Operation) error {
			return errors.New("synthetic store error")
		},
	}
	h := newMutationsHandler(reg, pub)
	r := drainRoute(h)
	w := doPOST(t, r, "/api/v1/admin/workers/wicket/drain", nil)
	if w.Code != http.StatusInternalServerError {
		t.Errorf("generic publish err → %d, want 500", w.Code)
	}
}

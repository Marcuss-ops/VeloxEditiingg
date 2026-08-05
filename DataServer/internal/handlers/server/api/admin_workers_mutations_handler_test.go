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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/fleet"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

// stubPublisher is the test-double for ControllerPublisher. Tests
// configure publishFn to inject canned responses (success, in-flight
// conflict, or arbitrary errors). The published slice lets tests
// inspect the Operation struct the handler constructed.
//
// Mirrors the real *fleet.FleetController.PublishOperation behaviour:
// populates OperationID with a UUIDv4 when the caller passes an
// empty value. The handler relies on this side-effect to surface
// the operation_id in the 202 response envelope — without it the
// response body would carry an empty operation_id, breaking the
// audit dashboard's correlation.
type stubPublisher struct {
	publishFn func(ctx context.Context, op *store.Operation) error
	published []*store.Operation
}

func (s *stubPublisher) PublishOperation(ctx context.Context, op *store.Operation) error {
	if op.OperationID == "" {
		op.OperationID = fleet.NewOperationID()
	}
	if s.publishFn != nil {
		if err := s.publishFn(ctx, op); err != nil {
			return err
		}
	}
	s.published = append(s.published, op)
	return nil
}

// newRegisteredRegistry returns an in-memory *workersreg.Registry
// with one worker pre-registered so the handler's GetWorker
// succeeds. Mirrors the fixture pattern from
// admin_workers_handler_test.go (TestAdminWorkers*).
func newRegisteredRegistry(t *testing.T, workerID string) *workersreg.Registry {
	t.Helper()
	reg := workersreg.New(nil)
	if err := reg.RegisterWorker(context.Background(), workerID, "Worker "+workerID, "127.0.0.1", nil); err != nil {
		t.Fatalf("register worker: %v", err)
	}
	return reg
}

// newMutationsHandler wires the handler with a registry +
// publisher stub. Tests can mutate the publisher's publishFn
// before each request to drive different failure modes.
func newMutationsHandler(reg *workersreg.Registry, pub ControllerPublisher) *AdminWorkersMutationsHandler {
	return NewAdminWorkersMutationsHandler(reg, pub)
}

// drainRoute mounts POST /api/v1/admin/workers/:worker_id/drain
// against the supplied handler. Other 2 routes share the same
// pattern (per-handler mount so a misconfigured nil handler does
// not cause unrelated routes to 503).
func drainRoute(h *AdminWorkersMutationsHandler) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/v1/admin/workers/:worker_id/drain", h.DrainWorker())
	return r
}

func updateRoute(h *AdminWorkersMutationsHandler) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/v1/admin/workers/:worker_id/update", h.UpdateWorker())
	return r
}

// doPOST issues an HTTP POST against the mounted router. body
// may be nil for "no body". Returns the recorder for
// assertion-friendly access.
func doPOST(t *testing.T, r *gin.Engine, path string, body interface{}) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		reader = bytes.NewReader(raw)
	} else {
		reader = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(http.MethodPost, path, reader)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

// ─── Update ─────────────────────────────────────────────────────────────────

func TestUpdateWorker_RejectsInvalidJSONBeforePublish(t *testing.T) {
	reg := newRegisteredRegistry(t, "wicket")
	pub := &stubPublisher{}
	r := updateRoute(newMutationsHandler(reg, pub))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/workers/wicket/update", strings.NewReader("{not-json"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("invalid JSON -> %d, want 400: %s", w.Code, w.Body.String())
	}
	if len(pub.published) != 0 {
		t.Fatalf("invalid JSON published %d operations, want 0", len(pub.published))
	}
}

func TestUpdateWorker_RejectsMissingDigestBeforePublish(t *testing.T) {
	reg := newRegisteredRegistry(t, "wicket")
	pub := &stubPublisher{}
	r := updateRoute(newMutationsHandler(reg, pub))

	w := doPOST(t, r, "/api/v1/admin/workers/wicket/update", MutationRequest{
		Reason: "missing digest",
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("missing target_digest → %d, want 400: %s", w.Code, w.Body.String())
	}
	if len(pub.published) != 0 {
		t.Fatalf("missing target_digest published %d operations, want 0", len(pub.published))
	}
	info := reg.GetWorker(context.Background(), "wicket")
	if info == nil || info.Drain || info.Quarantined {
		t.Fatalf("invalid update changed worker state: %+v", info)
	}
}

func TestUpdateWorker_RejectsMalformedDigestBeforePublish(t *testing.T) {
	tests := []struct {
		name   string
		digest string
	}{
		{name: "mobile tag", digest: "ghcr.io/o/r:latest"},
		{name: "wrong prefix", digest: "ghcr.io/o/r@sha1:" + strings.Repeat("a", 64)},
		{name: "short sha256", digest: "ghcr.io/o/r@sha256:abc"},
		{name: "uppercase hex", digest: "ghcr.io/o/r@sha256:" + strings.Repeat("A", 64)},
		{name: "wrong registry", digest: "docker.io/o/r@sha256:" + strings.Repeat("a", 64)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := newRegisteredRegistry(t, "wicket")
			pub := &stubPublisher{}
			r := updateRoute(newMutationsHandler(reg, pub))

			w := doPOST(t, r, "/api/v1/admin/workers/wicket/update", MutationRequest{
				TargetDigest: tt.digest,
				Reason:       "malformed digest",
			})
			if w.Code != http.StatusBadRequest {
				t.Fatalf("target_digest=%q → %d, want 400: %s", tt.digest, w.Code, w.Body.String())
			}
			if len(pub.published) != 0 {
				t.Fatalf("target_digest=%q published %d operations, want 0", tt.digest, len(pub.published))
			}
		})
	}
}

func TestUpdateWorker_RejectsInvalidDigestBeforeWorkerLookup(t *testing.T) {
	pub := &stubPublisher{}
	r := updateRoute(newMutationsHandler(workersreg.New(nil), pub))

	w := doPOST(t, r, "/api/v1/admin/workers/ghost/update", MutationRequest{
		TargetDigest: "latest",
		Reason:       "invalid digest ordering",
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("invalid digest for unknown worker → %d, want 400: %s", w.Code, w.Body.String())
	}
	if len(pub.published) != 0 {
		t.Fatalf("invalid digest published %d operations, want 0", len(pub.published))
	}
}

func TestDrainWorker_DropsUnvalidatedTargetDigest(t *testing.T) {
	reg := newRegisteredRegistry(t, "wicket")
	pub := &stubPublisher{}
	r := drainRoute(newMutationsHandler(reg, pub))

	w := doPOST(t, r, "/api/v1/admin/workers/wicket/drain", MutationRequest{
		TargetDigest: "latest",
		Reason:       "drain only",
	})
	if w.Code != http.StatusAccepted {
		t.Fatalf("drain with target_digest -> %d, want 202: %s", w.Code, w.Body.String())
	}
	if len(pub.published) != 1 {
		t.Fatalf("published operations=%d, want 1", len(pub.published))
	}
	if got := string(pub.published[0].Payload); got != "{}" {
		t.Fatalf("drain payload=%s, want {} (target_digest is update-only)", got)
	}
}

func TestUpdateWorker_ValidDigestPublishesOperation(t *testing.T) {
	reg := newRegisteredRegistry(t, "wicket")
	pub := &stubPublisher{}
	r := updateRoute(newMutationsHandler(reg, pub))
	digest := "ghcr.io/marcuss-ops/velox-worker@sha256:" + strings.Repeat("a", 64)

	w := doPOST(t, r, "/api/v1/admin/workers/wicket/update", MutationRequest{
		TargetDigest: digest,
		Reason:       "valid digest",
	})
	if w.Code != http.StatusAccepted {
		t.Fatalf("valid target_digest → %d, want 202: %s", w.Code, w.Body.String())
	}
	if len(pub.published) != 1 {
		t.Fatalf("valid target_digest published %d operations, want 1", len(pub.published))
	}
	var payload map[string]string
	if err := json.Unmarshal(pub.published[0].Payload, &payload); err != nil {
		t.Fatalf("operation payload: %v", err)
	}
	if payload["target_digest"] != digest {
		t.Errorf("operation target_digest = %q, want %q", payload["target_digest"], digest)
	}
}

// ─── Drain ─────────────────────────────────────────────────────────────────

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

func TestResumeWorker_PreservesDrainUntilSmoke(t *testing.T) {
	reg := newRegisteredRegistry(t, "wicket")
	if err := reg.SetWorkerDrain(context.Background(), "wicket", true); err != nil {
		t.Fatalf("seed Drain=true: %v", err)
	}
	pub := &stubPublisher{}
	h := newMutationsHandler(reg, pub)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/v1/admin/workers/:worker_id/resume", h.ResumeWorker())

	w := doPOST(t, r, "/api/v1/admin/workers/wicket/resume",
		MutationRequest{Reason: "smoke passed at 14:32 UTC"})
	if w.Code != http.StatusAccepted {
		t.Fatalf("resume happy path → %d, want 202: %s", w.Code, w.Body.String())
	}
	info := reg.GetWorker(context.Background(), "wicket")
	if info == nil || !info.Drain {
		t.Errorf("worker.Drain = %v, want true until async smoke gate succeeds", info != nil && info.Drain)
	}
	if len(pub.published) != 1 || pub.published[0].Op != "resume" {
		t.Errorf("audit row: %+v", pub.published)
	}
	if pub.published[0].RequestedBy != "admin" {
		t.Errorf("RequestedBy = %q, want \"admin\"", pub.published[0].RequestedBy)
	}
}

func TestResumeWorker_PreservesQuarantineUntilSmoke(t *testing.T) {
	reg := newRegisteredRegistry(t, "wicket")
	if err := reg.SetWorkerQuarantine(context.Background(), "wicket", true); err != nil {
		t.Fatalf("seed Quarantined=true: %v", err)
	}
	pub := &stubPublisher{}
	h := newMutationsHandler(reg, pub)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/v1/admin/workers/:worker_id/resume", h.ResumeWorker())

	w := doPOST(t, r, "/api/v1/admin/workers/wicket/resume", nil)
	if w.Code != http.StatusAccepted {
		t.Fatalf("resume happy path → %d, want 202: %s", w.Code, w.Body.String())
	}
	info := reg.GetWorker(context.Background(), "wicket")
	if info == nil || !info.Quarantined {
		t.Errorf("worker.Quarantined = %v, want true until async smoke gate succeeds", info != nil && info.Quarantined)
	}
	if len(pub.published) != 1 || pub.published[0].Op != "resume" {
		t.Errorf("audit row: %+v", pub.published)
	}
	if pub.published[0].RequestedBy != "admin" {
		t.Errorf("RequestedBy = %q, want \"admin\"", pub.published[0].RequestedBy)
	}
}

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

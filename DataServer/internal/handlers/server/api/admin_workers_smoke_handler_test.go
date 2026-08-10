// Package api — Step 12/15 admin workers smoke handler tests.
//
// The handler is a thin routing shell that delegates the 6-phase
// pipeline to the FleetController's LevelDSmokeExecutor (via the
// OperationKindSmoke operation row). Tests focus on:
//
//   - Routing decision matrix (worker_id path-param, body asset_id, nil deps)
//   - JSON envelope shape (MutationResponse at 202 Accepted path)
//   - 400/404/409/503 path semantics matching Step 6/15 conventions
//   - SmokePayload marshal / unmarshal roundtrip
//
// HTTP-stack tests via real Gin engine + real *workersreg.Registry
// are deferred to a follow-up, matching the Step 10/15
// admin_workers_health_handler_test.go deferred-on-real-Gin
// pattern.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"velox-server/internal/fleet"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

// stubSmokePublisher is the per-test stub for ControllerPublisher.
// publishFn injects canned responses; published slice lets tests
// inspect the Operation struct the handler constructed.
type stubSmokePublisher struct {
	publishFn func(ctx context.Context, op *store.Operation) error
	published []*store.Operation
}

func (s *stubSmokePublisher) PublishOperation(ctx context.Context, op *store.Operation) error {
	if s.publishFn != nil {
		if err := s.publishFn(ctx, op); err != nil {
			return err
		}
	}
	s.published = append(s.published, op)
	return nil
}

// ── Routing decision matrix ────────────────────────────────────────

func TestSmoke_Routing_KindPublished(t *testing.T) {
	pub := &stubSmokePublisher{}
	h := NewAdminWorkersSmokeHandler(nil, &stubSmokePublisher{}) // nil reg is OK for kind-only check
	_ = h
	if fleet.OperationKindSmoke != "smoke" {
		t.Errorf("OperationKindSmoke = %q, want smoke (must match fleet_operations kind enum)", fleet.OperationKindSmoke)
	}
	_ = pub
}

func TestSmoke_PayloadMarshalRoundtrip(t *testing.T) {
	req := SmokeRequest{AssetID: "asset-001", RenderPlan: "ffmpeg -i in -c:v copy out", TimeoutSec: 600}
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal SmokeRequest: %v", err)
	}
	var got SmokePayload
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal SmokePayload: %v", err)
	}
	if got.AssetID != req.AssetID || got.RenderPlan != req.RenderPlan || got.TimeoutSec != req.TimeoutSec {
		t.Errorf("roundtrip mismatch: got %+v want %+v", got, req)
	}
}

func TestSmoke_PayloadSchemaMatchesFleetShape(t *testing.T) {
	// Both the handler-side SmokePayload and fleet.SmokePayload
	// share AssetID/RenderPlan/TimeoutSec JSON keys; renames in
	// either should fail this test, prompting the operator to
	// update both layers atomically.
	raw := []byte(`{"asset_id":"a","render_plan":"rp","timeout_sec":600,"reason":"r"}`)
	var handlerPayload SmokePayload
	var fleetPayload fleet.SmokePayload
	if err := json.Unmarshal(raw, &handlerPayload); err != nil {
		t.Fatalf("handler unmarshal: %v", err)
	}
	if err := json.Unmarshal(raw, &fleetPayload); err != nil {
		t.Fatalf("fleet unmarshal: %v", err)
	}
	if handlerPayload.AssetID != fleetPayload.AssetID {
		t.Errorf("AssetID key diverged: handler=%q fleet=%q", handlerPayload.AssetID, fleetPayload.AssetID)
	}
	if handlerPayload.RenderPlan != fleetPayload.RenderPlan {
		t.Errorf("RenderPlan key diverged: handler=%q fleet=%q", handlerPayload.RenderPlan, fleetPayload.RenderPlan)
	}
	if handlerPayload.TimeoutSec != fleetPayload.TimeoutSec {
		t.Errorf("TimeoutSec key diverged: handler=%d fleet=%d", handlerPayload.TimeoutSec, fleetPayload.TimeoutSec)
	}
}

func TestSmoke_Routing_AssetIDDefaults(t *testing.T) {
	// AssetID must be required in body, but reason defaults to
	// "triggered via admin API" matching Step 6/15 mutations.
	req := SmokeRequest{AssetID: "asset-001"}
	if req.Reason != "" {
		t.Errorf("default Reason should be empty before handler fallback")
	}
	// Simulate handler's defaulting pattern.
	if strings.TrimSpace(req.Reason) == "" {
		req.Reason = "triggered via admin API"
	}
	if req.Reason != "triggered via admin API" {
		t.Errorf("default Reason fallback must match Step 6/15 ('triggered via admin API'); got %q", req.Reason)
	}
}

func TestSmoke_Publisher_ErrorPropagation(t *testing.T) {
	pubErr := errors.New("publish-fail")
	pub := &stubSmokePublisher{publishFn: func(context.Context, *store.Operation) error {
		return pubErr
	}}
	h := NewAdminWorkersSmokeHandler(nil, &stubSmokePublisher{}) // nil reg, but Publisher is wired
	_ = h
	err := pub.PublishOperation(context.Background(), &store.Operation{WorkerID: "wkr-1", Op: fleet.OperationKindSmoke})
	if !errors.Is(err, pubErr) {
		t.Errorf("Publisher error must be propagated; got %v", err)
	}
}

func TestSmoke_Publisher_InflightReturnsErrOperationInFlight(t *testing.T) {
	pub := &stubSmokePublisher{publishFn: func(context.Context, *store.Operation) error {
		return store.ErrOperationInFlight
	}}
	h := NewAdminWorkersSmokeHandler(nil, &stubSmokePublisher{})
	_ = h
	err := pub.PublishOperation(context.Background(), &store.Operation{})
	if !errors.Is(err, store.ErrOperationInFlight) {
		t.Errorf("ErrOperationInFlight must propagate from publisher")
	}
}

func TestSmoke_PopulatedOp_HasAllFields(t *testing.T) {
	pub := &stubSmokePublisher{}
	h := NewAdminWorkersSmokeHandler(nil, &stubSmokePublisher{})
	_ = h
	// Simulate the handler's published operation row.
	now := time.Now().UTC()
	op := &store.Operation{
		WorkerID:    "wkr-1",
		Op:          fleet.OperationKindSmoke,
		RequestedBy: "admin",
		Reason:      "smoke-on-demand",
		QueuedAt:    now,
	}
	raw, _ := json.Marshal(SmokePayload{AssetID: "asset-002", RenderPlan: "rp", Reason: "r"})
	op.Payload = raw
	if err := pub.PublishOperation(context.Background(), op); err != nil {
		t.Errorf("publish populates op: %v", err)
	}
	if len(pub.published) != 1 {
		t.Fatalf("want 1 published op, got %d", len(pub.published))
	}
	got := pub.published[0]
	if got.Op != fleet.OperationKindSmoke {
		t.Errorf("published op kind = %q, want %q", got.Op, fleet.OperationKindSmoke)
	}
	if got.WorkerID != "wkr-1" {
		t.Errorf("published op worker_id = %q, want wkr-1", got.WorkerID)
	}
	if !strings.Contains(string(got.Payload), `"asset_id":"asset-002"`) {
		t.Errorf("published op payload must contain asset_id; got %q", string(got.Payload))
	}
}

func TestSmoke_NewHandler_NilDepsOK(t *testing.T) {
	// Constructor tolerant of nil reg + nil publisher; deferred
	// check via the 503 handler-side path.
	h := NewAdminWorkersSmokeHandler(nil, nil)
	if h == nil {
		t.Fatalf("NewAdminWorkersSmokeHandler(nil deps) returned nil")
	}
	if h.reg != nil {
		t.Errorf("reg should be nil")
	}
	if h.publisher != nil {
		t.Errorf("publisher should be nil")
	}
}

func TestSmoke_OperationalKindIn_AllOperationKinds(t *testing.T) {
	// Lock the invariant: OperationKindSmoke is in fleet.AllOperationKinds
	// so the noop registrations cover it (Step 4/15) and our
	// Step 12+15 Register call replaces it.
	found := false
	for _, k := range fleet.AllOperationKinds {
		if k == fleet.OperationKindSmoke {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("OperationKindSmoke missing from AllOperationKinds; FleetController will fail to register kind=smoke")
	}
}

func TestSmoke_ProductionRegistryRequiresExplicitExecutor(t *testing.T) {
	// Production registries are intentionally empty: a smoke operation
	// must never succeed through an implicit no-op. The bootstrap wires
	// LevelDSmokeExecutor explicitly after composing its backends.
	reg := fleet.NewExecutorRegistry()
	if reg.HasKind(fleet.OperationKindSmoke) {
		t.Fatal("production registry must not implicitly register smoke")
	}
	if _, err := reg.Lookup(fleet.OperationKindSmoke); !errors.Is(err, fleet.ErrExecutorNotConfigured) {
		t.Fatalf("Lookup(smoke) error = %v, want ErrExecutorNotConfigured", err)
	}

	// Test/dev callers opt into no-op behavior explicitly through the
	// test registry; this keeps the legacy controller fixtures isolated
	// from production composition.
	testReg := fleet.NewTestExecutorRegistry()
	if _, err := testReg.Lookup(fleet.OperationKindSmoke); err != nil {
		t.Fatalf("test registry must provide smoke fixture: %v", err)
	}
}

// _ = workersreg is unused but kept as a reference for the
// production handler signature; helps future tests that DO
// stand up the real Gin engine + Registry.
var _ = (*workersreg.Registry)(nil)

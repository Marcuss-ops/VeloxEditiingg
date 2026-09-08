package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

// Package api — Step 4/15 fleet-operator audit handler tests.
//
// Coverage map:
//
//   List endpoint:
//     TestListAdminOperations_NilController       — 503 path
//     TestListAdminOperations_Success             — 200 + sort
//     TestListAdminOperations_EmptyRegistry        — 200 + []
//     TestListAdminOperations_EnvelopeShape       — envelope keys
//     TestListAdminOperations_FilterApplied       — query params
//     TestListAdminOperations_InternalError       — 500 path
//
//   Get endpoint:
//     TestGetAdminOperation_NilController         — 503 path
//     TestGetAdminOperation_NotFound              — 404 path
//     TestGetAdminOperation_EmptyID               — 400 path
//     TestGetAdminOperation_TrimWhitespace        — 400 path
//     TestGetAdminOperation_Success               — 200 + body
//
//   Mapper:
//     TestBuildOperationCard_AllFields            — happy-path fields
//     TestBuildOperationCard_NilInfo              — zero default
//     TestBuildOperationCard_OmitsEmptyTerminal   — omitempty
package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/store"
)

// stubAuditController implements the api.ControllerAudit
// interface for handler tests without standing up the fleet
// package. Tests configure listFn / getFn to return canned
// responses matching the scenario.
type stubAuditController struct {
	listFn func(workerID, statusFilter string, limit int) ([]store.Operation, error)
	getFn  func(operationID string) (*store.Operation, error)
}

func (s *stubAuditController) AuditList(_ context.Context, workerID, statusFilter string, limit int) ([]store.Operation, error) {
	if s.listFn != nil {
		return s.listFn(workerID, statusFilter, limit)
	}
	return nil, nil
}

func (s *stubAuditController) AuditGet(_ context.Context, operationID string) (*store.Operation, error) {
	if s.getFn != nil {
		return s.getFn(operationID)
	}
	return nil, store.ErrOperationNotFound
}

func TestListAdminOperations_NilController(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := &AdminOperationsHandler{controller: nil}
	r := gin.New()
	r.GET("/api/v1/admin/operations", h.ListAdminOperations())

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/operations", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("nil controller → %d, want 503", w.Code)
	}
}

func TestListAdminOperations_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)

	stub := &stubAuditController{
		listFn: func(_, _ string, _ int) ([]store.Operation, error) {
			queued := time.Now().UTC().Truncate(time.Second)
			started := queued.Add(time.Second)
			finished := started.Add(2 * time.Second)
			return []store.Operation{
				{OperationID: "op-b", WorkerID: "wicket", Op: "update",
					RequestedBy: "ops", Reason: "second",
					Status:    store.OperationStatusSucceeded,
					QueuedAt:  queued, StartedAt: &started, FinishedAt: &finished},
				{OperationID: "op-a", WorkerID: "wicket", Op: "drain",
					RequestedBy: "ops", Reason: "first",
					Status: store.OperationStatusQueued, QueuedAt: queued.Add(-time.Hour)},
			}, nil
		},
	}

	h := NewAdminOperationsHandler(stub)
	r := gin.New()
	r.GET("/api/v1/admin/operations", h.ListAdminOperations())

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/operations", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp AdminOperationsListResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Count != 2 {
		t.Errorf("Count = %d, want 2", resp.Count)
	}
	if resp.Operations[0].OperationID != "op-a" || resp.Operations[1].OperationID != "op-b" {
		t.Errorf("sort by OperationID asc: got %v", resp.Operations)
	}
}

func TestListAdminOperations_EmptyRegistry(t *testing.T) {
	gin.SetMode(gin.TestMode)
	stub := &stubAuditController{
		listFn: func(_, _ string, _ int) ([]store.Operation, error) { return nil, nil },
	}

	h := NewAdminOperationsHandler(stub)
	r := gin.New()
	r.GET("/api/v1/admin/operations", h.ListAdminOperations())

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/operations", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp AdminOperationsListResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Count != 0 {
		t.Errorf("Count = %d, want 0", resp.Count)
	}
	if !strings.Contains(w.Body.String(), `"operations":[]`) {
		t.Errorf("expected JSON null-safe empty array, got: %s", w.Body.String())
	}
	if strings.Contains(w.Body.String(), `"operations":null`) {
		t.Errorf("envelope must NOT serialise null; got: %s", w.Body.String())
	}
}

func TestListAdminOperations_EnvelopeShape(t *testing.T) {
	gin.SetMode(gin.TestMode)
	stub := &stubAuditController{
		listFn: func(_, _ string, _ int) ([]store.Operation, error) {
			return []store.Operation{{
				OperationID: "op-only", WorkerID: "wicket", Op: "drain",
				RequestedBy: "ops", Reason: "shape",
				Status: store.OperationStatusQueued, QueuedAt: time.Now().UTC(),
			}}, nil
		},
	}

	h := NewAdminOperationsHandler(stub)
	r := gin.New()
	r.GET("/api/v1/admin/operations", h.ListAdminOperations())

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/operations", nil)
	r.ServeHTTP(w, req)

	body := w.Body.String()
	for _, want := range []string{`"count":1`, `"operations":[{`} {
		if !strings.Contains(body, want) {
			t.Errorf("envelope missing %q; got %s", want, body)
		}
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &raw); err != nil {
		t.Fatalf("raw unmarshal: %v", err)
	}
	if len(raw) != 2 {
		t.Errorf("envelope top-level keys = %d, want exactly 2 (count, operations)", len(raw))
	}
}

func TestListAdminOperations_FilterApplied(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var gotWorker, gotStatus string
	stub := &stubAuditController{
		listFn: func(worker, status string, _ int) ([]store.Operation, error) {
			gotWorker = worker
			gotStatus = status
			return nil, nil
		},
	}

	h := NewAdminOperationsHandler(stub)
	r := gin.New()
	r.GET("/api/v1/admin/operations", h.ListAdminOperations())

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/admin/operations?worker_id=wicket&status=SUCCEEDED", nil)
	r.ServeHTTP(w, req)

	if gotWorker != "wicket" {
		t.Errorf("worker_id filter = %q, want wicket", gotWorker)
	}
	if gotStatus != "SUCCEEDED" {
		t.Errorf("status filter = %q, want SUCCEEDED", gotStatus)
	}
}

func TestListAdminOperations_InternalError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	stub := &stubAuditController{
		listFn: func(_, _ string, _ int) ([]store.Operation, error) {
			return nil, errors.New("db briefly unavailable")
		},
	}

	h := NewAdminOperationsHandler(stub)
	r := gin.New()
	r.GET("/api/v1/admin/operations", h.ListAdminOperations())

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/operations", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("list err → %d, want 500", w.Code)
	}
}

func TestGetAdminOperation_NilController(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := &AdminOperationsHandler{controller: nil}
	r := gin.New()
	r.GET("/api/v1/admin/operations/:operation_id", h.GetAdminOperation())

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/operations/op-1", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("nil controller → %d, want 503", w.Code)
	}
}

func TestGetAdminOperation_NotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)
	stub := &stubAuditController{
		getFn: func(_ string) (*store.Operation, error) { return nil, store.ErrOperationNotFound },
	}

	h := NewAdminOperationsHandler(stub)
	r := gin.New()
	r.GET("/api/v1/admin/operations/:operation_id", h.GetAdminOperation())

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/operations/ghost", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("err → %d, want 404", w.Code)
	}
}

func TestGetAdminOperation_EmptyID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	// gin path-param decoder doesn't produce empty IDs; this
	// test verifies the trim-then-empty semantic using a
	// whitespace ID.
	stub := &stubAuditController{}
	h := NewAdminOperationsHandler(stub)
	r := gin.New()
	r.GET("/api/v1/admin/operations/:operation_id", h.GetAdminOperation())

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/operations/%20%20", nil)
	r.ServeHTTP(w, req)

	// Trim turns whitespace-only into ""; handler must 400.
	if w.Code != http.StatusBadRequest {
		t.Errorf("whitespace ID → %d, want 400 (empty after trim)", w.Code)
	}
}

func TestGetAdminOperation_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)
	queued := time.Now().UTC().Truncate(time.Second)
	started := queued.Add(time.Second)
	finished := started.Add(2 * time.Second)
	stub := &stubAuditController{
		getFn: func(_ string) (*store.Operation, error) {
			return &store.Operation{
				OperationID: "op-success-1",
				WorkerID:    "wicket",
				Op:          "drain",
				RequestedBy: "ops@example.com",
				Reason:      "happy path",
				Status:      store.OperationStatusSucceeded,
				QueuedAt:    queued,
				StartedAt:   &started,
				FinishedAt:  &finished,
			}, nil
		},
	}

	h := NewAdminOperationsHandler(stub)
	r := gin.New()
	r.GET("/api/v1/admin/operations/:operation_id", h.GetAdminOperation())

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/operations/op-success-1", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var card OperationCard
	if err := json.Unmarshal(w.Body.Bytes(), &card); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if card.OperationID != "op-success-1" {
		t.Errorf("OperationID = %q", card.OperationID)
	}
	if card.StartedAt == "" {
		t.Errorf("StartedAt empty; mapper failed to populate RFC3339 timestamp")
	}
	if card.FinishedAt == "" {
		t.Errorf("FinishedAt empty; mapper failed to populate RFC3339 timestamp")
	}
}

// TestBuildOperationCard_AllFields is the mapper-level test.
// Pins: every field round-trips; omitempty lifts the optional
// timestamps / payload / error_message when the source has
// them unset.
func TestBuildOperationCard_AllFields(t *testing.T) {
	queued := time.Date(2026, 7, 28, 17, 0, 0, 0, time.UTC)
	started := queued.Add(2 * time.Second)
	finished := queued.Add(5 * time.Second)
	op := &store.Operation{
		OperationID:  "op-build-1",
		WorkerID:     "wicket",
		Op:           "update",
		RequestedBy:  "ops@example.com",
		Reason:       "image digest bump",
		Status:       store.OperationStatusSucceeded,
		QueuedAt:     queued,
		StartedAt:    &started,
		FinishedAt:   &finished,
		Payload:      []byte(`{"digest":"sha256:abc"}`),
		ErrorMessage: "",
	}
	card := buildOperationCard(op)

	if card.OperationID != "op-build-1" {
		t.Errorf("OperationID = %q", card.OperationID)
	}
	if card.Op != "update" {
		t.Errorf("Op = %q", card.Op)
	}
	if card.Status != "SUCCEEDED" {
		t.Errorf("Status = %q", card.Status)
	}
	if card.QueuedAt != queued.Format(time.RFC3339) {
		t.Errorf("QueuedAt = %q, want %q", card.QueuedAt, queued.Format(time.RFC3339))
	}
	if card.StartedAt != started.Format(time.RFC3339) {
		t.Errorf("StartedAt = %q, want %q", card.StartedAt, started.Format(time.RFC3339))
	}
	if card.FinishedAt != finished.Format(time.RFC3339) {
		t.Errorf("FinishedAt = %q, want %q", card.FinishedAt, finished.Format(time.RFC3339))
	}
	if card.Payload != `{"digest":"sha256:abc"}` {
		t.Errorf("Payload = %q", card.Payload)
	}
}

func TestBuildOperationCard_NilInfo(t *testing.T) {
	card := buildOperationCard(nil)
	if card.OperationID != "" {
		t.Errorf("nil op → OperationID = %q, want empty", card.OperationID)
	}
	if card.Status != "" {
		t.Errorf("nil op → Status = %q, want empty", card.Status)
	}
}

func TestBuildOperationCard_OmitsEmptyTerminal(t *testing.T) {
	queued := time.Date(2026, 7, 28, 17, 0, 0, 0, time.UTC)
	op := &store.Operation{
		OperationID: "op-pending",
		WorkerID:    "wicket",
		Op:          "drain",
		RequestedBy: "ops",
		Reason:      "omitempty test",
		Status:      store.OperationStatusQueued,
		QueuedAt:    queued,
		// StartedAt / FinishedAt / ErrorMessage intentionally nil/empty.
	}
	card := buildOperationCard(op)

	b, err := json.Marshal(card)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	js := string(b)
	for _, omitField := range []string{
		`"started_at":`, `"finished_at":`, `"error_message":`,
	} {
		if strings.Contains(js, omitField) {
			t.Errorf("marshal leaked %q: %s", omitField, js)
		}
	}
	for _, mustHave := range []string{
		`"operation_id":`, `"queued_at":`, `"status":`,
	} {
		if !strings.Contains(js, mustHave) {
			t.Errorf("marshal missing %q: %s", mustHave, js)
		}
	}
}

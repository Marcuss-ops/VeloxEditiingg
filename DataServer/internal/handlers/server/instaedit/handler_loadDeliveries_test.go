package instaedit

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"velox-server/internal/instaeditauth"
	"velox-server/internal/store"
)

// failingStore is a storeReader implementation that lets tests force
// failures on the delivery-related store calls.
type failingStore struct {
	listJobDeliveriesResult   []store.JobDelivery
	listJobDeliveriesErr      error
	getDeliveryDestinationErr error
}

func (f *failingStore) ListJobsByWorkspace(ctx context.Context, workspaceID int64, limit int) ([]map[string]any, error) {
	return nil, nil
}

func (f *failingStore) GetJobByWorkspace(ctx context.Context, jobID string, workspaceID int64) (map[string]any, error) {
	return map[string]any{"job_id": jobID}, nil
}

func (f *failingStore) ListWorkersByWorkspace(workspaceID int64) ([]map[string]any, error) {
	return nil, nil
}

func (f *failingStore) GetWorkerByWorkspace(workerID string, workspaceID int64) (map[string]any, error) {
	return nil, nil
}

func (f *failingStore) GetDeliveryDestinationByExternalID(ctx context.Context, externalID string) (*store.DeliveryDestination, error) {
	return nil, nil
}

func (f *failingStore) ListJobDeliveriesByJob(jobID string) ([]store.JobDelivery, error) {
	if f.listJobDeliveriesErr != nil {
		return nil, f.listJobDeliveriesErr
	}
	return f.listJobDeliveriesResult, nil
}

func (f *failingStore) GetDeliveryDestination(ctx context.Context, destID string) (*store.DeliveryDestination, error) {
	if f.getDeliveryDestinationErr != nil {
		return nil, f.getDeliveryDestinationErr
	}
	return &store.DeliveryDestination{ExternalDestinationID: "ext-" + destID}, nil
}

func newFailingRouter(mock storeReader) *gin.Engine {
	gin.SetMode(gin.TestMode)
	v, _ := instaeditauth.New(testSecret)
	h := NewHandler(HandlerDeps{Verifier: v, Store: mock})
	r := gin.New()
	h.RegisterRoutes(r)
	return r
}

func TestGetJob_ListDeliveriesFailure_Returns500(t *testing.T) {
	mock := &failingStore{listJobDeliveriesErr: errors.New("db: connection lost")}
	r := newFailingRouter(mock)
	token := mintToken(t, validClaims())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/instaedit/jobs/job-123", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 when ListJobDeliveriesByJob fails, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetJob_GetDestinationFailure_Returns500(t *testing.T) {
	mock := &failingStore{
		listJobDeliveriesResult: []store.JobDelivery{{DeliveryID: "d-1", DestinationID: "dest-1"}},
		getDeliveryDestinationErr: errors.New("db: destination lookup failed"),
	}
	r := newFailingRouter(mock)
	token := mintToken(t, validClaims())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/instaedit/jobs/job-123", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 when GetDeliveryDestination fails, got %d: %s", w.Code, w.Body.String())
	}
}

func TestListJobDeliveries_ListDeliveriesFailure_Returns500(t *testing.T) {
	mock := &failingStore{listJobDeliveriesErr: errors.New("db: connection lost")}
	r := newFailingRouter(mock)
	token := mintToken(t, validClaims())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/instaedit/jobs/job-123/deliveries", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 when ListJobDeliveriesByJob fails, got %d: %s", w.Code, w.Body.String())
	}
}

func TestListJobDeliveries_GetDestinationFailure_Returns500(t *testing.T) {
	mock := &failingStore{
		listJobDeliveriesResult: []store.JobDelivery{{DeliveryID: "d-1", DestinationID: "dest-1"}},
		getDeliveryDestinationErr: errors.New("db: destination lookup failed"),
	}
	r := newFailingRouter(mock)
	token := mintToken(t, validClaims())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/instaedit/jobs/job-123/deliveries", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 when GetDeliveryDestination fails, got %d: %s", w.Code, w.Body.String())
	}
}

func TestListJobDeliveries_Success_ReturnsDeliveries(t *testing.T) {
	mock := &failingStore{
		listJobDeliveriesResult: []store.JobDelivery{
			{DeliveryID: "d-1", DestinationID: "dest-1", Status: "PENDING", RemoteID: "remote-1", RemoteURL: "https://example.com"},
		},
	}
	r := newFailingRouter(mock)
	token := mintToken(t, validClaims())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/instaedit/jobs/job-123/deliveries", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp listDeliveriesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Deliveries) != 1 {
		t.Fatalf("expected 1 delivery, got %d", len(resp.Deliveries))
	}
	if resp.Deliveries[0].SocialDeliveryID != "d-1" {
		t.Fatalf("expected delivery id d-1, got %s", resp.Deliveries[0].SocialDeliveryID)
	}
}

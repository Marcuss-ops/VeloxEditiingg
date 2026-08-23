package validation

import (
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"velox-server/internal/config"
	"velox-server/internal/handlers/server/api"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

func TestInjectedHandlersAreIsolated(t *testing.T) {
	t.Parallel()

	first := &fakeValidationRepository{}
	second := &fakeValidationRepository{}

	firstHandler := NewHandler(first)
	secondHandler := NewHandler(second)

	firstResponse := newValidationRequest(t, http.MethodPost, "/api/workers/validation", "/api/workers/validation", `{"worker_id":"worker-a","validation_code":"PASS"}`, firstHandler.HandleValidationReport())
	secondResponse := newValidationRequest(t, http.MethodPost, "/api/workers/validation", "/api/workers/validation", `{"worker_id":"worker-b","validation_code":"MISSING_UNIT"}`, secondHandler.HandleValidationReport())

	require.Equal(t, http.StatusOK, firstResponse.Code)
	require.Equal(t, http.StatusOK, secondResponse.Code)
	firstPayload := decodeResponse(t, firstResponse)
	secondPayload := decodeResponse(t, secondResponse)
	require.Equal(t, true, firstPayload["ok"])
	require.Equal(t, true, firstPayload["valid"])
	require.Equal(t, "PASS", firstPayload["code"])
	require.Equal(t, false, secondPayload["valid"])
	require.Equal(t, "MISSING_UNIT", secondPayload["code"])
	require.NotNil(t, first.saved)
	require.NotNil(t, second.saved)
	require.Equal(t, "worker-a", first.saved.WorkerID)
	require.Equal(t, "worker-b", second.saved.WorkerID)
	require.Equal(t, "PASS", first.saved.ValidationCode)
	require.Equal(t, "MISSING_UNIT", second.saved.ValidationCode)
}

func TestValidationHandlersHandleConcurrentPOSTRequests(t *testing.T) {
	t.Parallel()

	const requestCount = 32
	repository := &fakeValidationRepository{}
	handler := NewHandler(repository)
	responses := make(chan *httptest.ResponseRecorder, requestCount)

	var wg sync.WaitGroup
	wg.Add(requestCount)
	for i := 0; i < requestCount; i++ {
		workerID := fmt.Sprintf("worker-%d", i)
		go func() {
			defer wg.Done()
			responses <- newValidationRequest(
				t,
				http.MethodPost,
				"/api/workers/validation",
				"/api/workers/validation",
				fmt.Sprintf(`{"worker_id":%q,"validation_code":"PASS"}`, workerID),
				handler.HandleValidationReport(),
			)
		}()
	}
	wg.Wait()
	close(responses)

	for response := range responses {
		require.Equal(t, http.StatusOK, response.Code)
		payload := decodeResponse(t, response)
		require.Equal(t, true, payload["ok"])
		require.Equal(t, true, payload["valid"])
		require.Equal(t, "PASS", payload["code"])
	}

	saveCalls, getCalls := repository.callCounts()
	require.Equal(t, requestCount, saveCalls)
	require.Zero(t, getCalls)

	savedIDs := repository.savedIDs()
	require.Len(t, savedIDs, requestCount)
	for i := 0; i < requestCount; i++ {
		_, ok := savedIDs[fmt.Sprintf("worker-%d", i)]
		require.True(t, ok, "worker-%d was not persisted", i)
	}
}

func TestHandleValidationReportPreservesHTTPContract(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		code int
	}{
		{name: "invalid json", body: "{", code: http.StatusBadRequest},
		{name: "missing worker", body: `{"validation_code":"PASS"}`, code: http.StatusBadRequest},
		{name: "missing code", body: `{"worker_id":"worker-a"}`, code: http.StatusBadRequest},
		{name: "valid report", body: `{"worker_id":"worker-a","validation_code":"PASS"}`, code: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			response := newValidationRequest(t, http.MethodPost, "/api/workers/validation", "/api/workers/validation", tt.body, NewHandler(&fakeValidationRepository{}).HandleValidationReport())
			require.Equal(t, tt.code, response.Code)
		})
	}
}

func TestHandleValidationReportRejectsMismatchedAuthenticatedWorker(t *testing.T) {
	t.Parallel()

	repository := &fakeValidationRepository{}
	router := gin.New()
	router.POST("/api/workers/validation", func(c *gin.Context) {
		c.Set("authenticated_worker_id", "worker-authenticated")
		NewHandler(repository).HandleValidationReport()(c)
	})
	request := httptest.NewRequest(http.MethodPost, "/api/workers/validation", strings.NewReader(`{"worker_id":"worker-other","validation_code":"PASS"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	require.Equal(t, http.StatusForbidden, response.Code)
	require.Empty(t, repository.savedIDs())
}

func TestValidationRouteBindsRealWorkerTokenIdentity(t *testing.T) {
	t.Parallel()

	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "auth-validation.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	tokenManager := workersreg.NewTokenManager(db)
	token := tokenManager.GenerateToken("worker-authenticated")
	repository := &fakeValidationRepository{}
	router := gin.New()
	router.POST("/api/v1/agent/validation", api.WorkerOrAdminAuthMiddleware(&config.Config{}, tokenManager), NewHandler(repository).HandleValidationReport())

	request := httptest.NewRequest(http.MethodPost, "/api/v1/agent/validation", strings.NewReader(`{"worker_id":"worker-other","validation_code":"PASS"}`))
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	require.Equal(t, http.StatusForbidden, response.Code)
	require.Empty(t, repository.savedIDs())

	request = httptest.NewRequest(http.MethodPost, "/api/v1/agent/validation", strings.NewReader(`{"worker_id":"worker-authenticated","validation_code":"PASS"}`))
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	router.ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code)
}

func TestAdminValidationRouteAllowsExplicitRemediation(t *testing.T) {
	t.Parallel()

	repository := &fakeValidationRepository{}
	cfg := &config.Config{Auth: config.AuthConfig{AdminToken: "admin-token"}}
	router := gin.New()
	router.POST("/api/v1/agent/validation", api.WorkerOrAdminAuthMiddleware(cfg, nil), NewHandler(repository).HandleValidationReport())

	request := httptest.NewRequest(http.MethodPost, "/api/v1/agent/validation", strings.NewReader(`{"worker_id":"worker-other","validation_code":"PASS"}`))
	request.Header.Set("Authorization", "Bearer admin-token")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, map[string]struct{}{"worker-other": {}}, repository.savedIDs())
}

func TestHandleValidationReportReturns500OnRepositorySaveError(t *testing.T) {
	t.Parallel()

	response := newValidationRequest(t, http.MethodPost, "/api/workers/validation", "/api/workers/validation", `{"worker_id":"worker-a","validation_code":"PASS"}`, NewHandler(&fakeValidationRepository{
		saveErr: errors.New("write failed"),
	}).HandleValidationReport())

	payload := decodeResponse(t, response)
	require.Equal(t, http.StatusInternalServerError, response.Code)
	require.Equal(t, false, payload["ok"])
	require.Equal(t, "failed to persist validation report", payload["error"])
}

func TestGetWorkerValidationHandlerReturnsValidationResults(t *testing.T) {
	t.Parallel()

	notValidated := newValidationRequest(t, http.MethodGet, "/api/workers/:id/validation", "/api/workers/worker-b/validation", "", NewHandler(&fakeValidationRepository{}).GetWorkerValidationHandler())
	require.Equal(t, http.StatusOK, notValidated.Code)
	notValidatedPayload := decodeResponse(t, notValidated)
	require.Equal(t, "worker-b", notValidatedPayload["worker_id"])
	require.Equal(t, false, notValidatedPayload["valid"])
	require.Equal(t, "NOT_VALIDATED", notValidatedPayload["code"])
	require.Equal(t, "Worker has not been validated yet", notValidatedPayload["message"])

	validatedAt := time.Date(2026, time.August, 10, 0, 0, 0, 0, time.UTC)
	validated := newValidationRequest(t, http.MethodGet, "/api/workers/:id/validation", "/api/workers/worker-c/validation", "", NewHandler(&fakeValidationRepository{
		status: &ValidationStatus{
			WorkerID:       "worker-c",
			ValidationCode: "PASS",
			CanonicalUnit:  "velox-worker.service",
			ExecStart:      "/usr/bin/velox-worker",
			ValidatedAt:    validatedAt,
		},
	}).GetWorkerValidationHandler())
	require.Equal(t, http.StatusOK, validated.Code)
	validatedPayload := decodeResponse(t, validated)
	require.Equal(t, "worker-c", validatedPayload["worker_id"])
	require.Equal(t, true, validatedPayload["valid"])
	require.Equal(t, "PASS", validatedPayload["code"])
	require.Equal(t, "velox-worker.service", validatedPayload["canonical_unit"])
	require.Equal(t, "/usr/bin/velox-worker", validatedPayload["exec_start"])
}

func TestGetWorkerValidationHandlerReturnsRepositoryError(t *testing.T) {
	t.Parallel()

	response := newValidationRequest(t, http.MethodGet, "/api/workers/:id/validation", "/api/workers/worker-a/validation", "", NewHandler(&fakeValidationRepository{
		getErr: errors.New("repository unavailable"),
	}).GetWorkerValidationHandler())

	require.Equal(t, http.StatusInternalServerError, response.Code)
	require.Contains(t, response.Body.String(), "repository unavailable")
}

func TestGetAllValidationsHandlerUsesInjectedRepository(t *testing.T) {
	t.Parallel()

	repository := &fakeValidationRepository{
		all: []map[string]any{{"worker_id": "worker-a", "validation_code": "PASS"}},
	}
	response := newValidationRequest(t, http.MethodGet, "/api/workers/validations", "/api/workers/validations", "", NewHandler(repository).GetAllValidationsHandler())

	require.Equal(t, http.StatusOK, response.Code)
	payload := decodeResponse(t, response)
	require.Equal(t, true, payload["ok"])
	require.Len(t, payload["validations"], 1)

	errorResponse := newValidationRequest(t, http.MethodGet, "/api/workers/validations", "/api/workers/validations", "", NewHandler(&fakeValidationRepository{
		listErr: errors.New("list failed"),
	}).GetAllValidationsHandler())
	require.Equal(t, http.StatusInternalServerError, errorResponse.Code)
	require.Contains(t, errorResponse.Body.String(), "list failed")
}

func TestHandlerRejectsMissingRepository(t *testing.T) {
	t.Parallel()

	post := newValidationRequest(t, http.MethodPost, "/api/workers/validation", "/api/workers/validation", `{"worker_id":"worker-a","validation_code":"PASS"}`, NewHandler(nil).HandleValidationReport())
	require.Equal(t, http.StatusServiceUnavailable, post.Code)

	get := newValidationRequest(t, http.MethodGet, "/api/workers/:id/validation", "/api/workers/worker-a/validation", "", NewHandler(nil).GetWorkerValidationHandler())
	require.Equal(t, http.StatusServiceUnavailable, get.Code)

	list := newValidationRequest(t, http.MethodGet, "/api/workers/validations", "/api/workers/validations", "", NewHandler(nil).GetAllValidationsHandler())
	require.Equal(t, http.StatusServiceUnavailable, list.Code)

}

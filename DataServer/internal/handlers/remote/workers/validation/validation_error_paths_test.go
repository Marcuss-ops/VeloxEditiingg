package validation

import (
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"velox-server/internal/store"
)

func TestValidationHandlersReturnSQLiteErrors(t *testing.T) {
	t.Parallel()

	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "validation.db"))
	require.NoError(t, err)
	repository := NewValidationStore(db)
	handler := NewHandler(repository)
	require.NoError(t, db.Close())

	postResponse := newValidationRequest(t, http.MethodPost, "/api/workers/validation", "/api/workers/validation", `{"worker_id":"worker-closed","validation_code":"PASS"}`, handler.HandleValidationReport())
	postPayload := decodeResponse(t, postResponse)
	require.Equal(t, http.StatusInternalServerError, postResponse.Code)
	require.Equal(t, false, postPayload["ok"])
	require.Equal(t, "failed to persist validation report", postPayload["error"])

	getResponse := newValidationRequest(t, http.MethodGet, "/api/workers/:id/validation", "/api/workers/worker-closed/validation", "", handler.GetWorkerValidationHandler())
	require.Equal(t, http.StatusInternalServerError, getResponse.Code)
	require.Contains(t, getResponse.Body.String(), "database is closed")

	listResponse := newValidationRequest(t, http.MethodGet, "/api/workers/validations", "/api/workers/validations", "", handler.GetAllValidationsHandler())
	require.Equal(t, http.StatusInternalServerError, listResponse.Code)
	require.Contains(t, listResponse.Body.String(), "database is closed")

}

func TestValidationHandlersHandleConcurrentGET(t *testing.T) {
	t.Parallel()

	const requestCount = 32
	repository := &fakeValidationRepository{status: &ValidationStatus{
		WorkerID:       "worker-a",
		ValidationCode: "PASS",
	}}
	handler := NewHandler(repository)

	var wg sync.WaitGroup
	wg.Add(requestCount)
	responses := make(chan *httptest.ResponseRecorder, requestCount)
	for i := 0; i < requestCount; i++ {
		go func() {
			defer wg.Done()
			responses <- newValidationRequest(
				t,
				http.MethodGet,
				"/api/workers/:id/validation",
				"/api/workers/worker-a/validation",
				"",
				handler.GetWorkerValidationHandler(),
			)
		}()
	}
	wg.Wait()
	close(responses)

	for response := range responses {
		require.Equal(t, http.StatusOK, response.Code)
		payload := decodeResponse(t, response)
		require.Equal(t, true, payload["valid"])
		require.Equal(t, "PASS", payload["code"])
	}
	saveCalls, getCalls := repository.callCounts()
	require.Zero(t, saveCalls)
	require.Equal(t, requestCount, getCalls)

}

func TestValidationStoreTypedNilRepositoryFailsClosed(t *testing.T) {
	t.Parallel()

	var nilStore *ValidationStore
	var repository ValidationRepository = nilStore
	handler := NewHandler(repository)

	post := newValidationRequest(t, http.MethodPost, "/api/workers/validation", "/api/workers/validation", `{"worker_id":"worker-a","validation_code":"PASS"}`, handler.HandleValidationReport())
	require.Equal(t, http.StatusInternalServerError, post.Code)

	get := newValidationRequest(t, http.MethodGet, "/api/workers/:id/validation", "/api/workers/worker-a/validation", "", handler.GetWorkerValidationHandler())
	require.Equal(t, http.StatusInternalServerError, get.Code)

	list := newValidationRequest(t, http.MethodGet, "/api/workers/validations", "/api/workers/validations", "", handler.GetAllValidationsHandler())
	require.Equal(t, http.StatusInternalServerError, list.Code)

}

package validation

import (
	"github.com/stretchr/testify/require"
	"net/http"
	"sync"
	"testing"
)

func TestValidationStoreHandlesConcurrentUpserts(t *testing.T) {
	t.Parallel()

	db := newMigratedValidationStore(t)
	repository := NewValidationStore(db)
	const writeCount = 24
	var wg sync.WaitGroup
	errorsCh := make(chan error, writeCount)
	wg.Add(writeCount)
	for i := 0; i < writeCount; i++ {
		code := "PASS"
		if i%2 == 0 {
			code = "MISSING_UNIT"
		}
		go func(code string) {
			defer wg.Done()
			errorsCh <- repository.SaveValidation(&ValidationReport{
				WorkerID:       "worker-concurrent",
				ValidationCode: code,
				Timestamp:      "2026-08-10T00:00:00Z",
			})
		}(code)
	}
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		require.NoError(t, err)
	}

	status, err := repository.GetValidation("worker-concurrent")
	require.NoError(t, err)
	require.NotNil(t, status)
	require.Contains(t, []string{"PASS", "MISSING_UNIT"}, status.ValidationCode)
}

func TestValidationStorePersistsPASSAndFailure(t *testing.T) {
	t.Parallel()

	db := newMigratedValidationStore(t)

	repository := NewValidationStore(db)
	handler := NewHandler(repository)

	passResponse := newValidationRequest(t, http.MethodPost, "/api/workers/validation", "/api/workers/validation", `{"worker_id":"worker-pass","validation_code":"PASS","timestamp":"2026-08-10T00:00:00Z"}`, handler.HandleValidationReport())
	require.Equal(t, http.StatusOK, passResponse.Code)
	passStatus, err := repository.GetValidation("worker-pass")
	require.NoError(t, err)
	require.NotNil(t, passStatus)
	require.Equal(t, "PASS", passStatus.ValidationCode)
	require.Empty(t, passStatus.FailureReason)

	failureResponse := newValidationRequest(t, http.MethodPost, "/api/workers/validation", "/api/workers/validation", `{"worker_id":"worker-failure","validation_code":"MISSING_UNIT","timestamp":"2026-08-10T00:00:00Z"}`, handler.HandleValidationReport())
	require.Equal(t, http.StatusOK, failureResponse.Code)
	failureStatus, err := repository.GetValidation("worker-failure")
	require.NoError(t, err)
	require.NotNil(t, failureStatus)
	require.Equal(t, "MISSING_UNIT", failureStatus.ValidationCode)
	require.Equal(t, "Canonical unit does not exist", failureStatus.FailureReason)
}

func TestUnknownWorkerReturnsNotValidated(t *testing.T) {
	t.Parallel()

	db := newMigratedValidationStore(t)

	response := newValidationRequest(t, http.MethodGet, "/api/workers/:id/validation", "/api/workers/never-seen/validation", "", NewHandler(NewValidationStore(db)).GetWorkerValidationHandler())
	payload := decodeResponse(t, response)

	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "never-seen", payload["worker_id"])
	require.Equal(t, false, payload["valid"])
	require.Equal(t, "NOT_VALIDATED", payload["code"])
}

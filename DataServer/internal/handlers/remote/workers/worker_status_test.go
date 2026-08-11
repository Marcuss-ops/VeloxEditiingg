package workers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

type failingWorkersRepository struct {
	store.WorkersRepository
}

func (failingWorkersRepository) ListWorkers() ([]map[string]any, error) {
	return nil, assertWorkerReadModelError{}
}

type assertWorkerReadModelError struct{}

func (assertWorkerReadModelError) Error() string { return "worker read model unavailable" }

func TestWorkersListFailsClosedWhenRepositoryReadFails(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/workers", WorkersList(workersreg.New(nil), failingWorkersRepository{}))

	req := httptest.NewRequest(http.MethodGet, "/workers", nil)
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)
	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want %d; body=%s", resp.Code, http.StatusServiceUnavailable, resp.Body.String())
	}
}

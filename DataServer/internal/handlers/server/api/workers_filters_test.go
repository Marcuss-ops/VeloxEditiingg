package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	workersreg "velox-server/internal/workers"
	"velox-shared/controltransport"

	"github.com/gin-gonic/gin"
)

func TestListWorkersAppliesDocumentedFilters(t *testing.T) {
	gin.SetMode(gin.TestMode)
	reg := workersreg.New(nil)
	if err := reg.Heartbeat(nil, "worker-a", "A", "", map[string]interface{}{
		"worker_class":  "cpu-xlarge",
		"rollout_group": "canary",
	}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Heartbeat(nil, "worker-b", "B", "", map[string]interface{}{
		"worker_class":  "cpu-small",
		"rollout_group": "stable",
	}); err != nil {
		t.Fatal(err)
	}

	r := gin.New()
	r.GET("/api/v1/workers", NewWorkersHandler(reg).ListWorkers())
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/workers?class=cpu-xlarge&status=DISCONNECTED&rollout_group=canary", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var resp WorkersListResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Workers) != 1 || resp.Workers[0].WorkerID != "worker-a" {
		t.Fatalf("filtered workers = %+v, want only worker-a", resp.Workers)
	}
}

func TestListWorkersRejectsInvalidStatusFilter(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/api/v1/workers", NewWorkersHandler(workersreg.New(nil)).ListWorkers())
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/workers?status=unknown", nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body = %s", w.Code, w.Body.String())
	}
}

func TestApplyFiltersMatchesTypedExecutorRegistry(t *testing.T) {
	registry, err := controltransport.NewExecutorRegistry(
		controltransport.ExecutorCapability{ID: "video.assemble.copy.v1", Version: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	workers := []workersreg.Worker{
		{WorkerID: "worker-a", Class: "cpu-xlarge", RolloutGroup: "canary", ExecutorCapabilities: registry},
		{WorkerID: "worker-b", Class: "cpu-xlarge", RolloutGroup: "canary"},
	}
	got := ApplyFilters(workers, Filters{Class: "cpu-xlarge", RolloutGroup: "canary", NeedsExecutor: "video.assemble.copy.v1@1"})
	if len(got) != 1 || got[0].WorkerID.String() != "worker-a" {
		t.Fatalf("filtered workers = %+v, want only worker-a", got)
	}
}

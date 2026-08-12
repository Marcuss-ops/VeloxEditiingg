package management

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

func closedRegistryWithWorker(t *testing.T) *workersreg.Registry {
	t.Helper()
	s, err := store.NewSQLiteStore(t.TempDir() + "/management.db")
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	reg := workersreg.New(s)
	if err := reg.RegisterWorker(context.Background(), "worker-management", "Worker Management", "127.0.0.1", nil); err != nil {
		t.Fatalf("register worker: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	return reg
}

func TestRenameWorkerFailsClosedWhenHeartbeatPersistenceFails(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/worker/rename", RenameWorker(closedRegistryWithWorker(t)))

	req := httptest.NewRequest(http.MethodPost, "/worker/rename", bytes.NewBufferString(`{"worker_id":"worker-management","new_name":"renamed"}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d; body=%s", resp.Code, http.StatusInternalServerError, resp.Body.String())
	}
}

func TestSetWorkerGroupFailsClosedWhenHeartbeatPersistenceFails(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/worker/set_group", SetWorkerGroup(closedRegistryWithWorker(t)))

	req := httptest.NewRequest(http.MethodPost, "/worker/set_group", bytes.NewBufferString(`{"worker_id":"worker-management","worker_group":"render"}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d; body=%s", resp.Code, http.StatusInternalServerError, resp.Body.String())
	}
}

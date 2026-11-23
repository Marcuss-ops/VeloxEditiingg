package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	workersreg "velox-server/internal/workers"
)

// TestWorkerDeleteBlockedReason pins the liveness gate: only a truly
// disconnected worker with no current job can be removed from the registry.
func TestWorkerDeleteBlockedReason(t *testing.T) {
	cases := []struct {
		name       string
		status     string
		currentJob string
		want       string
	}{
		{"disconnected", "DISCONNECTED", "", ""},
		{"case_insensitive", "disconnected", "", ""},
		{"connected", "CONNECTED", "", "worker is connected; drain and stop the agent before delete"},
		{"stale", "STALE", "", "worker heartbeat is stale; confirm the agent is stopped first"},
		{"draining", "DRAINING", "", "worker is draining; finish the drain before delete"},
		{"current_job", "DISCONNECTED", "job-1", "worker still reports a current job; wait for it to finish"},
		{"nil", "", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var info *workersreg.Worker
			if tc.name != "nil" {
				info = &workersreg.Worker{ConnectionStatus: tc.status, CurrentJob: tc.currentJob}
			}
			if got := workerDeleteBlockedReason(info); got != tc.want {
				t.Fatalf("workerDeleteBlockedReason() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDeleteAdminWorker_RemovesDisconnectedWorker(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	reg := workersreg.New(nil)
	if err := reg.RegisterWorker(ctx, "host_10_0_0_1", "worker-1", "10.0.0.1", nil); err != nil {
		t.Fatalf("register: %v", err)
	}
	h := NewAdminWorkersHandler(reg)
	r := gin.New()
	r.DELETE("/api/v1/admin/workers/:worker_id", h.DeleteAdminWorker())

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodDelete, "/api/v1/admin/workers/host_10_0_0_1", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", w.Code, w.Body.String())
	}
	if got := w.Body.String(); got != `{"deleted":true,"worker_id":"host_10_0_0_1"}` {
		t.Fatalf("body = %s", got)
	}
	if reg.GetWorker(ctx, "host_10_0_0_1") != nil {
		t.Fatal("worker still registered after delete")
	}

	// Second delete is a honest 404: the row is gone.
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodDelete, "/api/v1/admin/workers/host_10_0_0_1", nil))
	if w.Code != http.StatusNotFound {
		t.Fatalf("second delete status = %d, want 404", w.Code)
	}
}

func TestDeleteAdminWorker_NilRegistry(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := NewAdminWorkersHandler(nil)
	r := gin.New()
	r.DELETE("/api/v1/admin/workers/:worker_id", h.DeleteAdminWorker())
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodDelete, "/api/v1/admin/workers/host_10_0_0_1", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", w.Code)
	}
}

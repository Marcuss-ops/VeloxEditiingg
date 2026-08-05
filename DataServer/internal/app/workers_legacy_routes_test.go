package app

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	workersapi "velox-server/internal/handlers/remote/workers"
	"velox-server/internal/handlers/remote/workers/lifecycle"
)

func TestWorkersModule_RetiredBundleAndUpdateRoutesReturn404(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	cfg := &config.Config{}
	updateHandler := workersapi.NewWorkerUpdateHandler(cfg, nil, nil, nil, t.TempDir(), nil)
	workerLifecycle := lifecycle.NewHandler(cfg, nil, nil)
	m := NewWorkersModule(cfg, nil, workerLifecycle, updateHandler, nil, nil, nil)
	m.RegisterRoutes(r)

	cases := []struct {
		name   string
		method string
		path   string
	}{
		{name: "manifest generation", method: http.MethodPost, path: "/bundle/manifest/generate"},
		{name: "v2 manifest", method: http.MethodGet, path: "/api/worker/v2/manifest"},
		{name: "v2 chunk", method: http.MethodGet, path: "/api/worker/v2/chunk/example.chunk"},
		{name: "force bundle rebuild", method: http.MethodPost, path: "/install_worker/force_regenerate_zip"},
		{name: "full linux update", method: http.MethodPost, path: "/workers/full_update_linux"},
		{name: "latest bundle update", method: http.MethodPost, path: "/workers/update_all_latest_bundle"},
		{name: "worker requested update", method: http.MethodPost, path: "/worker/request_update"},
	}

	retired := make(map[string]struct{}, len(cases))
	for _, tc := range cases {
		retired[tc.method+" "+tc.path] = struct{}{}
	}
	for _, route := range r.Routes() {
		if _, found := retired[route.Method+" "+route.Path]; found {
			t.Fatalf("retired route remains registered: %s %s", route.Method, route.Path)
		}
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			if w.Code != http.StatusNotFound {
				t.Fatalf("%s %s returned HTTP %d, want 404; body=%s", tc.method, tc.path, w.Code, w.Body.String())
			}
		})
	}
}

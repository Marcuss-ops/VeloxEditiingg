package app

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// TestHealth_ReadyAliases verifies that the /health/ready and
// /api/health/ready aliases mirror the behaviour of /ready:
//   - 503 before MarkReady
//   - 200 after MarkReady
func TestHealth_ReadyAliases(t *testing.T) {
	gin.SetMode(gin.TestMode)

	m := NewHealthModule()
	r := gin.New()
	m.RegisterRoutes(r)

	paths := []string{"/ready", "/health/ready", "/api/ready", "/api/health/ready"}

	for _, path := range paths {
		path := path
		t.Run(path+"_before_ready", func(t *testing.T) {
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, path, nil)
			r.ServeHTTP(w, req)
			if w.Code != http.StatusServiceUnavailable {
				t.Fatalf("%s: expected 503 before MarkReady, got %d: %s", path, w.Code, w.Body.String())
			}
			var body map[string]any
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatalf("%s: invalid JSON body: %v", path, err)
			}
			if body["status"] != "not_ready" {
				t.Fatalf("%s: expected status=not_ready, got: %v", path, body)
			}
		})
	}

	m.MarkReady()

	for _, path := range paths {
		path := path
		t.Run(path+"_after_ready", func(t *testing.T) {
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, path, nil)
			r.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Fatalf("%s: expected 200 after MarkReady, got %d: %s", path, w.Code, w.Body.String())
			}
			var body map[string]any
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatalf("%s: invalid JSON body: %v", path, err)
			}
			if body["status"] != "ready" {
				t.Fatalf("%s: expected status=ready, got: %v", path, body)
			}
		})
	}
}

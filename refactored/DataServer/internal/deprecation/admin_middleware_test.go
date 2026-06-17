package deprecation_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/deprecation"
	"velox-server/internal/handlers/server/api"
)

// TestAdminAuthMiddlewareOnDeprecationStats mounts a real gin.Engine that
// mirrors the production wiring in registerDeprecationStatsRoute and asserts
// the contract:
//
//	- 401 Unauthorized when no Bearer is presented
//	- 401 Unauthorized when a wrong Bearer is presented
//	- 200 OK when the configured admin token is presented as Bearer
//	- 403 Forbidden when no admin token is configured at all
//	  (defense-in-depth: the route must NOT silently open up)
//
// It also asserts the snapshot body shape (one registered entry, valid JSON)
// on the success path — so a future regression that breaks the JSON contract
// is caught here, not in production.
func TestAdminAuthMiddlewareOnDeprecationStats(t *testing.T) {
	gin.SetMode(gin.TestMode)

	cases := []struct {
		name           string
		adminToken     string
		authHeader     string
		wantStatus     int
		wantStatsCount int // 0 when auth fails
	}{
		{
			name:       "no_authorization_header",
			adminToken: "test-admin-token-xyz",
			authHeader: "",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "wrong_bearer",
			adminToken: "test-admin-token-xyz",
			authHeader: "Bearer not-the-real-token",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:           "correct_bearer",
			adminToken:     "test-admin-token-xyz",
			authHeader:     "Bearer test-admin-token-xyz",
			wantStatus:     http.StatusOK,
			wantStatsCount: 1,
		},
		{
			name:       "admin_token_not_configured",
			adminToken: "", // operator never set VELOX_ADMIN_TOKEN
			authHeader: "Bearer anything",
			wantStatus: http.StatusForbidden,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel() // each subtest owns its own Registry + gin engine
			cfg := &config.Config{AdminToken: c.adminToken}
			now := time.Now().UTC()
			dep := deprecation.New(now, now.Add(72*time.Hour))
			dep.Register("GET", "/api/_internal/deprecation_stats", "")

			r := gin.New()
			internal := r.Group("/api/_internal")
			internal.Use(api.AdminAuthMiddleware(cfg))
			internal.GET("/deprecation_stats", func(c *gin.Context) {
				c.JSON(http.StatusOK, dep.Snapshot())
			})

			req := httptest.NewRequest(http.MethodGet, "/api/_internal/deprecation_stats", nil)
			// Non-loopback RemoteAddr so IsLocalRequestIP short-circuit cannot mask the test.
			req.RemoteAddr = "203.0.113.1:54321"
			if c.authHeader != "" {
				req.Header.Set("Authorization", c.authHeader)
			}
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			if w.Code != c.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", w.Code, c.wantStatus, w.Body.String())
			}

			if c.wantStatsCount == 0 {
				return
			}

			// Success path: assertion belt-and-suspenders:
			//   - Content-Type is JSON (gin emits this via c.JSON)
			//   - body round-trips through Snapshot
			if got := w.Header().Get("Content-Type"); got != "application/json; charset=utf-8" {
				t.Errorf("Content-Type = %q, want application/json; charset=utf-8", got)
			}
			var snap deprecation.Snapshot
			if err := json.Unmarshal(w.Body.Bytes(), &snap); err != nil {
				t.Fatalf("response body must be valid Snapshot JSON: %v (body: %s)", err, w.Body.String())
			}
			if len(snap.Stats) != c.wantStatsCount {
				t.Errorf("stats entries = %d, want %d", len(snap.Stats), c.wantStatsCount)
			}
		})
	}
}

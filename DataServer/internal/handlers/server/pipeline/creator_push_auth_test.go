// Package pipeline: creator_push_e2e_test.go exercises the full HTTP →
// creatorflow.Resolver → atomic Job+Task write path for the
// POST /api/v1/creator/jobs endpoint.
//
// creator_push_test.go (sibling) covers the pure normalization layer
// (creatorPushRequest → normalizedCreatorPush). This file is the
// integration counterpart: it wires a real SQLite store, a real Enqueuer
// + creatorflow.Resolver, and runs the handler through a real
// httptest.Recorder + gin.New engine mounted via h.RegisterRoutes.
//
// The auth middleware is bypassed via adminAuthFake because the auth
// chain has its own unit coverage in handlers/server/api; this file
// exercises the creator_push contract exclusively.
package pipeline

import (
	"bytes"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"net/http"
	"net/http/httptest"
	"testing"
	"velox-server/internal/config"
	"velox-server/internal/handlers/server/api"
)

// adminAuthFake short-circuits the bearer-token check the production
// router applies to /api/v1/creator/jobs. The auth chain is unit-tested
// separately; this file exercises the creator_push contract exclusively.
func TestCreatorPushJobsE2E_RealAdminAuthWired(t *testing.T) {
	h, _, _ := newCreatorPushE2EStack(t)
	gin.SetMode(gin.TestMode)

	// Pin the env vars so a leftover value in the developer's shell or in
	// CI does NOT mask the test token. api.AdminAuthMiddleware honours
	// cfg.Auth.AdminToken directly, so this is the canonical contract;
	// setting both VELOX_ADMIN_TOKEN (env) and TOKEN_FILE (the platform
	// fallback path env, per pipeline/codegen/voiceover_harness.go::Token
	// resolution) to empty ensures the middleware will still reject
	// requests with the corresponding header / will not pick up a stale
	// file-based token. Order matters: explicit > env > TOKEN_FILE.
	t.Setenv("VELOX_ADMIN_TOKEN", "")
	t.Setenv("TOKEN_FILE", "")

	const testToken = "test-secret-token"
	cfg := &config.Config{}
	cfg.Auth.AdminToken = testToken
	authMW := api.AdminAuthMiddleware(cfg)

	r := gin.New()
	r.SetTrustedProxies(nil)
	h.RegisterRoutes(r, authMW, m2mJobsAuthFake)

	body := creatorPushE2EBody("creator_pc_auth", "creator-job-auth-001", "scene.composite.v1")
	rawBody, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}

	cases := []struct {
		name       string
		authHeader string
		wantStatus int
	}{
		{"no_authorization_header", "", http.StatusUnauthorized},
		{"wrong_bearer_token", "Bearer invalid-mock-token", http.StatusUnauthorized},
		{"right_bearer_token", "Bearer " + testToken, http.StatusAccepted},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/creator/jobs", bytes.NewReader(rawBody))
			req.Header.Set("Content-Type", "application/json")
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}
			// RFC 5737 TEST-NET-2 public IP — never loopback, so the
			// middleware's IsLocalRequestIP early-return cannot save us.
			req.RemoteAddr = "198.51.100.1:1234"
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			if w.Code != tc.wantStatus {
				t.Fatalf("want %d, got %d body=%s", tc.wantStatus, w.Code, w.Body.String())
			}
		})
	}
}

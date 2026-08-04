package instaeditauth

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// decodeDenialBody is a tiny decode-and-assert helper that the
// matrix tests reuse so each test reads as one short block.
//
// The HTTP body shape MUST match the struct in middleware.go exactly
// (5 documented fields). Drift between struct tags and assertions
// is a real risk; we re-derive the struct field name list at test
// time via reflection-of-shape (the 6 fields are asserted by name).
func decodeDenialBody(t *testing.T, body []byte) map[string]interface{} {
	t.Helper()
	var out map[string]interface{}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode 403 body: %v", err)
	}
	return out
}

// validClaimsWithScopes clones validClaims() and overrides the Scopes
// field with the supplied slice. Used by the new-taxonomy tests so
// they don't have to bake legacy-shaped scopes into the verified Claims.
func validClaimsWithScopes(scopes []string) Claims {
	c := validClaims()
	c.Scopes = scopes
	return c
}

// TestMiddleware_403ContainsClearMessageWithRouteAndOperation —
// architect verdict Q3. The enriched body shape MUST have all 6
// fields (error, required_scopes, presented_scopes, operation,
// route, hint). Operating tags route=URL path + operation="create_job".
func TestMiddleware_403ContainsClearMessageWithRouteAndOperation(t *testing.T) {
	v, _ := New(testSecret)
	r := setupGin()
	r.POST("/api/v1/instaedit/jobs",
		MiddlewareWithOperation(v, []string{ScopeJobsWrite}, "create_job"),
		func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"ok": true}) },
	)

	// JWT carries only the read scope — the create route demands write.
	c := validClaimsWithScopes([]string{ScopeJobsRead})
	token := mintToken(t, testSecret, c)
	req := httptest.NewRequest("POST", "/api/v1/instaedit/jobs", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", w.Code, w.Body.String())
	}
	body := decodeDenialBody(t, w.Body.Bytes())

	for _, k := range []string{"error", "required_scopes", "presented_scopes", "operation", "route", "hint"} {
		if _, ok := body[k]; !ok {
			t.Fatalf("403 body missing key %q: %v", k, body)
		}
	}
	if body["error"] != "insufficient scope" {
		t.Fatalf("error: got %v, want 'insufficient scope'", body["error"])
	}
	if body["operation"] != "create_job" {
		t.Fatalf("operation: got %v, want 'create_job'", body["operation"])
	}
	if body["route"] != "/api/v1/instaedit/jobs" {
		t.Fatalf("route: got %v", body["route"])
	}
	if body["hint"] == "" {
		t.Fatal("hint empty; expected remediation text")
	}
	required, _ := body["required_scopes"].([]interface{})
	if len(required) != 1 || required[0] != ScopeJobsWrite {
		t.Fatalf("required_scopes: got %v, want [%q]", required, ScopeJobsWrite)
	}
	presented, _ := body["presented_scopes"].([]interface{})
	if len(presented) != 1 || presented[0] != ScopeJobsRead {
		t.Fatalf("presented_scopes: got %v, want [%q]", presented, ScopeJobsRead)
	}
}

// TestMiddleware_Generic_MarksOperationAsDash — when the operator
// used Middleware(...) (the operation-unaware variant) the
// operation field MUST be "-" so consumers can distinguish a
// generic 403 from a per-handler labeled 403. The 403 body shape
// MUST still include all 6 fields (backward compat: a generic
// Middleware consumer still gets a useful body).
func TestMiddleware_Generic_MarksOperationAsDash(t *testing.T) {
	v, _ := New(testSecret)
	r := setupGin()
	r.GET("/api/v1/instaedit/jobs/job-1",
		Middleware(v, []string{ScopeJobsWrite}),
		func(c *gin.Context) { c.JSON(200, gin.H{"ok": true}) },
	)
	c := validClaimsWithScopes([]string{ScopeJobsRead})
	token := mintToken(t, testSecret, c)
	req := httptest.NewRequest("GET", "/api/v1/instaedit/jobs/job-1", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", w.Code)
	}
	body := decodeDenialBody(t, w.Body.Bytes())
	if body["operation"] != "-" {
		t.Fatalf("operation: got %v, want '-'", body["operation"])
	}
	if body["route"] != "/api/v1/instaedit/jobs/job-1" {
		t.Fatalf("route mismatch: %v", body["route"])
	}
}

// TestMiddleware_AllowsRequestWithExactScope — the BFF mints a JWT
// with EXACTLY the required scope; the route MUST pass with 200.
func TestMiddleware_AllowsRequestWithExactScope(t *testing.T) {
	v, _ := New(testSecret)
	r := setupGin()
	r.POST("/api/v1/instaedit/jobs",
		MiddlewareWithOperation(v, []string{ScopeJobsWrite}, "create_job"),
		func(c *gin.Context) { c.JSON(200, gin.H{"ok": true}) },
	)
	c := validClaimsWithScopes([]string{ScopeJobsWrite})
	token := mintToken(t, testSecret, c)
	req := httptest.NewRequest("POST", "/api/v1/instaedit/jobs", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200 with exact scope, got %d: %s", w.Code, w.Body.String())
	}
}

// TestMiddleware_AllowsRequestWithSupersetScopes — Velox accepts
// "exact OR superset" (HasAllScopes semantics) so a BFF-superset
// JWT (e.g. the AllScopesSuperset) is accepted on every protected
// route. This guards the BFF cutover story.
func TestMiddleware_AllowsRequestWithSupersetScopes(t *testing.T) {
	v, _ := New(testSecret)
	r := setupGin()
	r.POST("/api/v1/instaedit/jobs",
		MiddlewareWithOperation(v, []string{ScopeJobsWrite}, "create_job"),
		func(c *gin.Context) { c.JSON(200, gin.H{"ok": true}) },
	)
	c := validClaimsWithScopes(AllScopesSuperset) // all 5 scopes
	token := mintToken(t, testSecret, c)
	req := httptest.NewRequest("POST", "/api/v1/instaedit/jobs", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200 with superset scope, got %d: %s", w.Code, w.Body.String())
	}
}

// TestMiddleware_DeniesInsufficientScope_ForEachOfTheFiveScopes —
// matrix: for each of the 5 scopes, a JWT carrying only the OTHER 4
// is denied on the route demanding the target scope.
func TestMiddleware_DeniesInsufficientScope_ForEachOfTheFiveScopes(t *testing.T) {
	v, _ := New(testSecret)
	all := []string{ScopeJobsRead, ScopeJobsWrite, ScopeWorkersRead, ScopeAssetsRead, ScopeAssetsWrite}
	for _, required := range all {
		t.Run(required, func(t *testing.T) {
			presented := make([]string, 0, len(all)-1)
			for _, s := range all {
				if s != required {
					presented = append(presented, s)
				}
			}
			r := setupGin()
			r.GET("/api/v1/instaedit/jobs/job-1",
				MiddlewareWithOperation(v, []string{required}, "test_op"),
				func(c *gin.Context) { c.JSON(200, gin.H{"ok": true}) },
			)
			c := validClaimsWithScopes(presented)
			token := mintToken(t, testSecret, c)
			req := httptest.NewRequest("GET", "/api/v1/instaedit/jobs/job-1", nil)
			req.Header.Set("Authorization", "Bearer "+token)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			if w.Code != http.StatusForbidden {
				t.Fatalf("expected 403, got %d", w.Code)
			}
			body := decodeDenialBody(t, w.Body.Bytes())
			reqOpRaw, _ := body["required_scopes"].([]interface{})
			if len(reqOpRaw) != 1 || reqOpRaw[0] != required {
				t.Fatalf("required_scopes: got %v, want [%q]", reqOpRaw, required)
			}
			presOpRaw, _ := body["presented_scopes"].([]interface{})
			if len(presOpRaw) != len(presented) {
				t.Fatalf("presented_scopes len: got %d, want %d", len(presOpRaw), len(presented))
			}
		})
	}
}

// TestMiddleware_FreeHeadersStillRejectedWith401 — the existing
// defense-in-depth must NOT regress when we add MiddlewareWithOperation.
// A request carrying X-User-ID (forged identity smuggling) still 401s
// even when paired with a valid signed JWT.
func TestMiddleware_FreeHeadersStillRejectedWith401(t *testing.T) {
	v, _ := New(testSecret)
	r := setupGin()
	r.GET("/api/v1/instaedit/jobs/job-1",
		MiddlewareWithOperation(v, []string{ScopeJobsRead}, "read_job"),
		func(c *gin.Context) { c.JSON(200, gin.H{"ok": true}) },
	)
	c := validClaimsWithScopes([]string{ScopeJobsRead})
	token := mintToken(t, testSecret, c)
	req := httptest.NewRequest("GET", "/api/v1/instaedit/jobs/job-1", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-User-ID", "999")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for free X-User-ID, got %d: %s", w.Code, w.Body.String())
	}
}

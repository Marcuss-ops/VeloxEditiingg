package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

const (
	securityAdminToken = "security-admin-token"
	securityWorkerID   = "security-worker-a"
)

func securityStoreAndWorkerToken(t *testing.T) (*store.SQLiteStore, *workersreg.TokenManager, string) {
	t.Helper()
	db, err := store.NewSQLiteStore(t.TempDir() + "/security.db")
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	tm := workersreg.NewTokenManager(db)
	return db, tm, tm.GenerateToken(securityWorkerID)
}

func securityAdminConfig() *config.Config {
	return &config.Config{Auth: config.AuthConfig{AdminToken: securityAdminToken}}
}

func securityAdminMutationRequest(t *testing.T, r *gin.Engine, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/workers/"+securityWorkerID+"/drain", strings.NewReader(`{"reason":"security test"}`))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func securityAdminMutationRoute(t *testing.T, reg *workersreg.Registry, pub ControllerPublisher) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := NewAdminWorkersMutationsHandler(reg, pub)
	r.POST("/api/v1/admin/workers/:worker_id/drain", AdminAuthMiddleware(securityAdminConfig()), h.DrainWorker())
	return r
}

func TestSecurity_AdminTokenMissingIsRejected(t *testing.T) {
	reg := newRegisteredRegistry(t, securityWorkerID)
	pub := &stubPublisher{}
	r := securityAdminMutationRoute(t, reg, pub)
	w := securityAdminMutationRequest(t, r, "")

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("missing admin token status=%d, want %d body=%s", w.Code, http.StatusUnauthorized, w.Body.String())
	}
	if len(pub.published) != 0 {
		t.Fatalf("missing admin token published %d operations, want 0", len(pub.published))
	}
	info := reg.GetWorker(context.Background(), securityWorkerID)
	if info == nil || info.Drain {
		t.Fatalf("missing admin token changed worker state: %+v", info)
	}
}

func TestSecurity_WorkerTokenCannotAccessAdminEndpoint(t *testing.T) {
	_, _, workerToken := securityStoreAndWorkerToken(t)
	reg := newRegisteredRegistry(t, securityWorkerID)
	pub := &stubPublisher{}
	r := securityAdminMutationRoute(t, reg, pub)
	w := securityAdminMutationRequest(t, r, workerToken)

	// AdminAuthMiddleware intentionally returns 401 for credentials that
	// are not valid admin credentials; the worker token must never reach
	// the real drain mutation handler.
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("worker token on admin endpoint status=%d, want %d body=%s", w.Code, http.StatusUnauthorized, w.Body.String())
	}
	if len(pub.published) != 0 {
		t.Fatalf("worker token on admin endpoint published %d operations, want 0", len(pub.published))
	}
	info := reg.GetWorker(context.Background(), securityWorkerID)
	if info == nil || info.Drain {
		t.Fatalf("worker token on admin endpoint changed worker state: %+v", info)
	}
}

func TestSecurity_RevokedWorkerTokenIsRejected(t *testing.T) {
	_, tm, workerToken := securityStoreAndWorkerToken(t)
	cfg := securityAdminConfig()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/api/v1/workers/protected", WorkerOrAdminAuthMiddleware(cfg, tm), func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})

	before := httptest.NewRecorder()
	beforeReq := httptest.NewRequest(http.MethodGet, "/api/v1/workers/protected", nil)
	beforeReq.Header.Set("Authorization", "Bearer "+workerToken)
	r.ServeHTTP(before, beforeReq)
	if before.Code != http.StatusNoContent {
		t.Fatalf("valid worker token status=%d, want %d body=%s", before.Code, http.StatusNoContent, before.Body.String())
	}

	tm.RevokeWorkerTokens(securityWorkerID)

	after := httptest.NewRecorder()
	afterReq := httptest.NewRequest(http.MethodGet, "/api/v1/workers/protected", nil)
	afterReq.Header.Set("Authorization", "Bearer "+workerToken)
	r.ServeHTTP(after, afterReq)
	if after.Code != http.StatusUnauthorized {
		t.Fatalf("revoked worker token status=%d, want %d body=%s", after.Code, http.StatusUnauthorized, after.Body.String())
	}
}

func TestSecurity_WorkerTokenMustMatchDeclaredWorkerID(t *testing.T) {
	_, tm, workerToken := securityStoreAndWorkerToken(t)

	if !workersreg.AuthorizeWorkerToken(tm, workerToken, securityWorkerID, "203.0.113.10") {
		t.Fatal("valid worker token was rejected for its owning WorkerID")
	}
	if workersreg.AuthorizeWorkerToken(tm, workerToken, "security-worker-forged", "203.0.113.10") {
		t.Fatal("worker token was accepted for a forged WorkerID")
	}

}

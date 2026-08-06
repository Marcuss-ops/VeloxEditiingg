package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"

	"velox-server/internal/protectedasset"
)

func TestPass6_Auth_WorkerToken_Returns200(t *testing.T) {
	db, err := store.NewSQLiteStore(t.TempDir() + "/worker-auth.db")
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	tokenMgr := workersreg.NewTokenManager(db)
	workerToken := tokenMgr.GenerateToken("pass6-worker")
	svc := newPass6Service(t, true, nil)

	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := NewProtectedAssetsHandler(svc)
	r.GET(testRoutePass6, WorkerOrAdminAuthMiddleware(&config.Config{}, tokenMgr), h.Snapshot())

	req := httptest.NewRequest(http.MethodGet, testRoutePass6, nil)
	req.Header.Set("Authorization", "Bearer "+workerToken)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("worker token status=%d want=200 body=%s", w.Code, w.Body.String())
	}
	var snapshot protectedasset.Snapshot
	if err := json.Unmarshal(w.Body.Bytes(), &snapshot); err != nil {
		t.Fatalf("unmarshal worker snapshot: %v body=%s", err, w.Body.String())
	}
	if snapshot.Version != 1 {
		t.Fatalf("worker snapshot version=%d want 1", snapshot.Version)
	}
}

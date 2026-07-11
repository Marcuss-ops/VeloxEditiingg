package assets

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

func TestServeAssetRequiresWorkerAuthentication(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tempDir := t.TempDir()

	db, err := store.NewSQLiteStore(filepath.Join(tempDir, "test.db"))
	if err != nil {
		t.Fatalf("failed to create sqlite store: %v", err)
	}
	defer db.Close()

	tokenMgr := workersreg.NewTokenManager(db)
	handler := NewHandler(&config.Config{Runtime: config.RuntimeConfig{DataDir: tempDir}}, tokenMgr, nil, nil)
	r := gin.New()
	r.GET("/api/v1/worker-assets/:asset_id", handler.ServeAsset())

	assetID := strings.Repeat("a", 64)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/worker-assets/"+assetID, nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestServeAssetRejectsTraversalAndUnknownAssets(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tempDir := t.TempDir()

	db, err := store.NewSQLiteStore(filepath.Join(tempDir, "test.db"))
	if err != nil {
		t.Fatalf("failed to create sqlite store: %v", err)
	}
	defer db.Close()

	tokenMgr := workersreg.NewTokenManager(db)
	token := tokenMgr.GenerateToken("worker-1")
	handler := NewHandler(&config.Config{Runtime: config.RuntimeConfig{DataDir: tempDir}}, tokenMgr, nil, nil)

	w := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(w)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/api/v1/worker-assets/../etc/passwd", nil)
	ctx.Request.Header.Set("Authorization", "Bearer "+token)
	ctx.Params = gin.Params{{Key: "asset_id", Value: "../etc/passwd"}}
	handler.ServeAsset()(ctx)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("want 400 for traversal, got %d body=%s", w.Code, w.Body.String())
	}

	r := gin.New()
	r.GET("/api/v1/worker-assets/:asset_id", handler.ServeAsset())
	req := httptest.NewRequest(http.MethodGet, "/api/v1/worker-assets/"+strings.Repeat("c", 64), nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503 for unknown asset when no asset service, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestServeAssetServiceUnavailable(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tempDir := t.TempDir()

	db, err := store.NewSQLiteStore(filepath.Join(tempDir, "test.db"))
	if err != nil {
		t.Fatalf("failed to create sqlite store: %v", err)
	}
	defer db.Close()

	tokenMgr := workersreg.NewTokenManager(db)
	token := tokenMgr.GenerateToken("worker-1")

	handler := NewHandler(&config.Config{Runtime: config.RuntimeConfig{DataDir: tempDir}}, tokenMgr, nil, nil)
	r := gin.New()
	r.GET("/api/v1/worker-assets/:asset_id", handler.ServeAsset())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/worker-assets/"+strings.Repeat("d", 64), nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503 when asset service unavailable, got %d body=%s", w.Code, w.Body.String())
	}
}

package assets

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

func newTestStoreAndTokenManager(t *testing.T) (*store.SQLiteStore, *workersreg.TokenManager) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test_workers.db")
	dbStore, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	t.Cleanup(func() { dbStore.Close() })
	return dbStore, workersreg.NewTokenManager(dbStore)
}

func TestServeAssetRequiresWorkerAuthentication(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tempDir := t.TempDir()
	assetID := strings.Repeat("a", 64)
	writeTestAsset(t, tempDir, assetID, []byte("voiceover-bytes"))

	_, tokenMgr := newTestStoreAndTokenManager(t)
	handler := NewHandler(&config.Config{Runtime: config.RuntimeConfig{DataDir: tempDir}}, tokenMgr)
	r := gin.New()
	r.GET("/api/v1/worker-assets/:asset_id", handler.ServeAsset())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/worker-assets/"+assetID, nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestServeAssetSupportsContentLengthTypeAndRange(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tempDir := t.TempDir()
	assetID := strings.Repeat("b", 64)
	assetBytes := []byte("0123456789abcdef")
	writeTestAsset(t, tempDir, assetID, assetBytes)

	_, tokenMgr := newTestStoreAndTokenManager(t)
	token := tokenMgr.GenerateToken("worker-1")

	handler := NewHandler(&config.Config{Runtime: config.RuntimeConfig{DataDir: tempDir}}, tokenMgr)
	r := gin.New()
	r.GET("/api/v1/worker-assets/:asset_id", handler.ServeAsset())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/worker-assets/"+assetID, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d body=%s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); ct == "" {
		t.Fatal("missing Content-Type header")
	}
	if cl := w.Header().Get("Content-Length"); cl == "" {
		t.Fatal("missing Content-Length header")
	}
	if got := w.Body.String(); got != string(assetBytes) {
		t.Fatalf("want full body, got %q", got)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/worker-assets/"+assetID, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Range", "bytes=0-3")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusPartialContent {
		t.Fatalf("want 206, got %d body=%s", w.Code, w.Body.String())
	}
	if cr := w.Header().Get("Content-Range"); cr == "" {
		t.Fatal("missing Content-Range header")
	}
	if got := w.Body.String(); got != "0123" {
		t.Fatalf("want range body 0123, got %q", got)
	}
}

func TestServeAssetRejectsTraversalAndUnknownAssets(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tempDir := t.TempDir()
	_, tokenMgr := newTestStoreAndTokenManager(t)
	token := tokenMgr.GenerateToken("worker-1")
	handler := NewHandler(&config.Config{Runtime: config.RuntimeConfig{DataDir: tempDir}}, tokenMgr)

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
	if w.Code != http.StatusNotFound {
		t.Fatalf("want 404 for unknown asset, got %d body=%s", w.Code, w.Body.String())
	}
}

func writeTestAsset(t *testing.T, dataDir, assetID string, content []byte) {
	t.Helper()
	assetDir := filepath.Join(dataDir, "worker_downloads", "assets", "audio")
	if err := os.MkdirAll(assetDir, 0o755); err != nil {
		t.Fatalf("mkdir asset dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(assetDir, assetID+".mp3"), content, 0o600); err != nil {
		t.Fatalf("write asset: %v", err)
	}
}

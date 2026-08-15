package assets

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	voiceoverassets "velox-server/internal/assets"
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
	r.GET("/api/v1/agent/assets/:asset_id", handler.ServeAsset())

	assetID := strings.Repeat("a", 64)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/agent/assets/"+assetID, nil)
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
	ctx.Request = httptest.NewRequest(http.MethodGet, "/api/v1/agent/assets/../etc/passwd", nil)
	ctx.Request.Header.Set("Authorization", "Bearer "+token)
	ctx.Params = gin.Params{{Key: "asset_id", Value: "../etc/passwd"}}
	handler.ServeAsset()(ctx)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("want 400 for traversal, got %d body=%s", w.Code, w.Body.String())
	}

	r := gin.New()
	r.GET("/api/v1/agent/assets/:asset_id", handler.ServeAsset())
	req := httptest.NewRequest(http.MethodGet, "/api/v1/agent/assets/"+strings.Repeat("c", 64), nil)
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
	r.GET("/api/v1/agent/assets/:asset_id", handler.ServeAsset())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/agent/assets/"+strings.Repeat("d", 64), nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503 when asset service unavailable, got %d body=%s", w.Code, w.Body.String())
	}
}

// seekableReadCloser mirrors the resolver staging contract: external/Drive
// resolvers return a reader that is both closable and seekable (a staged
// temp file in production). *strings.Reader supplies Seek via embedding.
type seekableReadCloser struct{ *strings.Reader }

func (seekableReadCloser) Close() error { return nil }

// driveRangeResolver is a minimal voiceoverassets.Resolver that serves a fixed
// byte string as a Drive source, so the handler's deferred-Drive path can be
// exercised without a real Drive integration.
type driveRangeResolver struct{ data string }

func (driveRangeResolver) Scheme() string   { return "drive" }
func (driveRangeResolver) ServerOnly() bool { return false }
func (r driveRangeResolver) Open(_ context.Context, _ string) (*voiceoverassets.Source, error) {
	return &voiceoverassets.Source{
		Reader:       seekableReadCloser{strings.NewReader(r.data)},
		MIMEType:     "video/mp4",
		ExpectedSize: int64(len(r.data)),
		SourceType:   "drive",
	}, nil
}

func TestServeAssetExternalSourceSupportsRange(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tempDir := t.TempDir()

	db, err := store.NewSQLiteStore(filepath.Join(tempDir, "test.db"))
	if err != nil {
		t.Fatalf("failed to create sqlite store: %v", err)
	}
	defer db.Close()

	tokenMgr := workersreg.NewTokenManager(db)
	token := tokenMgr.GenerateToken("worker-1")

	content := "0123456789abcdef0123456789abcdef" // 32 bytes
	svc := voiceoverassets.NewAssetService(nil, nil,
		voiceoverassets.NewResolverRegistry(driveRangeResolver{data: content}), nil)
	handler := NewHandler(&config.Config{Runtime: config.RuntimeConfig{DataDir: tempDir}}, tokenMgr, svc, nil)
	r := gin.New()
	r.GET("/api/v1/agent/assets/:asset_id", handler.ServeAsset())

	// Full GET returns the complete body with the resolver's Content-Type.
	req := httptest.NewRequest(http.MethodGet, "/api/v1/agent/assets/drive-file-123456", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("full GET: want 200, got %d body=%s", w.Code, w.Body.String())
	}
	if w.Body.String() != content {
		t.Fatalf("full GET body = %q, want %q", w.Body.String(), content)
	}
	if ct := w.Header().Get("Content-Type"); ct != "video/mp4" {
		t.Fatalf("Content-Type = %q, want video/mp4", ct)
	}

	// Range GET returns a 206 with the requested slice and Content-Range.
	req = httptest.NewRequest(http.MethodGet, "/api/v1/agent/assets/drive-file-123456", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Range", "bytes=4-11")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusPartialContent {
		t.Fatalf("range GET: want 206, got %d body=%s", w.Code, w.Body.String())
	}
	if w.Body.String() != content[4:12] {
		t.Fatalf("range body = %q, want %q", w.Body.String(), content[4:12])
	}
	if cr := w.Header().Get("Content-Range"); cr != "bytes 4-11/32" {
		t.Fatalf("Content-Range = %q, want bytes 4-11/32", cr)
	}
}

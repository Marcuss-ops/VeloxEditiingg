package instaedit

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/instaeditauth"
)

const testSecret = "this-is-a-32-byte-secret-for-test!"

func setupRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	v, _ := instaeditauth.New(testSecret)
	h := NewHandler(v)
	h.RegisterRoutes(r)
	return r
}

// mintToken signs a HS256 JWT with the given claims for testing.
func mintToken(t *testing.T, claims instaeditauth.Claims) string {
	t.Helper()
	header := map[string]string{"alg": "HS256", "typ": "JWT"}
	headerJSON, _ := json.Marshal(header)
	payloadJSON, _ := json.Marshal(claims)

	headerB64 := base64.RawURLEncoding.EncodeToString(headerJSON)
	payloadB64 := base64.RawURLEncoding.EncodeToString(payloadJSON)

	signingInput := headerB64 + "." + payloadB64
	mac := hmac.New(sha256.New, []byte(testSecret))
	mac.Write([]byte(signingInput))
	sigB64 := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return signingInput + "." + sigB64
}

func validClaims() instaeditauth.Claims {
	return instaeditauth.Claims{
		Issuer:      "instaedit",
		Audience:    "velox",
		Subject:     "123",
		WorkspaceID: 45,
		Scopes:      []string{"velox:jobs:read", "velox:jobs:write", "velox:workers:read", "velox:assets:read"},
		ExpiresAt:   time.Now().Add(5 * time.Minute).Unix(),
		JTI:         "test-jti",
	}
}

func TestInstaEditRoutes_MissingToken_401(t *testing.T) {
	r := setupRouter()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/instaedit/jobs", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
	}
}

func TestInstaEditRoutes_ValidToken_501(t *testing.T) {
	r := setupRouter()
	token := mintToken(t, validClaims())
	req := httptest.NewRequest(http.MethodGet, "/api/v1/instaedit/jobs", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501 (stub), got %d: %s", w.Code, w.Body.String())
	}
}

func TestInstaEditRoutes_MissingScope_403(t *testing.T) {
	r := setupRouter()
	claims := validClaims()
	claims.Scopes = []string{"velox:workers:read"} // no jobs write
	token := mintToken(t, claims)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/instaedit/jobs", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for missing scope, got %d: %s", w.Code, w.Body.String())
	}
}

func TestInstaEditRoutes_FreeIdentityHeader_401(t *testing.T) {
	r := setupRouter()
	token := mintToken(t, validClaims())
	req := httptest.NewRequest(http.MethodGet, "/api/v1/instaedit/workers", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-User-ID", "999")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for free X-User-ID, got %d: %s", w.Code, w.Body.String())
	}
}

func TestInstaEditRoutes_WrongAudience_401(t *testing.T) {
	r := setupRouter()
	claims := validClaims()
	claims.Audience = "wrong"
	token := mintToken(t, claims)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/instaedit/assets/asset-1", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for wrong audience, got %d: %s", w.Code, w.Body.String())
	}
}

func TestInstaEditRoutes_ExpiredToken_401(t *testing.T) {
	r := setupRouter()
	claims := validClaims()
	claims.ExpiresAt = time.Now().Add(-1 * time.Minute).Unix()
	token := mintToken(t, claims)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/instaedit/jobs", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for expired token, got %d: %s", w.Code, w.Body.String())
	}
}

func TestInstaEditRoutes_AssetRoute_RequiresAssetScope(t *testing.T) {
	r := setupRouter()
	claims := validClaims()
	claims.Scopes = []string{"velox:jobs:read"} // no assets read
	token := mintToken(t, claims)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/instaedit/assets/asset-1", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for missing asset scope, got %d: %s", w.Code, w.Body.String())
	}
}

func TestNotImplemented_Body(t *testing.T) {
	// Ensure the stub helper returns a JSON body so callers can
	// distinguish a protected-but-unimplemented endpoint from a
	// plain 404.
	r := setupRouter()
	token := mintToken(t, validClaims())
	req := httptest.NewRequest(http.MethodGet, "/api/v1/instaedit/jobs", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if !strings.Contains(w.Body.String(), "not implemented") {
		t.Fatalf("expected 'not implemented' in body, got: %s", w.Body.String())
	}
}

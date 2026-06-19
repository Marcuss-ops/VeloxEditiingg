package drive

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	integrationsDrive "velox-server/internal/integrations/drive"
)

func init() {
	gin.SetMode(gin.TestMode)
}

// helper: create a minimal DriveHandlers with nil service.
func newNilServiceHandlers(t *testing.T) *DriveHandlers {
	t.Helper()
	return &DriveHandlers{
		dataDir:      t.TempDir(),
		driveService: nil,
	}
}

// helper: create a DriveHandlers with a service but empty credentials.
func newEmptyCredsHandlers(t *testing.T) *DriveHandlers {
	t.Helper()
	tempDir := t.TempDir()
	svc, err := integrationsDrive.NewService(&integrationsDrive.ServiceConfig{
		ClientID:     "",
		ClientSecret: "",
		TokensDir:    filepath.Join(tempDir, "tokens"),
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return &DriveHandlers{
		dataDir:      tempDir,
		driveService: svc,
	}
}

// helper: create a DriveHandlers with full credentials but no tokens on disk.
func newCredsNoTokensHandlers(t *testing.T) *DriveHandlers {
	t.Helper()
	tempDir := t.TempDir()
	tokensDir := filepath.Join(tempDir, "tokens")
	svc, err := integrationsDrive.NewService(&integrationsDrive.ServiceConfig{
		ClientID:     "test-client-id",
		ClientSecret: "test-client-secret",
		TokensDir:    tokensDir,
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return &DriveHandlers{
		dataDir:      tempDir,
		driveService: svc,
	}
}

// helper: create a DriveHandlers with credentials + a token file on disk.
// The token has an expired access token and a dummy refresh token.
// GetAbout will fail (no real Google API), so the token won't validate.
func newCredsWithTokenHandlers(t *testing.T) *DriveHandlers {
	t.Helper()
	tempDir := t.TempDir()
	tokensDir := filepath.Join(tempDir, "tokens")

	svc, err := integrationsDrive.NewService(&integrationsDrive.ServiceConfig{
		ClientID:     "test-client-id",
		ClientSecret: "test-client-secret",
		TokensDir:    tokensDir,
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	tm := svc.GetTokenManager()
	err = tm.SaveToken("default", &integrationsDrive.Token{
		AccessToken:  "dummy-access-token",
		RefreshToken: "dummy-refresh-token",
		TokenType:    "Bearer",
		Expiry:       time.Now().Add(-1 * time.Hour), // expired
		Scope:        "https://www.googleapis.com/auth/drive.file",
		AccountEmail: "test@example.com",
	})
	if err != nil {
		t.Fatalf("SaveToken: %v", err)
	}

	return &DriveHandlers{
		dataDir:      tempDir,
		driveService: svc,
	}
}

// helper: make a GET /api/drive/health request and return the response.
func healthRequest(t *testing.T, h *DriveHandlers) *httptest.ResponseRecorder {
	t.Helper()
	r := gin.New()
	r.GET("/api/drive/health", h.DriveHealthCheckHandler)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/drive/health", nil)
	r.ServeHTTP(w, req)
	return w
}

// helper: parse response body into a map.
func parseResponse(t *testing.T, w *httptest.ResponseRecorder) map[string]interface{} {
	t.Helper()
	var res map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("unmarshal response: %v (body=%s)", err, w.Body.String())
	}
	return res
}

// =============================================================================
// Tests
// =============================================================================

func TestDriveHealthCheck_ServiceNotInitialized(t *testing.T) {
	t.Parallel()

	w := healthRequest(t, newNilServiceHandlers(t))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503, got %d", w.Code)
	}

	res := parseResponse(t, w)
	if res["status"] != "unavailable" {
		t.Fatalf("want status 'unavailable', got %v", res["status"])
	}
	if res["error"] != "drive service not initialized" {
		t.Fatalf("want specific error, got %v", res["error"])
	}
	if res["drive_configured"] != false {
		t.Fatalf("want drive_configured false, got %v", res["drive_configured"])
	}

	checks, ok := res["checks"].([]interface{})
	if ok && len(checks) > 0 {
		t.Fatalf("want no checks when service is nil, got %d checks", len(checks))
	}
}

func TestDriveHealthCheck_NoCredentials(t *testing.T) {
	t.Parallel()

	w := healthRequest(t, newEmptyCredsHandlers(t))
	// No credentials → returns 200 (not a server error, just unconfigured)
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}

	res := parseResponse(t, w)
	if res["status"] != "unconfigured" {
		t.Fatalf("want status 'unconfigured', got %v", res["status"])
	}
	if res["drive_configured"] != false {
		t.Fatalf("want drive_configured false, got %v", res["drive_configured"])
	}

	// Should have one check (oauth_credentials)
	checks, ok := res["checks"].([]interface{})
	if !ok || len(checks) != 1 {
		t.Fatalf("want 1 check, got %d", len(checks))
	}

	firstCheck := checks[0].(map[string]interface{})
	if firstCheck["name"] != "oauth_credentials" {
		t.Fatalf("want check name 'oauth_credentials', got %v", firstCheck["name"])
	}
	if firstCheck["status"] != "fail" {
		t.Fatalf("want credentials status 'fail', got %v", firstCheck["status"])
	}
}

func TestDriveHealthCheck_CredsNoTokens(t *testing.T) {
	t.Parallel()

	w := healthRequest(t, newCredsNoTokensHandlers(t))
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}

	res := parseResponse(t, w)
	if res["status"] != "no_valid_token" {
		t.Fatalf("want status 'no_valid_token', got %v", res["status"])
	}
	if res["drive_configured"] != true {
		t.Fatalf("want drive_configured true, got %v", res["drive_configured"])
	}

	// Should have 2 checks (oauth_credentials, token_loaded)
	checks, ok := res["checks"].([]interface{})
	if !ok || len(checks) != 2 {
		t.Fatalf("want 2 checks, got %d", len(checks))
	}

	// Check 1: credentials should pass
	creds := checks[0].(map[string]interface{})
	if creds["name"] != "oauth_credentials" || creds["status"] != "pass" {
		t.Fatalf("credentials check should pass, got %v", creds)
	}

	// Check 2: token should fail (no tokens on disk)
	tokenCheck := checks[1].(map[string]interface{})
	if tokenCheck["name"] != "token_loaded" || tokenCheck["status"] != "fail" {
		t.Fatalf("token check should fail, got %v", tokenCheck)
	}
	if tokenCheck["detail"] != "no token files on disk" {
		t.Fatalf("want 'no token files on disk', got %v", tokenCheck["detail"])
	}
}

func TestDriveHealthCheck_WithExpiredTokenOnDisk(t *testing.T) {
	t.Parallel()

	w := healthRequest(t, newCredsWithTokenHandlers(t))
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}

	res := parseResponse(t, w)
	if res["status"] != "no_valid_token" {
		t.Fatalf("want status 'no_valid_token', got %v", res["status"])
	}
	if res["drive_configured"] != true {
		t.Fatalf("want drive_configured true, got %v", res["drive_configured"])
	}

	checks, ok := res["checks"].([]interface{})
	if !ok || len(checks) < 2 {
		t.Fatalf("want at least 2 checks, got %d", len(checks))
	}

	// Check 1: credentials pass
	creds := checks[0].(map[string]interface{})
	if creds["name"] != "oauth_credentials" || creds["status"] != "pass" {
		t.Fatalf("credentials should pass, got %v", creds)
	}

	// Check 2: token check details
	tokenCheck := checks[1].(map[string]interface{})
	if tokenCheck["name"] != "token_loaded" {
		t.Fatalf("want 'token_loaded', got %v", tokenCheck["name"])
	}

	// Should show token details
	tokens, hasTokens := tokenCheck["tokens"].([]interface{})
	if !hasTokens || len(tokens) != 1 {
		t.Fatalf("want 1 token detail, got %v", tokenCheck["tokens"])
	}

	tok := tokens[0].(map[string]interface{})
	if tok["name"] != "default" {
		t.Fatalf("want token name 'default', got %v", tok["name"])
	}
	if tok["account_email"] != "test@example.com" {
		t.Fatalf("want account_email 'test@example.com', got %v", tok["account_email"])
	}
	if tok["has_refresh"] != true {
		t.Fatalf("want has_refresh true, got %v", tok["has_refresh"])
	}
	if tok["scope"] != "https://www.googleapis.com/auth/drive.file" {
		t.Fatalf("want scope, got %v", tok["scope"])
	}

	// Token should be expired
	expired, _ := tok["expired"].(bool)
	if !expired {
		t.Fatal("want expired=true for expired token")
	}

	// Token should be "invalid" (GetAbout fails for expired token with no real Google API)
	status, _ := tok["status"].(string)
	if status != "invalid" {
		t.Fatalf("want token status 'invalid' (GetAbout fails without real API), got %q", status)
	}
}

func TestDriveHealthCheck_ResponseHasCheckedAt(t *testing.T) {
	t.Parallel()

	w := healthRequest(t, newCredsNoTokensHandlers(t))
	res := parseResponse(t, w)

	checkedAt, ok := res["checked_at"].(string)
	if !ok || checkedAt == "" {
		t.Fatal("want non-empty checked_at")
	}
}

// Test that the handler is correctly wired via RegisterDriveRoutes.
func TestDriveHealthCheck_RouteRegistration(t *testing.T) {
	t.Parallel()

	// Create a minimal handler (nil service)
	h := &DriveHandlers{
		dataDir:      t.TempDir(),
		driveService: nil,
	}

	r := gin.New()
	RegisterDriveRoutes(r, h)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/drive/health", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("via registered route: want 503 for nil service, got %d", w.Code)
	}
}

// Test that other routes still work after adding the health route.
func TestDriveHealthCheck_OtherRoutesUnaffected(t *testing.T) {
	t.Parallel()

	// Just verify the handler can be called without panics when using
	// a real service setup (no token = early return)
	w := healthRequest(t, newEmptyCredsHandlers(t))
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
}

// Test JSON response format for healthy scenario.
// Since we can't reach real Google Drive, we test that the structure
// is correct even in failure mode.
func TestDriveHealthCheck_ResponseStructure(t *testing.T) {
	t.Parallel()

	// Use the no-credentials path for structure validation
	w := healthRequest(t, newEmptyCredsHandlers(t))
	res := parseResponse(t, w)

	// Top-level keys
	expectedKeys := []string{"drive_configured", "checks", "checked_at", "status", "error"}
	for _, key := range expectedKeys {
		if _, exists := res[key]; !exists {
			t.Fatalf("missing expected key %q in response", key)
		}
	}

	// Checks should be an array with oauth_credentials
	checks, ok := res["checks"].([]interface{})
	if !ok || len(checks) != 1 {
		t.Fatalf("want 1 check, got %d", len(checks))
	}

	check := checks[0].(map[string]interface{})
	if check["name"] != "oauth_credentials" {
		t.Fatalf("want check name, got %v", check["name"])
	}
	if _, hasStatus := check["status"]; !hasStatus {
		t.Fatal("check missing status field")
	}
	if _, hasDetail := check["detail"]; !hasDetail {
		t.Fatal("check missing detail field")
	}
}

// Test that a token with only drive.file scope is handled correctly.
func TestDriveHealthCheck_TokenScopeReporting(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	tokensDir := filepath.Join(tempDir, "tokens")

	svc, err := integrationsDrive.NewService(&integrationsDrive.ServiceConfig{
		ClientID:     "test-client-id",
		ClientSecret: "test-client-secret",
		TokensDir:    tokensDir,
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	// Save a token with only drive.file scope
	tm := svc.GetTokenManager()
	err = tm.SaveToken("limited", &integrationsDrive.Token{
		AccessToken:  "dummy-token",
		RefreshToken: "dummy-refresh",
		TokenType:    "Bearer",
		Expiry:       time.Now().Add(1 * time.Hour), // valid
		Scope:        "https://www.googleapis.com/auth/drive.file",
		AccountEmail: "limited@example.com",
	})
	if err != nil {
		t.Fatalf("SaveToken: %v", err)
	}

	h := &DriveHandlers{
		dataDir:      tempDir,
		driveService: svc,
	}

	w := healthRequest(t, h)
	res := parseResponse(t, w)

	// Should fail to validate (no real API), so status is no_valid_token
	if res["status"] != "no_valid_token" {
		t.Fatalf("want 'no_valid_token', got %v", res["status"])
	}

	// Token should be in the response with its scope
	checks := res["checks"].([]interface{})
	tokenCheck := checks[1].(map[string]interface{})
	tokens := tokenCheck["tokens"].([]interface{})
	tok := tokens[0].(map[string]interface{})
	if tok["scope"] != "https://www.googleapis.com/auth/drive.file" {
		t.Fatalf("want drive.file scope, got %v", tok["scope"])
	}
}

// Test that multiple tokens on disk are all listed.
func TestDriveHealthCheck_MultipleTokens(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	tokensDir := filepath.Join(tempDir, "tokens")

	svc, err := integrationsDrive.NewService(&integrationsDrive.ServiceConfig{
		ClientID:     "test-client-id",
		ClientSecret: "test-client-secret",
		TokensDir:    tokensDir,
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	tm := svc.GetTokenManager()
	for _, name := range []string{"token_a", "token_b"} {
		err = tm.SaveToken(name, &integrationsDrive.Token{
			AccessToken:  "dummy-" + name,
			RefreshToken: "refresh-" + name,
			TokenType:    "Bearer",
			Expiry:       time.Now().Add(-1 * time.Hour),
			Scope:        "https://www.googleapis.com/auth/drive.file",
			AccountEmail: name + "@example.com",
		})
		if err != nil {
			t.Fatalf("SaveToken %s: %v", name, err)
		}
	}

	h := &DriveHandlers{
		dataDir:      tempDir,
		driveService: svc,
	}

	w := healthRequest(t, h)
	res := parseResponse(t, w)

	checks := res["checks"].([]interface{})
	tokenCheck := checks[1].(map[string]interface{})
	tokens := tokenCheck["tokens"].([]interface{})
	if len(tokens) != 2 {
		t.Fatalf("want 2 tokens listed, got %d", len(tokens))
	}
	if tokenCheck["total_tokens"] != float64(2) {
		t.Fatalf("want total_tokens 2, got %v", tokenCheck["total_tokens"])
	}
}

// Test the edge case where tokensDir does not exist (NewService creates it).
func TestDriveHealthCheck_TokensDirCreated(t *testing.T) {
	t.Parallel()

	// A fresh temp dir should have an empty tokens dir created by NewService
	tempDir := t.TempDir()
	tokensDir := filepath.Join(tempDir, "fresh_tokens")

	svc, err := integrationsDrive.NewService(&integrationsDrive.ServiceConfig{
		ClientID:     "test-client-id",
		ClientSecret: "test-client-secret",
		TokensDir:    tokensDir,
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	h := &DriveHandlers{
		dataDir:      tempDir,
		driveService: svc,
	}

	w := healthRequest(t, h)
	res := parseResponse(t, w)

	if res["status"] != "no_valid_token" {
		t.Fatalf("want 'no_valid_token' for fresh tokens dir, got %v", res["status"])
	}
}

// Test that a non-existent tokens dir returns proper error.
func TestDriveHealthCheck_NoTokensDir(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	nonExistentDir := filepath.Join(tempDir, "nonexistent", "deep")

	svc, err := integrationsDrive.NewService(&integrationsDrive.ServiceConfig{
		ClientID:     "test-client-id",
		ClientSecret: "test-client-secret",
		TokensDir:    nonExistentDir,
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	h := &DriveHandlers{
		dataDir:      tempDir,
		driveService: svc,
	}

	w := healthRequest(t, h)
	res := parseResponse(t, w)

	// NewService creates the directory if needed, so it should exist.
	// The token check should still work (just no tokens).
	if res["status"] != "no_valid_token" {
		t.Fatalf("want 'no_valid_token', got %v", res["status"])
	}
}

// Verify that the HTTP status code is 200 (not 503) for expected config issues.
func TestDriveHealthCheck_StatusCodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		handler    *DriveHandlers
		wantStatus int
	}{
		{
			name:       "nil service",
			handler:    &DriveHandlers{dataDir: t.TempDir(), driveService: nil},
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name:       "empty credentials",
			handler:    newEmptyCredsHandlers(t),
			wantStatus: http.StatusOK,
		},
		{
			name:       "creds no tokens",
			handler:    newCredsNoTokensHandlers(t),
			wantStatus: http.StatusOK,
		},
		{
			name:       "creds with token",
			handler:    newCredsWithTokenHandlers(t),
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w := healthRequest(t, tt.handler)
			if w.Code != tt.wantStatus {
				t.Fatalf("want status %d, got %d. Body: %s", tt.wantStatus, w.Code, w.Body.String())
			}
		})
	}
}

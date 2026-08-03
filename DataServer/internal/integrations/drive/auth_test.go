package drive

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestTokenUnmarshalAcceptsPipelineGenAccessToken(t *testing.T) {
	var token Token
	if err := token.UnmarshalJSON([]byte(`{"access_token":"present","refresh_token":"refresh","expiry":"2099-01-01T00:00:00Z"}`)); err != nil {
		t.Fatalf("unmarshal token: %v", err)
	}
	if token.AccessToken != "present" {
		t.Fatalf("access token = %q, want compatibility value", token.AccessToken)
	}
	if token.RefreshToken != "refresh" {
		t.Fatalf("refresh token was not preserved")
	}
}

func TestTokenManagerEncryptsAndReadsLegacyPlaintext(t *testing.T) {
	t.Setenv("VELOX_CREDENTIAL_KEY", "01234567890123456789012345678901")
	tm, err := NewTokenManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	token := &Token{AccessToken: "access-secret", RefreshToken: "refresh-secret", Expiry: time.Now().Add(time.Hour)}
	if err := tm.SaveToken("default", token); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(tm.tokensDir, "default.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "access-secret") || strings.Contains(string(raw), "refresh-secret") {
		t.Fatalf("token file contains plaintext secret: %s", raw)
	}
	loaded, err := tm.LoadToken("default")
	if err != nil || loaded.AccessToken != "access-secret" || loaded.RefreshToken != "refresh-secret" {
		t.Fatalf("loaded token = %#v, err=%v", loaded, err)
	}
	if err := os.WriteFile(path, []byte(`{"access_token":"plaintext"}`), 0600); err != nil {
		t.Fatal(err)
	}
	legacy, err := tm.LoadToken("default")
	if err != nil || legacy.AccessToken != "plaintext" {
		t.Fatalf("legacy plaintext token = %#v, err=%v", legacy, err)
	}
}

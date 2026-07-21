package instaeditauth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// testSecret is a 32-byte secret used across all verifier tests.
var testSecret = strings.Repeat("a", 32)

// mintToken builds a compact JWS (HS256) from the given claims using
// the provided secret. Used by every test to generate valid or
// intentionally-invalid tokens.
func mintToken(t *testing.T, secret string, claims Claims) string {
	t.Helper()
	header := map[string]string{"alg": "HS256", "typ": "JWT"}
	headerJSON, _ := json.Marshal(header)
	payloadJSON, _ := json.Marshal(claims)

	headerB64 := base64.RawURLEncoding.EncodeToString(headerJSON)
	payloadB64 := base64.RawURLEncoding.EncodeToString(payloadJSON)

	signingInput := headerB64 + "." + payloadB64
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(signingInput))
	sigB64 := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	return signingInput + "." + sigB64
}

// validClaims returns a Claims struct with all required fields set to
// valid values. Tests clone this and mutate one field to exercise a
// specific failure mode.
func validClaims() Claims {
	return Claims{
		Issuer:      ExpectedIssuer,
		Audience:    ExpectedAudience,
		Subject:     "123",
		WorkspaceID: 45,
		Scopes:      []string{"velox:jobs:read", "velox:jobs:write", "velox:workers:read"},
		ExpiresAt:   time.Now().Add(5 * time.Minute).Unix(),
		JTI:         "test-jti",
	}
}

func TestNew_RejectsShortSecret(t *testing.T) {
	_, err := New("short")
	if err == nil {
		t.Fatal("expected error for short secret")
	}
}

func TestNew_AcceptsMinSecret(t *testing.T) {
	v, err := New(strings.Repeat("x", MinimumSecretBytes))
	if err != nil {
		t.Fatalf("expected no error for %d-byte secret, got %v", MinimumSecretBytes, err)
	}
	if v == nil {
		t.Fatal("expected non-nil verifier")
	}
}

func TestVerify_ValidToken(t *testing.T) {
	v, _ := New(testSecret)
	token := mintToken(t, testSecret, validClaims())
	claims, err := v.Verify(token)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if claims.WorkspaceID != 45 {
		t.Fatalf("workspace_id mismatch: got %d", claims.WorkspaceID)
	}
	if claims.Subject != "123" {
		t.Fatalf("sub mismatch: got %s", claims.Subject)
	}
}

func TestVerify_BadSignature(t *testing.T) {
	v, _ := New(testSecret)
	token := mintToken(t, "wrong-secret-aaaaaaaaaaaaaaaaaaaaaaaaa", validClaims())
	_, err := v.Verify(token)
	if !isErr(err, ErrSignatureMismatch) {
		t.Fatalf("expected ErrSignatureMismatch, got %v", err)
	}
}

func TestVerify_WrongIssuer(t *testing.T) {
	v, _ := New(testSecret)
	c := validClaims()
	c.Issuer = "evil"
	_, err := v.Verify(mintToken(t, testSecret, c))
	if !isErr(err, ErrWrongIssuer) {
		t.Fatalf("expected ErrWrongIssuer, got %v", err)
	}
}

func TestVerify_WrongAudience(t *testing.T) {
	v, _ := New(testSecret)
	c := validClaims()
	c.Audience = "evil"
	_, err := v.Verify(mintToken(t, testSecret, c))
	if !isErr(err, ErrWrongAudience) {
		t.Fatalf("expected ErrWrongAudience, got %v", err)
	}
}

func TestVerify_Expired(t *testing.T) {
	v, _ := New(testSecret)
	c := validClaims()
	c.ExpiresAt = time.Now().Add(-1 * time.Minute).Unix()
	_, err := v.Verify(mintToken(t, testSecret, c))
	if !isErr(err, ErrTokenExpired) {
		t.Fatalf("expected ErrTokenExpired, got %v", err)
	}
}

func TestVerify_MissingExp(t *testing.T) {
	v, _ := New(testSecret)
	c := validClaims()
	c.ExpiresAt = 0
	_, err := v.Verify(mintToken(t, testSecret, c))
	if !isErr(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken for missing exp, got %v", err)
	}
}

func TestVerify_MalformedToken(t *testing.T) {
	v, _ := New(testSecret)
	_, err := v.Verify("not.a.valid.jwt.extra")
	if !isErr(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken, got %v", err)
	}
}

func TestVerify_TwoSegments(t *testing.T) {
	v, _ := New(testSecret)
	_, err := v.Verify("only.two")
	if !isErr(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken for 2 segments, got %v", err)
	}
}

func TestVerify_EmptyToken(t *testing.T) {
	v, _ := New(testSecret)
	_, err := v.Verify("")
	if !isErr(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken for empty token, got %v", err)
	}
}

func TestVerify_NilVerifier(t *testing.T) {
	var v *Verifier
	_, err := v.Verify("any.token.here")
	if !isErr(err, ErrSecretNotConfigured) {
		t.Fatalf("expected ErrSecretNotConfigured for nil verifier, got %v", err)
	}
}

func TestVerify_WithClock_Expired(t *testing.T) {
	fixedNow := time.Unix(2000000000, 0) // far future
	v, _ := New(testSecret, WithClock(func() time.Time { return fixedNow }))
	c := validClaims()
	c.ExpiresAt = 1999999999 // 1 second before fixedNow
	_, err := v.Verify(mintToken(t, testSecret, c))
	if !isErr(err, ErrTokenExpired) {
		t.Fatalf("expected ErrTokenExpired with fixed clock, got %v", err)
	}
}

func TestHasScope(t *testing.T) {
	c := &Claims{Scopes: []string{"velox:jobs:read", "velox:workers:read"}}
	if !c.HasScope("velox:jobs:read") {
		t.Fatal("expected HasScope to find velox:jobs:read")
	}
	if c.HasScope("velox:assets:read") {
		t.Fatal("expected HasScope to NOT find velox:assets:read")
	}
}

func TestHasAllScopes(t *testing.T) {
	c := &Claims{Scopes: []string{"a", "b", "c"}}
	if !c.HasAllScopes("a", "b") {
		t.Fatal("expected HasAllScopes a,b to pass")
	}
	if c.HasAllScopes("a", "z") {
		t.Fatal("expected HasAllScopes a,z to fail")
	}
}

func TestHasScope_NilClaims(t *testing.T) {
	var c *Claims
	if c.HasScope("anything") {
		t.Fatal("expected HasScope on nil claims to return false")
	}
}

func TestVerifyAlgNoneAttack(t *testing.T) {
	v, _ := New(testSecret)
	// A token with alg=none and no signature — classic JWT attack.
	// The verifier checks the HMAC regardless of the header's alg field.
	headerJSON, _ := json.Marshal(map[string]string{"alg": "none", "typ": "JWT"})
	payloadJSON, _ := json.Marshal(validClaims())
	headerB64 := base64.RawURLEncoding.EncodeToString(headerJSON)
	payloadB64 := base64.RawURLEncoding.EncodeToString(payloadJSON)
	// alg=none tokens use an empty signature segment.
	token := headerB64 + "." + payloadB64 + "."
	_, err := v.Verify(token)
	if err == nil {
		t.Fatal("alg=none token must NOT be accepted")
	}
}

func TestVerify_DifferentScopesRoundTrip(t *testing.T) {
	v, _ := New(testSecret)
	c := validClaims()
	c.Scopes = []string{"velox:assets:read", "velox:assets:write"}
	token := mintToken(t, testSecret, c)
	claims, err := v.Verify(token)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(claims.Scopes) != 2 {
		t.Fatalf("expected 2 scopes, got %d", len(claims.Scopes))
	}
	if !claims.HasScope("velox:assets:write") {
		t.Fatal("expected velox:assets:write scope")
	}
}

func TestVerify_FutureExp(t *testing.T) {
	v, _ := New(testSecret)
	c := validClaims()
	c.ExpiresAt = time.Now().Add(2 * time.Minute).Unix()
	_, err := v.Verify(mintToken(t, testSecret, c))
	if err != nil {
		t.Fatalf("expected no error for future exp, got %v", err)
	}
}

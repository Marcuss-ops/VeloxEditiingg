// Package instaeditauth verifies the short-lived JWT issued by the
// InstaEdit BFF when proxying user-facing requests to the Velox master.
//
// The JWT is signed with HS256 using a shared secret
// (INSTAEDIT_CONTROL_JWT_SECRET) that is distinct from the service-to-
// service Bearer token (SOCIAL_API_TOKEN) used for the reverse
// direction. The two secrets MUST NOT be reused across directions.
//
// DESIGN:
//   - Zero new dependencies: HS256 is implemented with crypto/hmac +
//     crypto/sha256 + encoding/base64, matching the project's
//     minimal-dependency style (the existing socialclient also uses
//     only stdlib for its HTTP layer).
//   - The verifier checks: signature, issuer == "instaedit",
//     audience == "velox", exp not expired, and that the token's
//     scopes include all required scopes for the operation.
//   - Free headers X-User-ID / X-Workspace-ID are NEVER trusted
//     without a valid JWT signature. The identity (user_id,
//     workspace_id) is extracted ONLY from the verified JWT claims.
//
// The JWT shape (from the architectural spec):
//
//	{
//	  "iss": "instaedit",
//	  "aud": "velox",
//	  "sub": "123",
//	  "workspace_id": 45,
//	  "scopes": ["velox:jobs:read", "velox:jobs:write", "velox:workers:read"],
//	  "exp": 1780000000,
//	  "jti": "random-id"
//	}
//
// Durata consigliata: 2-5 minuti. The verifier does NOT enforce a
// maximum lifetime — the issuer is trusted to keep exp short. A
// separate replay-protection (jti blacklist) is out of scope for this
// package; callers that need replay protection should layer it on top.
package instaeditauth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ExpectedIssuer is the only accepted value for the iss claim.
const ExpectedIssuer = "instaedit"

// ExpectedAudience is the only accepted value for the aud claim.
const ExpectedAudience = "velox"

// MinimumSecretBytes is the minimum secret length enforced at
// construction time. 32 bytes (256 bits) matches the HS256 nominal
// entropy and prevents operators from accidentally shipping a
// weak shared secret.
const MinimumSecretBytes = 32

// Sentinel errors. Callers can use errors.Is to map to HTTP status
// codes. All verification failures return one of these (wrapped with
// detail via %w) so the middleware can distinguish 401 (bad token)
// from 503 (config error). Scope enforcement is NOT handled by Verify
// — the middleware checks claims.HasAllScopes AFTER a successful
// Verify and aborts with 403 directly.
var (
	ErrInvalidToken        = errors.New("instaeditauth: invalid token")
	ErrSignatureMismatch   = errors.New("instaeditauth: signature mismatch")
	ErrTokenExpired        = errors.New("instaeditauth: token expired")
	ErrWrongIssuer         = errors.New("instaeditauth: wrong issuer")
	ErrWrongAudience       = errors.New("instaeditauth: wrong audience")
	ErrSecretNotConfigured = errors.New("instaeditauth: secret not configured")
)

// Claims is the set of JWT claims the verifier extracts. Only the
// fields used for authz are parsed; unrecognized claims are ignored.
//
// WorkspaceID and Subject (sub) are the identity fields forwarded to
// Velox handlers. Scopes is the authorization grant list. JTI is the
// unique token id (available for future replay-protection layers).
type Claims struct {
	Issuer      string   `json:"iss"`
	Audience    string   `json:"aud"`
	Subject     string   `json:"sub"`
	WorkspaceID int64    `json:"workspace_id"`
	Scopes      []string `json:"scopes"`
	ExpiresAt   int64    `json:"exp"`
	JTI         string   `json:"jti"`
}

// Verifier validates InstaEdit-issued JWTs against a shared secret.
// Construct once at bootstrap; safe for concurrent use (no mutable
// state). A nil or empty secret makes Verify return
// ErrSecretNotConfigured so the middleware can map it to 503.
type Verifier struct {
	secret []byte
	now    func() time.Time
}

// Option configures a Verifier at construction time.
type Option func(*Verifier)

// WithClock overrides the time source (testing only).
func WithClock(now func() time.Time) Option {
	return func(v *Verifier) { v.now = now }
}

// New builds a Verifier from the shared secret. Returns an error when
// the secret is shorter than MinimumSecretBytes so a misconfigured
// deployment fails at boot rather than accepting forged tokens.
func New(secret string, opts ...Option) (*Verifier, error) {
	s := []byte(secret)
	if len(s) < MinimumSecretBytes {
		return nil, fmt.Errorf(
			"instaeditauth: secret must be at least %d bytes, got %d",
			MinimumSecretBytes, len(s))
	}
	v := &Verifier{secret: s, now: time.Now}
	for _, opt := range opts {
		opt(v)
	}
	return v, nil
}

// Verify parses and validates the token. Returns the Claims on
// success, or one of the sentinel errors (wrapped) on failure.
//
// The token must be a compact JWS (three base64url segments separated
// by '.'). The header is NOT inspected for the alg field — HS256 is
// the only algorithm this verifier accepts, so a token claiming a
// different alg simply fails the signature check (defensive: prevents
// the "alg=none" attack vector).
func (v *Verifier) Verify(token string) (*Claims, error) {
	if v == nil || len(v.secret) == 0 {
		return nil, ErrSecretNotConfigured
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("%w: expected 3 segments, got %d", ErrInvalidToken, len(parts))
	}

	// Verify the signature BEFORE parsing the payload. This prevents
	// a malformed-payload error from masking a signature failure
	// (callers must always see ErrSignatureMismatch when the token is
	// forged, regardless of payload validity).
	signingInput := parts[0] + "." + parts[1]
	expectedSig := v.computeSignature(signingInput)
	actualSig, err := decodeBase64URL(parts[2])
	if err != nil {
		return nil, fmt.Errorf("%w: signature decode: %v", ErrInvalidToken, err)
	}
	if !hmac.Equal(expectedSig, actualSig) {
		return nil, ErrSignatureMismatch
	}

	// Parse the payload (claims). The signature is already verified,
	// so a parse error here means the issuer sent a structurally
	// invalid token (not a forgery).
	payloadBytes, err := decodeBase64URL(parts[1])
	if err != nil {
		return nil, fmt.Errorf("%w: payload decode: %v", ErrInvalidToken, err)
	}
	var claims Claims
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return nil, fmt.Errorf("%w: payload json: %v", ErrInvalidToken, err)
	}

	// Issuer check.
	if claims.Issuer != ExpectedIssuer {
		return nil, fmt.Errorf("%w: got %q, want %q", ErrWrongIssuer, claims.Issuer, ExpectedIssuer)
	}
	// Audience check.
	if claims.Audience != ExpectedAudience {
		return nil, fmt.Errorf("%w: got %q, want %q", ErrWrongAudience, claims.Audience, ExpectedAudience)
	}
	// Expiry check. A zero exp is rejected (the issuer MUST set exp).
	if claims.ExpiresAt <= 0 {
		return nil, fmt.Errorf("%w: exp missing", ErrInvalidToken)
	}
	expTime := time.Unix(claims.ExpiresAt, 0)
	if v.now().After(expTime) {
		return nil, fmt.Errorf("%w: exp %s", ErrTokenExpired, expTime.Format(time.RFC3339))
	}

	return &claims, nil
}

// HasScope reports whether the claims grant the required scope.
// Returns false when claims is nil (defense-in-depth for callers that
// forget to check the Verify error).
func (c *Claims) HasScope(required string) bool {
	if c == nil {
		return false
	}
	for _, s := range c.Scopes {
		if s == required {
			return true
		}
	}
	return false
}

// HasAllScopes reports whether the claims include every required scope.
func (c *Claims) HasAllScopes(required ...string) bool {
	for _, r := range required {
		if !c.HasScope(r) {
			return false
		}
	}
	return true
}

// computeSignature returns the HS256 MAC of the signing input.
func (v *Verifier) computeSignature(signingInput string) []byte {
	mac := hmac.New(sha256.New, v.secret)
	mac.Write([]byte(signingInput))
	return mac.Sum(nil)
}

// decodeBase64URL decodes a base64url string without padding (the JWS
// compact serialization form). Returns an error on malformed input.
func decodeBase64URL(s string) ([]byte, error) {
	// JWS uses base64url WITHOUT padding. encoding/base64 requires
	// padded input for StdEncoding, so we add the padding back.
	padded := s
	if rem := len(s) % 4; rem != 0 {
		padded += strings.Repeat("=", 4-rem)
	}
	return base64.URLEncoding.DecodeString(padded)
}

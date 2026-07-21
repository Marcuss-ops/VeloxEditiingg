package instaeditauth

import (
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// Context keys for the verified identity stamped into the Gin context.
// Downstream handlers retrieve the Claims via FromContext(ctx) rather
// than re-parsing the JWT, so the middleware is the single verification
// gate.
const (
	ctxKeyClaims = "instaeditauth_claims"
)

// FromContext extracts the verified Claims from a Gin context. Returns
// nil when the middleware did not run or the token was rejected (in
// which case the middleware already aborted the request).
func FromContext(c *gin.Context) *Claims {
	if c == nil {
		return nil
	}
	v, ok := c.Get(ctxKeyClaims)
	if !ok {
		return nil
	}
	claims, _ := v.(*Claims)
	return claims
}

// Middleware returns a Gin middleware that verifies the InstaEdit JWT
// from the Authorization: Bearer header. On success it stamps the
// Claims into the context and calls c.Next(). On failure it aborts
// with 401/403/503.
//
// CRITICAL: free headers X-User-ID and X-Workspace-ID are NEVER
// trusted. The middleware actively REJECTS requests that carry these
// headers WITHOUT a valid signed JWT, so a caller cannot bypass the
// identity layer by injecting raw headers. The identity (user_id via
// Subject, workspace_id via Claims.WorkspaceID) is extracted ONLY from
// the verified JWT claims.
//
// requiredScopes is the scope list the endpoint demands. When the JWT
// does not include all of them, the middleware aborts with 403. Pass
// nil to skip scope enforcement (identity-only verification).
func Middleware(verifier *Verifier, requiredScopes []string) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Defense-in-depth: reject free identity headers up front so
		// a caller cannot smuggle user_id / workspace_id without a
		// signed JWT. The headers are only meaningful AFTER a verified
		// JWT establishes the caller's identity — and even then, the
		// values come from the JWT claims, not from the headers.
		if hasFreeIdentityHeaders(c) {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "free X-User-ID / X-Workspace-ID headers are not accepted; use a signed JWT",
			})
			return
		}

		if verifier == nil {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{
				"error": "instaedit control JWT verifier not configured",
			})
			return
		}

		token := extractBearerToken(c.GetHeader("Authorization"))
		if token == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "missing bearer token",
			})
			return
		}

		claims, err := verifier.Verify(token)
		if err != nil {
			// Map sentinel errors to HTTP status codes. ErrSecretNotConfigured
			// is 503 (server misconfiguration); all other verification
			// failures are 401 (bad token). Scope enforcement happens AFTER
			// a successful Verify (see the HasAllScopes check below) and
			// aborts with 403.
			switch {
			case isErr(err, ErrSecretNotConfigured):
				c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{
					"error": "instaedit control JWT verifier not configured",
				})
			default:
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
					"error": "invalid token",
				})
			}
			return
		}

		// Scope enforcement (when requiredScopes is non-empty).
		if len(requiredScopes) > 0 && !claims.HasAllScopes(requiredScopes...) {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"error":            "insufficient scope",
				"required_scopes":  requiredScopes,
				"presented_scopes": claims.Scopes,
			})
			return
		}

		// Stamp the verified claims into the context for downstream
		// handlers. The claims carry the authoritative user_id (via
		// Subject) and workspace_id — NOT the rejected free headers.
		c.Set(ctxKeyClaims, claims)
		c.Next()
	}
}

// hasFreeIdentityHeaders reports whether the request carries
// X-User-ID or X-Workspace-ID. These headers are rejected unconditionally
// because the identity MUST come from the signed JWT claims, not from
// caller-supplied headers.
func hasFreeIdentityHeaders(c *gin.Context) bool {
	return strings.TrimSpace(c.GetHeader("X-User-ID")) != "" ||
		strings.TrimSpace(c.GetHeader("X-Workspace-ID")) != ""
}

// extractBearerToken pulls the raw token from an Authorization header
// of the form "Bearer <token>". Returns "" when the header is absent
// or malformed. Case-insensitive on the "Bearer" prefix per RFC 6750.
func extractBearerToken(authHeader string) string {
	authHeader = strings.TrimSpace(authHeader)
	if authHeader == "" {
		return ""
	}
	if !strings.HasPrefix(strings.ToLower(authHeader), "bearer ") {
		return ""
	}
	return strings.TrimSpace(authHeader[7:])
}

// isErr reports whether err wraps target via errors.Is.
func isErr(err, target error) bool {
	return errors.Is(err, target)
}

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

// 403 body shape — produced by Middleware / MiddlewareWithOperation
// on insufficient scope. Designed to satisfy the spec's
// "messaggio chiaro" requirement: an operator who sees this body can
// immediately tell:
//
//   - what the JWT lacked (required_scopes);
//   - what the JWT actually carried (presented_scopes, which may be
//     nil when claims.Scopes is unset — e.g. on tokens issued by an
//     older InstaEdit build);
//   - which Velox route rejected them (route, exact URL path);
//   - which logical operation they attempted (operation, set by
//     MiddlewareWithOperation callers — falls back to "-" when the
//     generic Middleware is used);
//   - how to remediate (hint, a fixed instruction string).
//
// All 5 fields are unconditionally present on a 403 from this
// package so downstream consumers can rely on the schema being
// stable across handlers.
type scopeDenialBody struct {
	Error           string   `json:"error"`
	RequiredScopes  []string `json:"required_scopes"`
	PresentedScopes []string `json:"presented_scopes"`
	Operation       string   `json:"operation"`
	Route           string   `json:"route"`
	Hint            string   `json:"hint"`
}

// scopedHint is the operator-facing remediation string baked into the
// 403 body. Keep it stable: the BFF may surface it verbatim to the
// dark-editor SPA's "why was this rejected?" UI.
const scopedHint = "the InstaEdit BFF must re-mint the control JWT with at least the required_scopes; this is a server-side misconfiguration (or a Velox route demanding a higher grant than the BFF is currently issuing for this operation)."

// Middleware returns a Gin middleware that verifies the InstaEdit JWT
// from the Authorization: Bearer header. On success it stamps the
// Claims into the context and calls c.Next(). On failure it aborts
// with 401 (bad/expired token or forged header), 403 (insufficient
// scope), or 503 (server misconfiguration).
//
// CRITICAL: free headers X-User-ID and X-Workspace-ID are NEVER
// trusted. The middleware actively REJECTS requests that carry these
// headers WITHOUT a valid signed JWT, so a caller cannot bypass the
// identity layer by injecting raw headers. The identity (user_id via
// Subject, workspace_id via Claims.WorkspaceID) is extracted ONLY from
// the verified JWT claims.
//
// requiredScopes is the scope list the endpoint demands. When the JWT
// does not include all of them, the middleware aborts with the
// enriched 403 body documented on scopeDenialBody (route extracted
// from c.Request.URL.Path, operation = "-", hint = scopedHint).
//
// Use MiddlewareWithOperation when a per-handler operation label
// (e.g. "publish_thumbnail_session") improves the operator's
// diagnosis.
func Middleware(verifier *Verifier, requiredScopes []string) gin.HandlerFunc {
	return MiddlewareWithOperation(verifier, requiredScopes, "-")
}

// MiddlewareWithOperation is the operation-tagged variant of
// Middleware. The operation argument is a short snake_case label that
// the 403 body exposes under the "operation" field. Operators reading
// the response can grep the InstaEdit BFF source / Velox route table
// for the label without having to cross-reference path wildcards.
//
// "publish_thumbnail_session", "read_editor_project", "upload_editor_asset",
// "list_editor_workers" are example operation labels.
//
// The label MUST be a stable identifier — the architecture roadmap
// tracks it as a contract the dark-editor UI's "why rejected" panel
// may display verbatim.
func MiddlewareWithOperation(verifier *Verifier, requiredScopes []string, operation string) gin.HandlerFunc {
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
			abortInsufficientScope(c, requiredScopes, claims.Scopes, operation)
			return
		}

		// Stamp the verified claims into the context for downstream
		// handlers. The claims carry the authoritative user_id (via
		// Subject) and workspace_id — NOT the rejected free headers.
		c.Set(ctxKeyClaims, claims)
		c.Next()
	}
}

// abortInsufficientScope writes the enriched 403 body. Centralized
// here so the shape is identical across Middleware and
// MiddlewareWithOperation call sites — adding a new field to the
// 403 body means one edit, not 4+ duplicated literals.
func abortInsufficientScope(c *gin.Context, required, presented []string, operation string) {
	if operation == "" {
		operation = "-"
	}
	c.AbortWithStatusJSON(http.StatusForbidden, scopeDenialBody{
		Error:           "insufficient scope",
		RequiredScopes:  required,
		PresentedScopes: presented,
		Operation:       operation,
		Route:           c.Request.URL.Path,
		Hint:            scopedHint,
	})
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

// Package pipeline: m2m_auth.go is the M2M (machine-to-machine)
// auth middleware for POST /api/v1/jobs. It REPLACES the legacy
// adminAuth on that endpoint. Operators manage credentials via the
// admin CRUD endpoints under /api/v1/admin/m2m (see
// handlers/server/api/admin_m2m_keys.go).
//
// Middleware chain semantics, IN ORDER:
//
//  1. Parse Bearer token (workersreg.ExtractBearerToken for parity
//     with adminAuth's extractor).
//
//  2. SHA-256 hash the secret, lookup the active key row. Constant-
//     time on length-prefixed (the actual compare is in
//     store.M2MSecretMatches via store.SecretHash=
//     sha256(plaintext)). The constant-time compare protects
//     against attacker time-side-channel on storedHash length.
//
//  3. Scope check: requires `jobs.submit` in the key's scope list.
//     Always required, no overrides — different scopes belong to
//     different future endpoints.
//
//  4. Rate limit: in-memory token bucket keyed by client_id. Bucket
//     capacity = EffectiveBurst; refill = EffectiveRateLimitRPS.
//     On exhaustion, return 429 immediately and audit the reject.
//
//  5. Set client_id / request-scene-count expectations in
//     gin.Context so the handler can stamp external_client_id on
//     creator_forwardings. The actual body validation (incl. SSRF
//     URL policy + per-request quota enforcement) runs INSIDE the
//     handler because body content determines scene count and
//     total duration.
//
//  6. After the handler returns, write one row to m2m_audit_log
//     with the recorded status_code. Best-effort: audit failures
//     log a warning but do NOT alter the response (audit trail is
//     observability, not gate-keeping).
//
// File split by responsibility:
//   - m2m_auth.go        → context keys + NewM2MJwAuthMiddleware
//   - m2m_rate_limiter.go → token-bucket limiter + sqlNullString
//   - m2m_audit.go       → audit writer, audit rows, quota, helpers
package pipeline

import (
	"context"
	"crypto/subtle"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	workersreg "velox-server/internal/workers"

	"velox-server/internal/config"
	"velox-server/internal/creatorflow"
	"velox-server/internal/m2mkeys"
	"velox-server/internal/store"
)

// NewM2MOrAdminAuthMiddleware accepts either a valid operator admin token or
// a valid M2M token. It is used for the pipeline-run read/action routes so
// M2M callers receive the same ownership checks as /api/v1/jobs while the
// existing admin surface remains backward-compatible.
//
// Token selection is fail-closed: only an exact match with the configured
// admin token enters adminAuth; every other bearer token must pass the M2M
// middleware. The admin middleware remains responsible for its own origin and
// loopback policy.
func NewM2MOrAdminAuthMiddleware(cfg *config.Config, st *store.SQLiteStore, limiter *m2mRateLimiter, adminToken string, adminAuth gin.HandlerFunc) gin.HandlerFunc {
	m2mAuth := NewM2MJwAuthMiddleware(cfg, st, limiter)
	return func(c *gin.Context) {
		token := workersreg.ExtractBearerToken(c.GetHeader("Authorization"), "", "")
		if strings.TrimSpace(adminToken) != "" && subtle.ConstantTimeCompare([]byte(token), []byte(strings.TrimSpace(adminToken))) == 1 {
			if adminAuth == nil {
				c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "admin auth not configured"})
				return
			}
			adminAuth(c)
			return
		}
		m2mAuth(c)
	}
}

// NewServiceOrM2MOrAdminAuthMiddleware protects internal artifact reads for
// the reverse Velox→InstaEdit service contract. The service uses the shared
// SOCIAL_API_TOKEN for both delivery requests and artifact downloads; M2M and
// operator-admin tokens remain valid for tooling and backwards compatibility.
// Browser-origin requests are rejected even when the service token matches.
func NewServiceOrM2MOrAdminAuthMiddleware(cfg *config.Config, st *store.SQLiteStore, limiter *m2mRateLimiter, adminToken string, adminAuth gin.HandlerFunc) gin.HandlerFunc {
	m2mOrAdmin := NewM2MOrAdminAuthMiddleware(cfg, st, limiter, adminToken, adminAuth)
	serviceToken := ""
	if cfg != nil {
		serviceToken = strings.TrimSpace(cfg.Runtime.Social.APIKey)
	}
	return func(c *gin.Context) {
		if c.GetHeader("Origin") != "" {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "direct browser access forbidden"})
			return
		}
		token := workersreg.ExtractBearerToken(c.GetHeader("Authorization"), "", "")
		if serviceToken != "" && subtle.ConstantTimeCompare([]byte(token), []byte(serviceToken)) == 1 {
			c.Next()
			return
		}
		m2mOrAdmin(c)
	}
}

// =====================================================================
// gin.Context keys
// =====================================================================

const (
	m2mCtxKeyClientID     = "m2m_client_id"
	m2mCtxKeyM2MKey       = "m2m_key_row"
	m2mCtxKeyStartedAt    = "m2m_started_at"
	m2mCtxKeyIntakeSource = "m2m_intake_source"
)

// SetIntakeSource stamps the intake source for the current request. It is
// used by the batch endpoint to distinguish its items from the plain
// single-job surface; the single-job handler reads it via
// IntakeSourceFromContext and stamps it on the CanonicalJobSubmission.
func SetIntakeSource(c *gin.Context, source string) {
	if c == nil {
		return
	}
	c.Set(m2mCtxKeyIntakeSource, source)
}

// IntakeSourceFromContext returns the stamped intake source, or the
// canonical default when none was set (the plain POST /api/v1/jobs path).
func IntakeSourceFromContext(c *gin.Context) string {
	if c == nil {
		return creatorflow.IntakeSourceCanonical
	}
	if v, ok := c.Get(m2mCtxKeyIntakeSource); ok {
		if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
			return s
		}
	}
	return creatorflow.IntakeSourceCanonical
}

// ClientIDFromContext extracts the resolved client_id from a context
// populated by the M2M middleware. Returns the empty string when the
// middleware did not run (e.g., a test mount without M2M auth).
func ClientIDFromContext(c *gin.Context) string {
	if c == nil {
		return ""
	}
	if v, ok := c.Get(m2mCtxKeyClientID); ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// KeyFromContext returns the typed M2MAPIKey the middleware
// resolved against, or nil if M2M auth did not run.
func KeyFromContext(c *gin.Context) *m2mkeys.M2MAPIKey {
	if c == nil {
		return nil
	}
	if v, ok := c.Get(m2mCtxKeyM2MKey); ok {
		if k, ok := v.(*m2mkeys.M2MAPIKey); ok {
			return k
		}
	}
	return nil
}

// =====================================================================
// Middleware factory
// =====================================================================

// NewM2MJwAuthMiddleware returns the dedicated M2M middleware for
// POST /api/v1/jobs. Operator-grade wiring:
//
//   - cfg supplies defaults (rate limit, burst) for clients without
//     per-key overrides.
//   - st is the SQLite store where m2m_api_keys + m2m_audit_log live.
//   - limiter is the in-memory rate limit ledger. May be shared
//     across multiple m2m middleware instances on different routes
//     (a client hitting both gets their bucket drained twice as
//     fast — the typical "jobs.submit + future.read" combination
//     needs the operator to allocate separate keys).
func NewM2MJwAuthMiddleware(cfg *config.Config, st *store.SQLiteStore, limiter *m2mRateLimiter) gin.HandlerFunc {
	if cfg == nil {
		cfg = &config.Config{}
	}
	if limiter == nil {
		limiter = newM2MRateLimiter()
	}
	requiredScope := "jobs.submit"

	return func(c *gin.Context) {
		// Dev-mode loopback bypass: SSH forwards / curl from the
		// host hit loopback and historically presented
		// VELOX_ADMIN_TOKEN. M2M auth is a different surface
		// (per-client credentials), so loopback bypass is OFF by
		// default. There's no AllowLoopback flag for M2M: tests
		// that want loopback use the in-package `m2mJobsAuthFake`
		// fixture instead, so a misconfigured production
		// deployment doesn't accidentally leak.
		token := workersreg.ExtractBearerToken(c.GetHeader("Authorization"), "", "")
		if token == "" {
			auditM2MReject(c, st, "", "missing_token")
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"ok":      false,
				"error":   "m2m_token_required",
				"message": "Authorization: Bearer <m2m-secret> required for /api/v1/jobs",
			})
			return
		}

		hashed := m2mkeys.HashM2MSecret(token)
		ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		key, err := m2mkeys.GetActiveM2MAPIKeyBySecretHash(ctx, st.DB(), hashed)
		cancel()
		if err != nil {
			auditM2MReject(c, st, "", "db_error")
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				"ok":      false,
				"error":   "m2m_auth_failure",
				"message": "M2M key lookup failed",
			})
			return
		}
		if key == nil {
			auditM2MReject(c, st, "", "unknown_or_revoked_token")
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"ok":      false,
				"error":   "m2m_token_rejected",
				"message": "Bearer token not recognised (revoked, disabled, or never issued)",
			})
			return
		}

		// Scope check (single-scope v1).
		if !key.HasScope(requiredScope) {
			auditM2MReject(c, st, key.ClientID, "scope_missing:"+requiredScope)
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"ok":      false,
				"error":   "m2m_scope_rejected",
				"message": "client_id=" + key.ClientID + " lacks required scope " + requiredScope,
			})
			return
		}

		// Rate limit (in-memory token bucket per client_id).
		burst := float64(key.EffectiveBurst(cfg.M2M.DefaultBurst))
		rps := float64(key.EffectiveRateLimitRPS(cfg.M2M.DefaultRPS))
		if !limiter.take(key.ClientID, burst, rps) {
			auditM2MReject(c, st, key.ClientID, "rate_limited")
			c.Header("Retry-After", "1")
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"ok":      false,
				"error":   "m2m_rate_limited",
				"message": "client_id=" + key.ClientID + " exceeded rate limit (" + strconv.Itoa(int(rps)) + " req/sec, burst " + strconv.Itoa(int(burst)) + ")",
			})
			return
		}

		// Stash for the handler.
		c.Set(m2mCtxKeyClientID, key.ClientID)
		c.Set(m2mCtxKeyM2MKey, key)
		c.Set(m2mCtxKeyStartedAt, time.Now())

		// Wrap the writer to capture the post-handler status_code for
		// the audit row. This is the canonical Gin pattern for
		// observability middleware that needs what-the-client-saw
		// (NOT what-the-handler-decided).
		originalWriter := c.Writer
		c.Writer = &m2mAuditWriter{
			ResponseWriter: originalWriter,
			statusCode:     http.StatusOK,
		}

		// c.Next — runs the handler.
		c.Next()

		// Read the captured status BEFORE we restore c.Writer. Once we
		// reassign c.Writer, the audit wrapper is no longer reachable
		// via a type assertion on c.Writer (it's an interface-typed
		// value) — recording -1 in m2m_audit_log.status_code would
		// silently break every audit query (security forensics,
		// traffic dashboards). Capture the code from
		// *m2mAuditWriter.statusCode BEFORE the restore, then read
		// the result via the direct field.
		statusCode := -1
		if aw, ok := c.Writer.(*m2mAuditWriter); ok {
			statusCode = aw.statusCode
		}

		// Restore for any post-handler instrumentation (rarely used
		// here; defensive).
		c.Writer = originalWriter
		// Compute the audit row from the request: scene count +
		// total duration (if the handler accepted the body).
		auditM2MSuccess(c, st, key.ClientID, statusCode)
	}
}

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
package pipeline

import (
	"context"
	"database/sql"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	workersreg "velox-server/internal/workers"

	"velox-server/internal/config"
	"velox-server/internal/store"
)

// sqlNullString is a tiny helper used by the audit helpers below
// to lift a string into sql.NullString without repeating the
// Valid=len>0 ceremony at every call site.
func sqlNullString(s string) sql.NullString {
	s = strings.TrimSpace(s)
	return sql.NullString{String: s, Valid: s != ""}
}

// =====================================================================
// Per-client rate limiter (in-memory token bucket).
// =====================================================================
//
// In-memory by design: the cluster-wide counter via SQLite would
// serialize every request on a single DB connection (the writes
// would dominate submission latency). The restart-loss window is
// acceptable because the start of the master drains load and a
// fresh token bucket is more lenient than the prior steady-state —
// the NewResolver path's identity-dedup invariants protect against
// the burstier post-restart window producing duplicate jobs.
//
// One bucket per (client_id) → keyed bucket map. Buckets are
// created lazily on first request and held indefinitely; entries
// are NOT cleared for inactive clients (the GC is "operator calls
// DisableM2MAPIKey", at which point the bucket is unreachable
// anyway).

type m2mRateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*m2mBucket
}

type m2mBucket struct {
	tokens     float64
	capacity   float64
	refillRate float64 // tokens per second
	lastRefill time.Time
}

func newM2MRateLimiter() *m2mRateLimiter {
	return &m2mRateLimiter{
		buckets: make(map[string]*m2mBucket),
	}
}

// take returns true if a token was successfully taken; false if
// the bucket was empty (caller should reject with 429). The bucket
// state is mutated atomically.
//
// Standard token-bucket semantics: capacity is the MAXIMUM tokens
// the bucket can hold at any time. On lazy init the bucket is
// created FULLY loaded then ONE token is immediately consumed for
// the in-progress request. Without the drain-on-init, capacity=N
// effectively grants N+1 requests before exhaustion, which is the
// universal bug in naive implementations.
func (l *m2mRateLimiter) take(clientID string, capacity, refillRate float64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	bucket, ok := l.buckets[clientID]
	if !ok {
		// Lazily allocate. Bucket starts at full capacity; we
		// consume the first token on behalf of this in-flight
		// request so a freshly observed client_id gets exactly
		// `capacity` requests before exhaustion (not capacity+1).
		// capacity < 1 → bucket cannot satisfy this request.
		bucket = &m2mBucket{
			tokens:     capacity - 1,
			capacity:   capacity,
			refillRate: refillRate,
			lastRefill: now,
		}
		l.buckets[clientID] = bucket
		return capacity >= 1
	}
	// Refill based on elapsed time. Cap at capacity so a long
	// downtime doesn't grant a huge burst.
	elapsed := now.Sub(bucket.lastRefill).Seconds()
	bucket.tokens += elapsed * bucket.refillRate
	if bucket.tokens > bucket.capacity {
		bucket.tokens = bucket.capacity
	}
	bucket.lastRefill = now
	if bucket.tokens < 1 {
		return false
	}
	bucket.tokens -= 1
	return true
}

// =====================================================================
// gin.Context keys
// =====================================================================

const (
	m2mCtxKeyClientID  = "m2m_client_id"
	m2mCtxKeyM2MKey    = "m2m_key_row"
	m2mCtxKeyStartedAt = "m2m_started_at"
)

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
func KeyFromContext(c *gin.Context) *store.M2MAPIKey {
	if c == nil {
		return nil
	}
	if v, ok := c.Get(m2mCtxKeyM2MKey); ok {
		if k, ok := v.(*store.M2MAPIKey); ok {
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

		hashed := store.HashM2MSecret(token)
		ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		key, err := st.GetActiveM2MAPIKeyBySecretHash(ctx, hashed)
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

// =====================================================================
// Audit writer (response-capturing wrapper)
// =====================================================================

type m2mAuditWriter struct {
	gin.ResponseWriter
	statusCode int
}

func (w *m2mAuditWriter) StatusCode() int {
	if w.statusCode == 0 {
		return http.StatusOK
	}
	return w.statusCode
}

func (w *m2mAuditWriter) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}

// =====================================================================
// Audit append (helper; the middleware invokes these)
// =====================================================================

// auditM2MReject is called BEFORE AbortWithStatusJSON returns; it
// stamps a row with status_code=the 4xx code we'll return. Best
// effort — errors only log.
func auditM2MReject(c *gin.Context, st *store.SQLiteStore, clientID, reason string) {
	if st == nil {
		return
	}
	if clientID == "" {
		// Anonymous reject — we don't have a key row. Use a single
		// bucket "anonymous" so the audit table isn't littered with
		// NULLs.
		clientID = "anonymous_rejected"
	}
	status := statusFromReason(reason)
	_ = st.AppendM2MAuditLog(c.Request.Context(), store.M2MAuditEntry{
		ClientID:             clientID,
		IdemKeyHash:          idemHashForLog(c.GetHeader("Idempotency-Key")),
		Method:               c.Request.Method,
		Path:                 c.Request.URL.Path,
		StatusCode:           status,
		Scope:                "jobs.submit",
		SceneCount:           0,
		TotalDurationSeconds: 0,
		IPAddress:            c.ClientIP(),
		RejectReason:         sqlNullString(reason),
	})
}

// auditM2MSuccess is called AFTER the handler, with the captured
// status code. For 4xx rejections emitted by the handler itself
// (validator, SSRF, quota), the audit row records the per-request
// resource usage from the parsed body (best-effort; if the handler
// never parsed, counts stay at 0).
func auditM2MSuccess(c *gin.Context, st *store.SQLiteStore, clientID string, statusCode int) {
	if st == nil || clientID == "" {
		return
	}
	scenes, totalDur := usageFromContext(c)
	entry := store.M2MAuditEntry{
		ClientID:             clientID,
		IdemKeyHash:          idemHashForLog(c.GetHeader("Idempotency-Key")),
		Method:               c.Request.Method,
		Path:                 c.Request.URL.Path,
		StatusCode:           statusCode,
		Scope:                "jobs.submit",
		SceneCount:           scenes,
		TotalDurationSeconds: totalDur,
		IPAddress:            c.ClientIP(),
	}
	if statusCode >= 400 {
		entry.RejectReason = sqlNullString("handler_rejected")
	}
	_ = st.AppendM2MAuditLog(c.Request.Context(), entry)
}

// usageFromContext extracts scene count + total duration the
// handler stashed in gin.Context on a successful accept. The
// handler sets these via SetUsageStats so a 202 case records the
// real numbers in the audit row.
func usageFromContext(c *gin.Context) (int, float64) {
	if c == nil {
		return 0, 0
	}
	if v, ok := c.Get("m2m_audit_scene_count"); ok {
		if n, ok := v.(int); ok {
			if v2, ok2 := c.Get("m2m_audit_total_duration_s"); ok2 {
				if d, ok3 := v2.(float64); ok3 {
					return n, d
				}
			}
		}
	}
	return 0, 0
}

// SetUsageStats is called by the handler on a successful accept
// (status 202). Stashes scene count + total duration so the
// audit row records the actual request shape, not zeros.
func SetUsageStats(c *gin.Context, scenes int, totalDurationS float64) {
	if c == nil {
		return
	}
	c.Set("m2m_audit_scene_count", scenes)
	c.Set("m2m_audit_total_duration_s", totalDurationS)
}

// =====================================================================
// Per-request quota (called by the handler)
// =====================================================================

// EnforcePerRequestQuota returns nil when both scene count and
// total duration are within the resolved per-client caps. Cap
// resolution: per-key override > cfg.M2M default.
//
// The handler MUST call this AFTER byte-level idem validation but
// BEFORE invoking the resolver, so a quota-failed request doesn't
// touch creator_forwardings. Returns *QuotaError (HTTP 429 in the
// handler) so the response carries a machine-readable shape.
type QuotaError struct {
	Reason   string
	Observed float64
	Cap      float64
}

func (e *QuotaError) Error() string {
	return "m2m_quota_" + e.Reason + ": observed=" + strconv.FormatFloat(e.Observed, 'f', 2, 64) +
		" cap=" + strconv.FormatFloat(e.Cap, 'f', 2, 64)
}

func EnforcePerRequestQuota(c *gin.Context, req SubmitJobRequest, cfg *config.Config) error {
	_ = cfg // cfg is consumed only when key is non-nil
	key := KeyFromContext(c)
	if key == nil {
		// No M2M context: M2M middleware did not run. This is the
		// shape produced by the `m2mJobsAuthFake` test fixture
		// (the resolver-layer happy path deliberately runs without
		// M2M so production-wiring concerns don't pollute it) and
		// also the default adminAuth fallback for non-prod mounts.
		// Returning nil here is the right semantic: enforce only
		// when there's a key to enforce against. The handler still
		// ran behind SOME auth chain (adminAuth or M2M), so the
		// request was authorized — we just don't have per-client
		// quota numbers to apply.
		return nil
	}
	defaultMaxScenes := cfg.M2M.MaxScenesPerRequest
	defaultMaxDur := cfg.M2M.MaxTotalDurationSecondsPerRequest
	maxScenes := key.EffectiveMaxScenes(defaultMaxScenes)
	if maxScenes > 0 && len(req.Scenes) > maxScenes {
		return &QuotaError{
			Reason:   "scenes_exceeded",
			Observed: float64(len(req.Scenes)),
			Cap:      float64(maxScenes),
		}
	}
	maxDur := key.EffectiveMaxTotalDurationS(defaultMaxDur)
	if maxDur > 0 {
		var sum float64
		for _, s := range req.Scenes {
			sum += s.DurationSeconds
		}
		if sum > maxDur {
			return &QuotaError{
				Reason:   "duration_exceeded",
				Observed: sum,
				Cap:      maxDur,
			}
		}
	}
	return nil
}

// =====================================================================
// Helpers
// =====================================================================

// idemHashForLog returns a 12-char hex prefix of the SHA-256 of
// the supplied idempotency key (or ""), suitable for the
// m2m_audit_log.idem_key_hash column. NEVER includes the raw key —
// raw keys can carry client PII and re-identification risk.
func idemHashForLog(key string) string {
	if key = strings.TrimSpace(key); key == "" {
		return ""
	}
	full := store.HashM2MSecret(key)
	if len(full) >= 12 {
		return full[:12]
	}
	return full
}

// statusFromReason maps a reject reason string to the HTTP status
// the client will see (kept consistent with the four AbortWithStatusJSON
// sites in NewM2MJwAuthMiddleware).
func statusFromReason(reason string) int {
	switch {
	case strings.HasPrefix(reason, "missing_token"),
		strings.HasPrefix(reason, "unknown_or_revoked_token"):
		return http.StatusUnauthorized
	case strings.HasPrefix(reason, "scope_missing"),
		strings.HasPrefix(reason, "forbidden"):
		return http.StatusForbidden
	case strings.HasPrefix(reason, "rate_limited"):
		return http.StatusTooManyRequests
	default:
		return http.StatusInternalServerError
	}
}

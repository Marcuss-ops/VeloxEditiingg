// Package pipeline: m2m_audit.go — the response-capturing audit writer,
// audit-row helpers and the per-request quota enforcement for the M2M
// auth surface. Split out of m2m_auth.go; the middleware lives in
// m2m_auth.go and the rate limiter in m2m_rate_limiter.go.
package pipeline

import (
	"database/sql"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/m2mkeys"
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
	_ = m2mkeys.AppendM2MAuditLog(c.Request.Context(), st.DB(), m2mkeys.M2MAuditEntry{
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
	entry := m2mkeys.M2MAuditEntry{
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
	_ = m2mkeys.AppendM2MAuditLog(c.Request.Context(), st.DB(), entry)
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
	full := m2mkeys.HashM2MSecret(key)
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

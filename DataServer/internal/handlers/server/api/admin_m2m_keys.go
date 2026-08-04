// Package api / admin_m2m_keys.go: admin CRUD endpoints for the
// per-client M2M API keys. Mounted under /api/v1/admin/m2m/keys
// (and /api/v1/admin/m2m/audit) via the existing adminAuth — only
// operators (not external M2M clients) can rotate / disable keys or
// read the audit log. This split is intentional: a M2M client must
// NOT be able to mint another client. Admin lives behind VELOX_ADMIN_TOKEN
// (operator-only DEPLOY_KEY).
//
// Endpoints:
//
//	POST   /api/v1/admin/m2m/keys                       → IssueM2MKey
//	GET    /api/v1/admin/m2m/keys                       → ListM2MKeys
//	GET    /api/v1/admin/m2m/keys/:client_id            → GetM2MKey (no plaintext)
//	DELETE /api/v1/admin/m2m/keys/:client_id            → DisableM2MKey (soft)
//	GET    /api/v1/admin/m2m/audit?client_id=…&limit=…  → ListM2MAudit
//
// The plaintext secret is returned ONCE at creation (POST). It is
// never persisted — the store only holds SHA-256(secret). If the
// operator loses the plaintext response, the supported remediation
// is: POST a new key, DELETE the old one. This is the standard
// "client-secret lost, no recovery, just rotate" policy.
//
// The endpoints respect the adminAuth guard: when AdminAuthMiddleware
// is wired into /api/v1/admin/* in cmd/server/router.go, every
// handler here runs only after the operator's Bearer token passes.
package api

import (
	"context"
	"database/sql"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/store"
)

// =====================================================================
// request / response shapes
// =====================================================================

// issueM2MKeyRequest is the JSON body for POST /api/v1/admin/m2m/keys.
//
// All fields other than ClientID are optional. Defaults applied at
// issue time:
//   - Scopes:             ["jobs.submit"]
//   - Description:        ""
//   - RateLimitRPS:       0 → cfg.M2M.DefaultRPS at enforcement
//   - RateLimitBurst:     0 → cfg.M2M.DefaultBurst
//   - QuotaMaxScenes:     0 → cfg.M2M.MaxScenesPerRequest
//   - QuotaMaxTotalS:     0 → cfg.M2M.MaxTotalDurationSecondsPerRequest
type issueM2MKeyRequest struct {
	ClientID          string   `json:"client_id"`
	Description       string   `json:"description"`
	Scopes            []string `json:"scopes"`
	RateLimitRPS      *int     `json:"rate_limit_rps"`
	RateLimitBurst    *int     `json:"rate_limit_burst"`
	QuotaMaxScenes    *int     `json:"quota_max_scenes"`
	QuotaMaxTotalSecs *float64 `json:"quota_max_total_secs"`
}

// issueM2MKeyResponse is the JSON returned by POST on success.
// PlaintextSecret is the ONLY time the plaintext exists outside the
// admin's clipboard; the row in `m2m_api_keys` contains only its
// SHA-256 hash.
type issueM2MKeyResponse struct {
	ClientID        string    `json:"client_id"`
	PlaintextSecret string    `json:"plaintext_secret"` // returned ONCE
	SecretHash      string    `json:"secret_hash"`
	Scopes          []string  `json:"scopes"`
	IsActive        bool      `json:"is_active"`
	Description     string    `json:"description"`
	RateLimitRPS    *int      `json:"rate_limit_rps"`
	RateLimitBurst  *int      `json:"rate_limit_burst"`
	QuotaMaxScenes  *int      `json:"quota_max_scenes"`
	QuotaMaxTotalS  *float64  `json:"quota_max_total_secs"`
	CreatedAt       time.Time `json:"created_at"`
}

// =====================================================================
// helper: validation
// =====================================================================

// validateIssueM2MKeyRequest vets the request before insertion.
// Reject: empty ClientID, ClientID containing `:`, scopes list with
// unknown scope. Known / accepted scopes: `jobs.submit` (the only
// scope supported at v1).
func validateIssueM2MKeyRequest(req *issueM2MKeyRequest) error {
	if req == nil {
		return errAdminBadRequest("request body is required")
	}
	if cid := strings.TrimSpace(req.ClientID); cid == "" {
		return errAdminBadRequest("client_id is required")
	} else if strings.Contains(cid, ":") || strings.Contains(cid, " ") {
		return errAdminBadRequest("client_id must not contain ':' or whitespace")
	}
	for _, s := range req.Scopes {
		s = strings.TrimSpace(s)
		if s != "" && s != "jobs.submit" {
			return errAdminBadRequest("unsupported scope: " + s + " (only jobs.submit is supported in v1)")
		}
	}
	// Range guards: RPS+Burst non-negative; quotas non-negative.
	if req.RateLimitRPS != nil && *req.RateLimitRPS < 0 {
		return errAdminBadRequest("rate_limit_rps must be >= 0 (0 → use cfg.M2M.DefaultRPS)")
	}
	if req.RateLimitBurst != nil && *req.RateLimitBurst < 0 {
		return errAdminBadRequest("rate_limit_burst must be >= 0 (0 → use cfg.M2M.DefaultBurst)")
	}
	if req.QuotaMaxScenes != nil && *req.QuotaMaxScenes < 0 {
		return errAdminBadRequest("quota_max_scenes must be >= 0 (0 → use cfg.M2M.MaxScenesPerRequest)")
	}
	if req.QuotaMaxTotalSecs != nil && *req.QuotaMaxTotalSecs < 0 {
		return errAdminBadRequest("quota_max_total_secs must be >= 0 (0 → use cfg.M2M.MaxTotalDurationSecondsPerRequest)")
	}
	return nil
}

// errAdminBadRequest is a small typed-error helper keeping the
// 400 envelope consistent with the rest of the API admin surface.
type adminBadRequest struct{ msg string }

func (e *adminBadRequest) Error() string  { return e.msg }
func errAdminBadRequest(msg string) error { return &adminBadRequest{msg: msg} }

// =====================================================================
// handlers (no DB driver dependency on the package — handlers
// receive the store via the wrapped handler's GetM2MStore closure
// installed at boot by cmd/server/router.go).
// =====================================================================

// IssueM2MKey (POST /api/v1/admin/m2m/keys) creates a new key row,
// generates a 32-byte (64 hex char) plaintext secret via crypto/rand,
// returns it once with the typed row metadata, and stores only the
// SHA-256 of the secret in m2m_api_keys.
func IssueM2MKey(st *store.SQLiteStore) gin.HandlerFunc {
	return func(c *gin.Context) {
		if st == nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "store unavailable"})
			return
		}
		var req issueM2MKeyRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   "invalid_payload",
				"message": "request body must be valid JSON",
			})
			return
		}
		req.ClientID = strings.TrimSpace(req.ClientID)
		if err := validateIssueM2MKeyRequest(&req); err != nil {
			if e, ok := err.(*adminBadRequest); ok {
				c.JSON(http.StatusBadRequest, gin.H{
					"error": "invalid_payload", "message": e.Error(),
				})
				return
			}
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_payload", "message": err.Error()})
			return
		}
		plaintext := store.GenerateM2MSecret()
		hash := store.HashM2MSecret(plaintext)
		scopes := req.Scopes
		if len(scopes) == 0 {
			scopes = []string{"jobs.submit"}
		}
		key := store.M2MAPIKey{
			ClientID:       req.ClientID,
			SecretHash:     hash,
			Scopes:         scopes,
			IsActive:       true,
			Description:    req.Description,
			RateLimitRPS:   derefInt(req.RateLimitRPS),
			RateLimitBurst: derefInt(req.RateLimitBurst),
			Quotas: store.M2MQuotas{
				MaxScenes:         derefInt(req.QuotaMaxScenes),
				MaxTotalDurationS: dereqF64(req.QuotaMaxTotalSecs),
			},
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
		defer cancel()
		if err := st.InsertM2MAPIKey(ctx, key); err != nil {
			// The most common error is the PK violation on a duplicate
			// client_id. Map to 409 with a stable error code.
			if strings.Contains(err.Error(), "UNIQUE") || strings.Contains(err.Error(), "constraint failed") {
				c.JSON(http.StatusConflict, gin.H{
					"error":   "duplicate_client_id",
					"message": "client_id already exists",
				})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": "store_failure", "message": err.Error()})
			return
		}

		// Return the row PLUS plaintext_secret (one-shot). The
		// response envelope preserves typed pointers for the
		// operator-supplied overrides so they can copy them into
		// their password manager without re-parsing.
		c.JSON(http.StatusCreated, issueM2MKeyResponse{
			ClientID:        key.ClientID,
			PlaintextSecret: plaintext,
			SecretHash:      hash,
			Scopes:          key.Scopes,
			IsActive:        key.IsActive,
			Description:     key.Description,
			RateLimitRPS:    req.RateLimitRPS,
			RateLimitBurst:  req.RateLimitBurst,
			QuotaMaxScenes:  req.QuotaMaxScenes,
			QuotaMaxTotalS:  req.QuotaMaxTotalSecs,
			CreatedAt:       time.Now().UTC(),
		})
	}
}

// ListM2MKeys (GET /api/v1/admin/m2m/keys) returns all key rows.
// The plaintext is NEVER returned (it doesn't exist in the DB).
// Soft-disabled rows are included so operators can audit revoked
// clients.
func ListM2MKeys(st *store.SQLiteStore) gin.HandlerFunc {
	return func(c *gin.Context) {
		if st == nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "store unavailable"})
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
		defer cancel()
		rows, err := st.ListM2MAPIKeys(ctx)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "store_failure", "message": err.Error()})
			return
		}
		out := make([]gin.H, 0, len(rows))
		for _, r := range rows {
			out = append(out, gin.H{
				"client_id":         r.ClientID,
				"secret_hash":       r.SecretHash,
				"scopes":            r.Scopes,
				"is_active":         r.IsActive,
				"description":       r.Description,
				"rate_limit_rps":    r.RateLimitRPS,
				"rate_limit_burst":  r.RateLimitBurst,
				"quota_max_scenes":  r.Quotas.MaxScenes,
				"quota_max_total_s": r.Quotas.MaxTotalDurationS,
				"created_at":        r.CreatedAt,
				"updated_at":        r.UpdatedAt,
				"last_used_at":      nilOrTime(r.LastUsedAt),
			})
		}
		c.JSON(http.StatusOK, gin.H{"ok": true, "keys": out})
	}
}

// GetM2MKey (GET /api/v1/admin/m2m/keys/:client_id) — single-row
// read. Returns 404 when client_id is unknown. No plaintext (it
// doesn't exist in the DB).
func GetM2MKey(st *store.SQLiteStore) gin.HandlerFunc {
	return func(c *gin.Context) {
		if st == nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "store unavailable"})
			return
		}
		clientID := strings.TrimSpace(c.Param("client_id"))
		if clientID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "client_id required"})
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
		defer cancel()
		k, err := st.GetM2MAPIKeyByClientID(ctx, clientID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "store_failure", "message": err.Error()})
			return
		}
		if k == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "client_id unknown"})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"client_id":         k.ClientID,
			"secret_hash":       k.SecretHash,
			"scopes":            k.Scopes,
			"is_active":         k.IsActive,
			"description":       k.Description,
			"rate_limit_rps":    k.RateLimitRPS,
			"rate_limit_burst":  k.RateLimitBurst,
			"quota_max_scenes":  k.Quotas.MaxScenes,
			"quota_max_total_s": k.Quotas.MaxTotalDurationS,
			"created_at":        k.CreatedAt,
			"updated_at":        k.UpdatedAt,
			"last_used_at":      nilOrTime(k.LastUsedAt),
		})
	}
}

// DisableM2MKey (DELETE /api/v1/admin/m2m/keys/:client_id) is the
// supported revocation path: sets is_active=0 so the middleware
// rejects subsequent requests with the same plaintext. 404 on
// unknown client_id; 204 on success.
func DisableM2MKey(st *store.SQLiteStore) gin.HandlerFunc {
	return func(c *gin.Context) {
		if st == nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "store unavailable"})
			return
		}
		clientID := strings.TrimSpace(c.Param("client_id"))
		if clientID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "client_id required"})
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
		defer cancel()
		if err := st.DisableM2MAPIKey(ctx, clientID); err != nil {
			if err == sql.ErrNoRows {
				c.JSON(http.StatusNotFound, gin.H{"error": "client_id unknown"})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": "store_failure", "message": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"ok":        true,
			"client_id": clientID,
			"is_active": false,
		})
	}
}

// ListM2MAudit (GET /api/v1/admin/m2m/audit?client_id=…&limit=…)
// returns recent audit rows. Optional query filters: client_id
// (exact match), limit (1..1000; default 100). Returns 200 with
// `entries` array. Never includes the original idempotency_key.
func ListM2MAudit(st *store.SQLiteStore) gin.HandlerFunc {
	return func(c *gin.Context) {
		if st == nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "store unavailable"})
			return
		}
		clientID := strings.TrimSpace(c.Query("client_id"))
		limit := 0
		if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil || n <= 0 {
				c.JSON(http.StatusBadRequest, gin.H{"error": "limit must be a positive integer"})
				return
			}
			limit = n
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
		defer cancel()
		rows, err := st.ListM2MAuditLog(ctx, clientID, limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "store_failure", "message": err.Error()})
			return
		}
		out := make([]gin.H, 0, len(rows))
		for _, r := range rows {
			entry := gin.H{
				"id":                     r.ID,
				"client_id":              r.ClientID,
				"idem_key_hash":          r.IdemKeyHash,
				"method":                 r.Method,
				"path":                   r.Path,
				"status_code":            r.StatusCode,
				"scope":                  r.Scope,
				"scene_count":            r.SceneCount,
				"total_duration_seconds": r.TotalDurationSeconds,
				"ip_address":             r.IPAddress,
				"created_at":             r.CreatedAt,
			}
			if r.RejectReason.Valid {
				entry["reject_reason"] = r.RejectReason.String
			}
			out = append(out, entry)
		}
		c.JSON(http.StatusOK, gin.H{"ok": true, "entries": out})
	}
}

// =====================================================================
// helpers
// =====================================================================

func derefInt(p *int) int {
	if p == nil {
		return 0
	}
	return *p
}
func dereqF64(p *float64) float64 {
	if p == nil {
		return 0
	}
	return *p
}
func nilOrTime(t sql.NullTime) any {
	if !t.Valid {
		return nil
	}
	return t.Time.UTC()
}

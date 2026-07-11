package drive

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	integrationsDrive "velox-server/internal/integrations/drive"
)

// DriveHealthCheckHandler verifies Drive token, permissions, and connectivity.
// GET /api/drive/health
func (h *DriveHandlers) DriveHealthCheckHandler(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 15*time.Second)
	defer cancel()

	result := map[string]interface{}{
		"drive_configured": false,
		"checks":           []map[string]interface{}{},
		"checked_at":       time.Now().UTC().Format(time.RFC3339),
	}

	svc := h.svc.DriveService()
	if svc == nil {
		result["status"] = "unavailable"
		result["error"] = "drive service not initialized"
		c.JSON(http.StatusServiceUnavailable, result)
		return
	}

	oauthCfg := svc.GetOAuthConfig()
	if oauthCfg == nil {
		result["status"] = "unavailable"
		result["error"] = "oauth config missing"
		c.JSON(http.StatusServiceUnavailable, result)
		return
	}

	// ── Check 1: OAuth config ──────────────────────────────────────────
	clientID := strings.TrimSpace(oauthCfg.ClientID)
	clientSecret := strings.TrimSpace(oauthCfg.ClientSecret)
	hasCredentials := clientID != "" && clientSecret != ""

	credsCheck := map[string]interface{}{
		"name":   "oauth_credentials",
		"status": "fail",
	}
	if hasCredentials {
		credsCheck["status"] = "pass"
		credsCheck["detail"] = "client_id and client_secret configured"
	} else {
		missing := []string{}
		if clientID == "" {
			missing = append(missing, "client_id")
		}
		if clientSecret == "" {
			missing = append(missing, "client_secret")
		}
		credsCheck["detail"] = "missing: " + strings.Join(missing, ", ")
	}
	result["checks"] = append(result["checks"].([]map[string]interface{}), credsCheck)

	if !hasCredentials {
		result["drive_configured"] = false
		result["status"] = "unconfigured"
		result["error"] = "oauth credentials incomplete"
		c.JSON(http.StatusOK, result)
		return
	}
	result["drive_configured"] = true

	// ── Check 2: Token manager and stored tokens ────────────────────────
	tm := svc.GetTokenManager()
	tokenCheck := map[string]interface{}{
		"name":   "token_loaded",
		"status": "fail",
	}

	var workingToken *integrationsDrive.Token
	tokenNames, listErr := tm.ListTokens()
	if listErr != nil || len(tokenNames) == 0 {
		if listErr != nil {
			tokenCheck["detail"] = "no tokens found: " + listErr.Error()
		} else {
			tokenCheck["detail"] = "no token files on disk"
		}
	} else {
		// Try to load and validate each token
		var tokenDetails []map[string]interface{}
		for _, name := range tokenNames {
			tok, loadErr := tm.LoadToken(name)
			if loadErr != nil || tok == nil {
				tokenDetails = append(tokenDetails, map[string]interface{}{
					"name":   name,
					"status": "error",
					"error":  "failed to load",
				})
				continue
			}

			now := time.Now()
			expired := now.After(tok.Expiry)
			expiresIn := tok.Expiry.Sub(now).Round(time.Second).String()
			if expired {
				expiresIn = "expired (" + expiresIn + " ago)"
			}

			td := map[string]interface{}{
				"name":          name,
				"account_email": tok.AccountEmail,
				"expired":       expired,
				"expires_in":    expiresIn,
				"expiry":        tok.Expiry.UTC().Format(time.RFC3339),
				"scope":         tok.Scope,
				"token_type":    tok.TokenType,
				"has_refresh":   tok.RefreshToken != "",
				"created_at":    tok.CreatedAt.UTC().Format(time.RFC3339),
			}

			// Try to use this token
			svc.SetToken(tok)
			if _, aboutErr := svc.GetAbout(ctx); aboutErr == nil {
				td["status"] = "active"
				if workingToken == nil {
					workingToken = tok
				}
			} else {
				td["status"] = "invalid"
				td["error"] = aboutErr.Error()
			}

			tokenDetails = append(tokenDetails, td)
		}

		tokenCheck["tokens"] = tokenDetails
		tokenCheck["total_tokens"] = len(tokenDetails)

		if workingToken != nil {
			tokenCheck["status"] = "pass"
			tokenCheck["account"] = workingToken.AccountEmail
			tokenCheck["detail"] = "token loaded and validated"
		} else if len(tokenDetails) > 0 {
			tokenCheck["status"] = "warn"
			tokenCheck["detail"] = "tokens found but none validated against Drive API"
		}
	}
	result["checks"] = append(result["checks"].([]map[string]interface{}), tokenCheck)

	if workingToken == nil {
		result["status"] = "no_valid_token"
		result["error"] = "no working Drive token found"
		c.JSON(http.StatusOK, result)
		return
	}

	// Set the working token
	svc.SetToken(workingToken)

	// ── Check 3: Drive API connectivity (GetAbout) ──────────────────────
	connCheck := map[string]interface{}{
		"name":   "drive_connectivity",
		"status": "fail",
	}

	about, aboutErr := svc.GetAbout(ctx)
	if aboutErr != nil {
		connCheck["detail"] = "cannot reach Drive API: " + aboutErr.Error()
	} else {
		connCheck["status"] = "pass"
		connCheck["detail"] = "Drive API reachable"

		// Extract user info
		if user, ok := about["user"].(map[string]interface{}); ok {
			connCheck["user"] = map[string]interface{}{
				"display_name": user["displayName"],
				"email":        user["emailAddress"],
			}
		}
		// Extract storage quota
		if quota, ok := about["storageQuota"].(map[string]interface{}); ok {
			connCheck["storage_quota"] = quota
		}
	}
	result["checks"] = append(result["checks"].([]map[string]interface{}), connCheck)

	// ── Check 4: Permissions / scopes ───────────────────────────────────
	// Build a set of granted scopes from the token scope string.
	tokenScopes := make(map[string]bool)
	for _, s := range strings.Fields(workingToken.Scope) {
		tokenScopes[s] = true
	}

	scopeCheck := map[string]interface{}{
		"name":              "permissions",
		"status":            "fail",
		"configured_scopes": oauthCfg.Scopes,
		"token_scopes":      workingToken.Scope,
	}

	requiredScopes := []string{
		"https://www.googleapis.com/auth/drive.file",
		"https://www.googleapis.com/auth/drive",
	}
	var missingScopes []string
	for _, rs := range requiredScopes {
		if !tokenScopes[rs] {
			missingScopes = append(missingScopes, rs)
		}
	}
	if len(missingScopes) == 0 {
		scopeCheck["status"] = "pass"
		scopeCheck["detail"] = "all expected scopes present"
	} else {
		scopeCheck["status"] = "warn"
		scopeCheck["missing_scopes"] = missingScopes
		scopeCheck["detail"] = "some expected scopes missing from token"
	}
	result["checks"] = append(result["checks"].([]map[string]interface{}), scopeCheck)

	// ── Determine overall status ────────────────────────────────────────
	allPass := true
	hasWarn := false
	for _, ck := range result["checks"].([]map[string]interface{}) {
		s := ck["status"].(string)
		if s == "fail" {
			allPass = false
		}
		if s == "warn" {
			hasWarn = true
		}
	}

	if !allPass && connCheck["status"] != "pass" {
		result["status"] = "degraded"
	} else if hasWarn {
		result["status"] = "degraded"
	} else {
		result["status"] = "healthy"
	}

	c.JSON(http.StatusOK, result)
}

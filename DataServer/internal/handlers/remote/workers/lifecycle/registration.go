package lifecycle

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

// RegisterV2Handler handles worker registration (V2) with persistent
// credential validation via the worker_credentials table.
//
// Flow:
//  1. Parse WorkerInfo from JSON body.
//  2. If Credential is provided:
//     a. Check HasWorkerCredential(workerID).
//     b. If credential exists → ValidateWorkerCredential(workerID, credential):
//     - Match → proceed with registration.
//     - No match → 401 Unauthorized (possible impersonation).
//     c. If no credential exists (first registration) → SetWorkerCredential +
//     proceed.
//  3. If Credential is NOT provided → skip validation (backward compat with
//     pre-credential workers).
//  4. Register worker in the registry, generate session token, return 200.
func (h *Handler) RegisterV2Handler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID   string `json:"worker_id"`
			Credential string `json:"credential,omitempty"`
			// WorkerInfo fields forwarded from the worker's buildHello.
			WorkerName      string                 `json:"worker_name,omitempty"`
			Hostname        string                 `json:"hostname,omitempty"`
			IP              string                 `json:"ip,omitempty"`
			Version         string                 `json:"version,omitempty"`
			CodeVersion     string                 `json:"code_version,omitempty"`
			BundleVersion   string                 `json:"bundle_version,omitempty"`
			BundleHash      string                 `json:"bundle_hash,omitempty"`
			ProtocolVersion string                 `json:"protocol_version,omitempty"`
			EngineVersion   string                 `json:"engine_version,omitempty"`
			Capabilities    map[string]interface{} `json:"capabilities,omitempty"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
			return
		}

		if body.WorkerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id is required"})
			return
		}

		// ── Persistent credential validation ──────────────────────────
		if body.Credential != "" {
			hasCred, err := h.dbStore.HasWorkerCredential(body.WorkerID)
			if err != nil {
				log.Printf("[REGISTER] credential check error for worker %s: %v",
					body.WorkerID[:min(16, len(body.WorkerID))]+"...", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "credential check failed"})
				return
			}

			if hasCred {
				// Known worker — validate the credential.
				match, err := h.dbStore.ValidateWorkerCredential(body.WorkerID, body.Credential)
				if err != nil {
					log.Printf("[REGISTER] credential validation error for worker %s: %v",
						body.WorkerID[:min(16, len(body.WorkerID))]+"...", err)
					c.JSON(http.StatusInternalServerError, gin.H{"error": "credential validation failed"})
					return
				}
				if !match {
					log.Printf("[REGISTER] credential mismatch for worker %s — rejecting",
						body.WorkerID[:min(16, len(body.WorkerID))]+"...")
					c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid credential"})
					return
				}
				log.Printf("[REGISTER] credential validated for known worker %s",
					body.WorkerID[:min(16, len(body.WorkerID))]+"...")
			} else {
				// First registration — persist the credential hash directly.
				// The worker already sends SHA256(workerID + ":" + secret) as
				// the credential field; don't hash it again.
				if err := h.dbStore.SetWorkerCredential(body.WorkerID, body.Credential); err != nil {
					log.Printf("[REGISTER] failed to store credential for new worker %s: %v",
						body.WorkerID[:min(16, len(body.WorkerID))]+"...", err)
					c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to store credential"})
					return
				}
				log.Printf("[REGISTER] credential stored for new worker %s",
					body.WorkerID[:min(16, len(body.WorkerID))]+"...")
			}
		}
		// Else: no credential → skip validation (backward compat).

		// ── Registration ─────────────────────────────────────────────
		// Register the worker in the in-memory registry.
		ctx := c.Request.Context()
		if err := h.reg.RegisterWorker(ctx, body.WorkerID, body.WorkerName, body.IP, nil); err != nil {
			log.Printf("[REGISTER] failed to register worker %s in registry: %v",
				body.WorkerID[:min(16, len(body.WorkerID))]+"...", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to register worker"})
			return
		}

		// Generate a fresh session token.
		sessionToken := h.tokenMgr.GenerateToken(body.WorkerID)
		if sessionToken == "" {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate session token"})
			return
		}

		log.Printf("[REGISTER] worker %s registered successfully (credential=%v)",
			body.WorkerID[:min(16, len(body.WorkerID))]+"...", body.Credential != "")

		c.JSON(http.StatusOK, gin.H{
			"ok":         true,
			"worker_id":  body.WorkerID,
			"session_id": sessionToken,
			"message":    "worker registered",
		})
	}
}

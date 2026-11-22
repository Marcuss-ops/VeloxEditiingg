package pipeline

import (
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/forwardingstore"
)

// CancelSubmittedJob cancels an M2M-owned render job and the task/attempts
// atomically through the canonical job writer. The forwarding lookup is an
// ownership check; callers cannot cancel jobs created by another client.
func (h *Handlers) CancelSubmittedJob() gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID := strings.TrimSpace(c.Param("id"))
		clientID := strings.TrimSpace(ClientIDFromContext(c))
		if jobID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "job_id_required"})
			return
		}
		if clientID == "" || h.store == nil {
			writeM2MJobNotFound(c)
			return
		}
		if h.jobs.Writer == nil || h.jobs.Reader == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": "job_cancellation_unavailable"})
			return
		}
		forwarding, err := h.store.Forwarding().GetCreatorForwardingByTargetJobID(c.Request.Context(), jobID, clientID)
		if err != nil {
			if errors.Is(err, forwardingstore.ErrCreatorForwardingNoRow) {
				writeM2MJobNotFound(c)
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "store_failure"})
			return
		}
		if forwarding == nil {
			writeM2MJobNotFound(c)
			return
		}
		current, err := h.jobs.Reader.Get(c.Request.Context(), jobID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "store_failure"})
			return
		}
		if current == nil {
			writeM2MJobNotFound(c)
			return
		}
		if current.Status.IsTerminal() {
			c.JSON(http.StatusOK, gin.H{"ok": true, "job_id": jobID, "status": string(current.Status), "terminal": true, "status_url": "/api/v1/jobs/" + jobID})
			return
		}
		if err := h.jobs.Writer.Cancel(c.Request.Context(), jobID, "cancelled by job owner", -1); err != nil {
			// The render may have completed between the status read and cancel
			// CAS. Re-read the authority and return that terminal outcome so the
			// caller can reconcile a successful artifact instead of guessing.
			latest, readErr := h.jobs.Reader.Get(c.Request.Context(), jobID)
			if readErr == nil && latest != nil && latest.Status.IsTerminal() {
				c.JSON(http.StatusOK, gin.H{"ok": true, "job_id": jobID, "status": string(latest.Status), "terminal": true, "status_url": "/api/v1/jobs/" + jobID})
				return
			}
			c.JSON(http.StatusConflict, gin.H{"ok": false, "error": "job_not_cancellable"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"ok": true, "job_id": jobID, "status": "CANCELLED", "terminal": true, "status_url": "/api/v1/jobs/" + jobID})
	}
}

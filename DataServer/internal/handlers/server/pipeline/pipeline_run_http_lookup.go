package pipeline

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"velox-server/internal/forwardingcontract"
	"velox-server/internal/pipelineruns"

	"github.com/gin-gonic/gin"
)

func pipelineRunJobID(pr *pipelineruns.PipelineRun, forwarding *forwardingcontract.CreatorForwarding) string {
	if pr == nil {
		return ""
	}
	if pr.VeloxJobID != "" {
		return pr.VeloxJobID
	}
	if forwarding != nil {
		return forwarding.TargetJobID
	}
	return ""
}

// lookupPipelineRunRequest owns the common HTTP validation and error mapping
// for pipeline run endpoints. The endpoint supplies its established
// unavailable message so responses remain compatible.
func (h *Handlers) lookupPipelineRunRequest(
	c *gin.Context,
	storeUnavailable string,
) (context.Context, string, string, *pipelineruns.PipelineRun, *forwardingcontract.CreatorForwarding, bool) {
	if h.store == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": storeUnavailable})
		return nil, "", "", nil, nil, false
	}
	id := strings.TrimSpace(c.Param("id"))
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "id is required"})
		return nil, "", "", nil, nil, false
	}
	ctx := c.Request.Context()
	clientID := strings.TrimSpace(ClientIDFromContext(c))
	pr, forwarding, err := h.lookupPipelineRun(ctx, id, clientID)
	if err != nil {
		if errors.Is(err, errPipelineRunNotFound) {
			if clientID != "" {
				writeM2MJobNotFound(c)
			} else {
				c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "pipeline run not found"})
			}
			return nil, "", "", nil, nil, false
		}
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return nil, "", "", nil, nil, false
	}
	return ctx, id, clientID, pr, forwarding, true
}

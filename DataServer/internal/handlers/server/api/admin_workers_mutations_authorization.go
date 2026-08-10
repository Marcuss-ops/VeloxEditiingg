package api

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"velox-server/internal/fleet"
)

// authorizeMutation enforces the runtime update capability gate. Admin
// authentication itself remains the route middleware.
func (h *AdminWorkersMutationsHandler) authorizeMutation(c *gin.Context, kind string) bool {
	if kind == fleet.OperationKindUpdate && h.updateGate != nil {
		if err := h.updateGate(); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error":  "update capability not ready",
				"detail": err.Error(),
			})
			return false
		}
	}
	return true
}

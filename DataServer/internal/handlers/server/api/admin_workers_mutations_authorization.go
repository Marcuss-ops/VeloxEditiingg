package api

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"velox-server/internal/fleet"
)

// authorizeMutation enforces the runtime update capability gate. Admin
// authentication itself remains the route middleware.
func (h *AdminWorkersMutationsHandler) authorizeMutation(c *gin.Context, kind string) bool {
	var gate func() error
	switch kind {
	case fleet.OperationKindUpdate:
		gate = h.updateGate
	case fleet.OperationKindResume:
		gate = h.resumeGate
	}
	if gate != nil {
		if err := gate(); err != nil {
			message := "mutation capability not ready"
			if kind == fleet.OperationKindUpdate {
				message = "update capability not ready"
			}
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error":  message,
				"detail": err.Error(),
			})
			return false
		}
	}
	return true
}

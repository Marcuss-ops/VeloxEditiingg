package pipeline

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func writeM2MJobNotFound(c *gin.Context) {
	c.JSON(http.StatusNotFound, gin.H{
		"ok":      false,
		"error":   "job_not_found",
		"message": "job_id does not match any known creator forwarding",
	})
}

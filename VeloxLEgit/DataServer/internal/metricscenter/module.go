// Package metricscenter provides the Velox Metrics Center dashboard UI.
// Served at /metrics-center/ — a single-page HTML+JS app that reads
// data from the existing /api/observability REST endpoints.
package metricscenter

import (
	_ "embed"
	"net/http"

	"github.com/gin-gonic/gin"
)

//go:embed dashboard.html
var dashboardHTML []byte

// Module implements app.Module for the metrics center dashboard.
type Module struct{}

// NewModule creates a new metrics center module.
func NewModule() *Module {
	return &Module{}
}

// Name returns the module identifier.
func (m *Module) Name() string {
	return "metrics-center"
}

// RegisterRoutes registers the /metrics-center/ route.
func (m *Module) RegisterRoutes(r *gin.Engine) {
	r.GET("/metrics-center/", func(c *gin.Context) {
		c.Data(http.StatusOK, "text/html; charset=utf-8", dashboardHTML)
	})
	r.GET("/metrics-center", func(c *gin.Context) {
		c.Redirect(http.StatusMovedPermanently, "/metrics-center/")
	})
}

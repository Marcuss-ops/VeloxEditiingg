package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/metrics"
)

func TestRegisterMetricsRoutesUsesAuthWhenProvided(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	registerMetricsRoutes(r, MetricsRouteDeps{Registry: metrics.NewRegistry()}, func(c *gin.Context) {
		c.AbortWithStatus(http.StatusUnauthorized)
	})

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	res := httptest.NewRecorder()
	r.ServeHTTP(res, req)
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("metrics status = %d, want %d", res.Code, http.StatusUnauthorized)
	}
}

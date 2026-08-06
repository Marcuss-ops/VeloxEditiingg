package main

import (
	"fmt"
	"log"
	"time"

	"github.com/gin-gonic/gin"

	velmetrics "velox-server/internal/metrics"
)

func configureTrustedProxies(r *gin.Engine) {
	if err := r.SetTrustedProxies([]string{"127.0.0.1", "::1"}); err != nil {
		log.Printf("bootstrap: SetTrustedProxies failed: %v", err)
	}
}

func requestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.GetHeader("X-Request-ID") == "" {
			c.Writer.Header().Set("X-Request-ID", fmt.Sprintf("%d", time.Now().UnixNano()))
		}
		c.Next()
	}
}

func accessLogMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		log.Printf("%s %s %d %s", c.Request.Method, c.Request.URL.Path, c.Writer.Status(), time.Since(start))
	}
}

// routeUsageMiddleware counts every control-plane HTTP request against the
// Phase 6 route-surface taxonomy (agent | admin | fleet | legacy | other).
// The route label is the gin FullPath TEMPLATE (e.g.
// "/api/v1/admin/workers/:worker_id"), so cardinality stays bounded by the
// route table regardless of traffic volume. A nil sink (metrics exporter
// disabled) is a no-op.
//
// Legacy surfaces — the old /api/v1/workers diagnostic surface, the
// /api/v1/worker-assets agent path, the /worker admin group, and the
// pre-canonical fleet aggregates under /api/v1/admin/* — are deliberately
// counted under surface=legacy so the removal decision is evidence-driven:
// a legacy route with sustained usage must stay; a quiet one can be
// retired. Requests that match NO route template (404 on an unknown path)
// have an empty FullPath and are dropped by RecordHTTPRoute, so the
// counter covers only the registered route table by design.
func routeUsageMiddleware(sink velmetrics.HTTPRouteUsageSink) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()
		if sink == nil {
			return
		}
		sink.RecordHTTPRoute(velmetrics.ClassifyRouteSurface(c.FullPath()), c.FullPath())
	}
}

func addGzipHeaders() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Vary", "Accept-Encoding")
		c.Next()
	}
}

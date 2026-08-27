// Package api — Admin concurrent benchmark endpoint.
//
// POST /api/v1/admin/benchmarks/concurrent — triggers a concurrent benchmark
// run that tests 1, 2, 3, 4 concurrent jobs and determines the sweet spot
// per VPS.
//
// The handler validates the request, triggers the benchmark run, and returns
// the full result including throughput gains and the recommended sweet spot.
package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/performance"
)

// ConcurrentBenchmarkDeps holds the dependencies for the concurrent benchmark
// handler.
type ConcurrentBenchmarkDeps struct {
	// Renderer executes individual benchmark renders. Must not be nil.
	Renderer performance.RenderRunner
}

// ConcurrentBenchmarkRequest is the request body for the concurrent benchmark
// endpoint.
type ConcurrentBenchmarkRequest struct {
	FixtureID           string `json:"fixture_id"`
	MaxConcurrency      int    `json:"max_concurrency"`
	RunsPerLevel        int    `json:"runs_per_level"`
	WorkerID            string `json:"worker_id"`
	CacheMode           string `json:"cache_mode"`
}

// AdminConcurrentBenchmarkHandler serves POST /api/v1/admin/benchmarks/concurrent.
type AdminConcurrentBenchmarkHandler struct {
	deps ConcurrentBenchmarkDeps
}

// NewAdminConcurrentBenchmarkHandler creates a new handler.
func NewAdminConcurrentBenchmarkHandler(deps ConcurrentBenchmarkDeps) *AdminConcurrentBenchmarkHandler {
	return &AdminConcurrentBenchmarkHandler{deps: deps}
}

// RunConcurrentBenchmark returns a gin.HandlerFunc for the concurrent benchmark
// endpoint. It triggers the benchmark and returns the full result.
func (h *AdminConcurrentBenchmarkHandler) RunConcurrentBenchmark() gin.HandlerFunc {
	return func(c *gin.Context) {
		var req ConcurrentBenchmarkRequest
		if err := json.NewDecoder(c.Request.Body).Decode(&req); err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
			return
		}

		// Validate required fields
		if req.FixtureID == "" {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "fixture_id is required"})
			return
		}
		if req.WorkerID == "" {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "worker_id is required"})
			return
		}

		// Set defaults
		if req.MaxConcurrency <= 0 {
			req.MaxConcurrency = 4
		}
		if req.MaxConcurrency > 8 {
			req.MaxConcurrency = 8
		}
		if req.RunsPerLevel <= 0 {
			req.RunsPerLevel = 3
		}
		if req.RunsPerLevel > 10 {
			req.RunsPerLevel = 10
		}

		// Build config
		config := performance.ConcurrentBenchmarkConfig{
			FixtureID:      performance.BenchmarkFixtureID(req.FixtureID),
			MaxConcurrency: req.MaxConcurrency,
			RunsPerLevel:   req.RunsPerLevel,
			CacheMode:      performance.CacheMode(req.CacheMode),
		}

		// Create a minimal fixture from the request
		fixture := performance.BenchmarkFixture{
			ID: performance.BenchmarkFixtureID(req.FixtureID),
		}

		// Run the benchmark with a timeout
		ctx := c.Request.Context()
		if _, ok := ctx.Deadline(); !ok {
			var cancel func()
			ctx, cancel = context.WithTimeout(ctx, 5*time.Minute)
			defer cancel()
		}

		result, err := performance.RunConcurrentBenchmark(
			ctx,
			h.deps.Renderer,
			fixture,
			config,
			req.WorkerID,
		)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				"error": "benchmark run failed: " + err.Error(),
			})
			return
		}

		c.JSON(http.StatusOK, result)
	}
}

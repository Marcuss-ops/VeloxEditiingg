package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/handlers/server/api"
	"velox-server/internal/performance"
)

const benchmarkRunsPath = "/api/v1/performance/benchmarks/runs"

func registerBenchmarkRoutes(r *gin.Engine, deps MetricsRouteDeps, cfg *config.Config) {
	if deps.BenchmarkRuns == nil || cfg == nil {
		return
	}
	r.POST(benchmarkRunsPath, api.AdminAuthMiddleware(cfg), recordBenchmarkRunHandler(deps.BenchmarkRuns))
}

func recordBenchmarkRunHandler(repo performance.BenchmarkRunRepository) gin.HandlerFunc {
	return func(c *gin.Context) {
		var run performance.BenchmarkRun
		decoder := json.NewDecoder(c.Request.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&run); err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "invalid benchmark run payload"})
			return
		}
		if err := repo.RecordBenchmarkRun(c.Request.Context(), &run); err != nil {
			if performance.IsBenchmarkRunConflict(err) {
				c.AbortWithStatusJSON(http.StatusConflict, gin.H{"error": err.Error()})
				return
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				c.AbortWithStatusJSON(http.StatusRequestTimeout, gin.H{"error": "benchmark persistence canceled"})
				return
			}
			if performance.IsBenchmarkRunValidation(err) {
				c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "benchmark persistence failed"})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"run_id": run.RunID,
			"status": "recorded",
		})
	}
}

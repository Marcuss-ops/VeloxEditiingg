// Package api — Admin benchmark validation endpoint.
//
// POST /api/v1/admin/benchmarks/validate — runs a concurrent benchmark on a
// worker, compares the observed sweet spot against the scorecard prediction,
// persists the result, and returns threshold tuning suggestions.

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/performance"
	"velox-server/internal/store"
)

// BenchmarkValidateDeps holds the dependencies for the validate endpoint.
type BenchmarkValidateDeps struct {
	// Renderer executes individual benchmark renders.
	Renderer performance.RenderRunner
	// Store persists benchmark results and reads the current scorecard.
	Store *store.SQLiteStore
}

// BenchmarkValidateRequest is the request body.
type BenchmarkValidateRequest struct {
	FixtureID      string `json:"fixture_id"`
	WorkerID       string `json:"worker_id"`
	MaxConcurrency int    `json:"max_concurrency"`
	RunsPerLevel   int    `json:"runs_per_level"`
	CacheMode      string `json:"cache_mode"`
}

// BenchmarkValidateResponse is the response body.
type BenchmarkValidateResponse struct {
	performance.ConcurrentBenchmarkResult
	Validation performance.ValidationResult `json:"validation"`
}

// AdminBenchmarkValidateHandler serves POST /api/v1/admin/benchmarks/validate.
type AdminBenchmarkValidateHandler struct {
	deps BenchmarkValidateDeps
}

// NewAdminBenchmarkValidateHandler creates a new handler.
func NewAdminBenchmarkValidateHandler(deps BenchmarkValidateDeps) *AdminBenchmarkValidateHandler {
	return &AdminBenchmarkValidateHandler{deps: deps}
}

// RunAndValidate returns a gin.HandlerFunc for the validate endpoint.
func (h *AdminBenchmarkValidateHandler) RunAndValidate() gin.HandlerFunc {
	return func(c *gin.Context) {
		var req BenchmarkValidateRequest
		if err := json.NewDecoder(c.Request.Body).Decode(&req); err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
			return
		}
		if req.FixtureID == "" || req.WorkerID == "" {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "fixture_id and worker_id are required"})
			return
		}
		// Fail-closed: nil renderer means the benchmark capability is
		// not configured. Return 503 instead of panicking.
		if h.deps.Renderer == nil {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{
				"code":  "BENCHMARK_MISCONFIGURED",
				"error": "benchmark renderer is not configured",
			})
			return
		}
		if req.MaxConcurrency <= 0 {
			req.MaxConcurrency = 4
		}
		if req.RunsPerLevel <= 0 {
			req.RunsPerLevel = 3
		}

		config := performance.ConcurrentBenchmarkConfig{
			FixtureID:      performance.BenchmarkFixtureID(req.FixtureID),
			MaxConcurrency: req.MaxConcurrency,
			RunsPerLevel:   req.RunsPerLevel,
			CacheMode:      performance.CacheMode(req.CacheMode),
		}
		fixture := performance.BenchmarkFixture{
			ID: performance.BenchmarkFixtureID(req.FixtureID),
		}

		ctx := c.Request.Context()
		if _, ok := ctx.Deadline(); !ok {
			var cancel func()
			ctx, cancel = context.WithTimeout(ctx, 10*time.Minute)
			defer cancel()
		}

		// 1. Run the benchmark
		result, err := performance.RunConcurrentBenchmark(ctx, h.deps.Renderer, fixture, config, req.WorkerID)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "benchmark run failed: " + err.Error()})
			return
		}

		// 2. Persist the benchmark result
		if h.deps.Store != nil {
			row := store.BenchmarkResultRow{
				WorkerID:       req.WorkerID,
				BenchmarkRunID: result.BenchmarkRunID,
				FixtureID:      req.FixtureID,
				MaxConcurrency: req.MaxConcurrency,
				RunsPerLevel:   req.RunsPerLevel,
				CacheMode:      req.CacheMode,
				SweetSpot:      result.SweetSpot,
				LimitingFactor: result.LimitingFactor,
				Levels:         result.Levels,
				Gains:          result.Gains,
				Summary:        result.Summary,
				StartedAt:      result.StartedAt.Format(time.RFC3339),
				CompletedAt:    result.CompletedAt.Format(time.RFC3339),
			}
			_ = h.deps.Store.UpsertBenchmarkResult(ctx, row)
		}

		// 3. Validate against the scorecard prediction
		validation := h.validateScorecard(req.WorkerID, result)

		// 4. Persist validation results
		if h.deps.Store != nil {
			_ = h.deps.Store.UpdateBenchmarkValidation(ctx,
				result.BenchmarkRunID,
				&validation.PredictedSweetSpot,
				&validation.ObservedSweetSpot,
				&validation.Accuracy,
				&validation.SuggestedRAMSafety,
				&validation.SuggestedDiskSafety,
				&validation.SuggestedNetworkSafety,
				&validation.Rationale,
			)
		}

		resp := BenchmarkValidateResponse{
			ConcurrentBenchmarkResult: *result,
			Validation:                validation,
		}
		c.JSON(http.StatusOK, resp)
	}
}

// validateScorecard reads the persisted scorecard and compares its prediction
// against the benchmark result.
func (h *AdminBenchmarkValidateHandler) validateScorecard(workerID string, result *performance.ConcurrentBenchmarkResult) performance.ValidationResult {
	const defRAM = 0.75
	const defDisk = 0.75
	const defNetwork = 0.80
	defaultResult := func(reason string) performance.ValidationResult {
		return performance.ValidationResult{
			PredictedSweetSpot:     result.SweetSpot,
			ObservedSweetSpot:      result.SweetSpot,
			Accuracy:               "no_data",
			SuggestedRAMSafety:     defRAM,
			SuggestedDiskSafety:    defDisk,
			SuggestedNetworkSafety: defNetwork,
			Rationale:              reason,
		}
	}
	if h.deps.Store == nil {
		return defaultResult("no store available for scorecard lookup")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sc, err := h.deps.Store.GetScorecard(ctx, workerID)
	if err != nil || sc == nil {
		return defaultResult("no persisted scorecard found for comparison")
	}

	prediction := performance.ScorecardPrediction{
		RenderSlots: sc.RenderSlots,
		SweetSpot:   sc.RenderSlots, // conservative: use render slots as sweet spot proxy
	}

	return performance.ValidateScorecard(prediction, result)
}

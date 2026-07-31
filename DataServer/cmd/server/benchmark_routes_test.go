package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"velox-server/internal/config"
	"velox-server/internal/performance"
)

type benchmarkRunRepoStub struct {
	err   error
	calls int
}

func (s *benchmarkRunRepoStub) RecordBenchmarkRun(_ context.Context, _ *performance.BenchmarkRun) error {
	s.calls++
	return s.err
}

func (s *benchmarkRunRepoStub) GetBenchmarkRun(context.Context, string) (*performance.BenchmarkRun, error) {
	return nil, nil
}

const benchmarkRoutePayload = `{
  "run_id":"gervais-final-v1:cold_cache:attempt-1",
  "benchmark_case_id":"gervais-final-v1",
  "job_id":"job-1",
  "task_id":"task-1",
  "attempt_id":"attempt-1",
  "worker_id":"worker-1",
  "cache_mode":"cold_cache",
  "status":"SUCCEEDED",
  "wall_ms":3000,
  "output_duration_ms":6000
}`

func TestRecordBenchmarkRunEndpoint_AuthReplayAndConflict(t *testing.T) {
	gin.SetMode(gin.TestMode)
	stub := &benchmarkRunRepoStub{}
	r := gin.New()
	cfg := &config.Config{Auth: config.AuthConfig{AdminToken: "test-admin"}}
	registerBenchmarkRoutes(r, MetricsRouteDeps{BenchmarkRuns: stub}, cfg)

	request := func(auth, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, benchmarkRunsPath, strings.NewReader(body))
		if auth != "" {
			req.Header.Set("Authorization", "Bearer "+auth)
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		return w
	}

	if got := request("wrong", benchmarkRoutePayload).Code; got != http.StatusUnauthorized {
		t.Fatalf("wrong auth status=%d, want %d", got, http.StatusUnauthorized)
	}
	if got := request("test-admin", benchmarkRoutePayload).Code; got != http.StatusOK {
		t.Fatalf("first record status=%d, want %d", got, http.StatusOK)
	}
	if got := request("test-admin", benchmarkRoutePayload).Code; got != http.StatusOK {
		t.Fatalf("identical replay status=%d, want %d", got, http.StatusOK)
	}
	if stub.calls != 2 {
		t.Fatalf("repository calls=%d, want 2", stub.calls)
	}

	stub.err = &performance.BenchmarkRunConflictError{RunID: "gervais-final-v1:cold_cache:attempt-1"}
	if got := request("test-admin", benchmarkRoutePayload).Code; got != http.StatusConflict {
		t.Fatalf("conflict status=%d, want %d", got, http.StatusConflict)
	}
	if got := request("test-admin", `{"run_id":`).Code; got != http.StatusBadRequest {
		t.Fatalf("invalid JSON status=%d, want %d", got, http.StatusBadRequest)
	}
}

func TestRecordBenchmarkRunEndpoint_RepositoryFailureIsServerError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	stub := &benchmarkRunRepoStub{err: errors.New("sqlite unavailable")}
	r := gin.New()
	cfg := &config.Config{Auth: config.AuthConfig{AdminToken: "test-admin"}}
	registerBenchmarkRoutes(r, MetricsRouteDeps{BenchmarkRuns: stub}, cfg)

	req := httptest.NewRequest(http.MethodPost, benchmarkRunsPath, strings.NewReader(benchmarkRoutePayload))
	req.Header.Set("Authorization", "Bearer test-admin")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("repository failure status=%d, want %d", w.Code, http.StatusInternalServerError)
	}
}

package performance

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	BenchmarkCaseGervaisFinalV1 = "gervais-final-v1"
	BenchmarkCacheCold          = "cold_cache"
	BenchmarkCacheWarm          = "warm_cache"
)

// BenchmarkRunValidationError means the request does not satisfy the
// benchmark contract and should be reported as HTTP 400 by the endpoint.
type BenchmarkRunValidationError struct{ Message string }

func (e *BenchmarkRunValidationError) Error() string { return e.Message }

func IsBenchmarkRunValidation(err error) bool {
	var target *BenchmarkRunValidationError
	return errors.As(err, &target)
}

// ErrBenchmarkRunConflict means a replay reused run_id with different
// benchmark evidence. Existing rows are immutable.
type BenchmarkRunConflictError struct{ RunID string }

func (e *BenchmarkRunConflictError) Error() string {
	return fmt.Sprintf("performance benchmark run conflict: run_id %q already has different evidence", e.RunID)
}

// IsBenchmarkRunConflict reports whether err is a divergent replay conflict.
func IsBenchmarkRunConflict(err error) bool {
	var target *BenchmarkRunConflictError
	return errors.As(err, &target)
}

// BenchmarkRun is one immutable benchmark observation submitted by the
// benchmark harness. PayloadHash is derived from the canonical evidence
// fields and is not supplied as trusted input by callers.
type BenchmarkRun struct {
	RunID             string    `json:"run_id"`
	PayloadHash       string    `json:"payload_hash,omitempty"`
	BenchmarkCaseID   string    `json:"benchmark_case_id"`
	JobID             string    `json:"job_id"`
	TaskID            string    `json:"task_id"`
	AttemptID         string    `json:"attempt_id"`
	WorkerID          string    `json:"worker_id"`
	WorkerSnapshotID  string    `json:"worker_snapshot_id,omitempty"`
	CacheMode         string    `json:"cache_mode"`
	GitSHA            string    `json:"git_sha,omitempty"`
	EngineVersion     string    `json:"engine_version,omitempty"`
	FFmpegVersion     string    `json:"ffmpeg_version,omitempty"`
	ConfigHash        string    `json:"config_hash,omitempty"`
	DockerImageDigest string    `json:"docker_image_digest,omitempty"`
	Status            string    `json:"status"`
	RenderFactor      float64   `json:"render_factor"`
	WallMS            float64   `json:"wall_ms"`
	OutputDurationMS  float64   `json:"output_duration_ms"`
	OutputSHA256      string    `json:"output_sha256,omitempty"`
	CreatedAt         time.Time `json:"created_at"`
}

// BenchmarkRunRepository persists immutable benchmark evidence.
type BenchmarkRunRepository interface {
	RecordBenchmarkRun(ctx context.Context, run *BenchmarkRun) error
	GetBenchmarkRun(ctx context.Context, runID string) (*BenchmarkRun, error)
}

// canonicalBenchmarkRun contains only fields that define benchmark evidence.
// PayloadHash and CreatedAt are intentionally excluded so identical replays
// remain idempotent even when received at different times.
type canonicalBenchmarkRun struct {
	RunID             string  `json:"run_id"`
	BenchmarkCaseID   string  `json:"benchmark_case_id"`
	JobID             string  `json:"job_id"`
	TaskID            string  `json:"task_id"`
	AttemptID         string  `json:"attempt_id"`
	WorkerID          string  `json:"worker_id"`
	WorkerSnapshotID  string  `json:"worker_snapshot_id"`
	CacheMode         string  `json:"cache_mode"`
	GitSHA            string  `json:"git_sha"`
	EngineVersion     string  `json:"engine_version"`
	FFmpegVersion     string  `json:"ffmpeg_version"`
	ConfigHash        string  `json:"config_hash"`
	DockerImageDigest string  `json:"docker_image_digest"`
	Status            string  `json:"status"`
	RenderFactor      float64 `json:"render_factor"`
	WallMS            float64 `json:"wall_ms"`
	OutputDurationMS  float64 `json:"output_duration_ms"`
	OutputSHA256      string  `json:"output_sha256"`
}

func (r *BenchmarkRun) canonical() canonicalBenchmarkRun {
	return canonicalBenchmarkRun{
		RunID: r.RunID, BenchmarkCaseID: r.BenchmarkCaseID, JobID: r.JobID,
		TaskID: r.TaskID, AttemptID: r.AttemptID, WorkerID: r.WorkerID,
		WorkerSnapshotID: r.WorkerSnapshotID, CacheMode: r.CacheMode,
		GitSHA: r.GitSHA, EngineVersion: r.EngineVersion, FFmpegVersion: r.FFmpegVersion,
		ConfigHash: r.ConfigHash, DockerImageDigest: r.DockerImageDigest,
		Status: r.Status, RenderFactor: r.RenderFactor, WallMS: r.WallMS,
		OutputDurationMS: r.OutputDurationMS, OutputSHA256: r.OutputSHA256,
	}
}

// ComputeBenchmarkRunPayloadHash returns the stable SHA-256 of benchmark evidence.
func ComputeBenchmarkRunPayloadHash(r *BenchmarkRun) (string, error) {
	if r == nil {
		return "", fmt.Errorf("benchmark run is nil")
	}
	data, err := json.Marshal(r.canonical())
	if err != nil {
		return "", fmt.Errorf("marshal benchmark run evidence: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func (r *BenchmarkRun) Validate() error {
	if r == nil {
		return &BenchmarkRunValidationError{Message: "benchmark run is nil"}
	}
	if strings.TrimSpace(r.RunID) == "" || strings.TrimSpace(r.JobID) == "" ||
		strings.TrimSpace(r.TaskID) == "" || strings.TrimSpace(r.AttemptID) == "" ||
		strings.TrimSpace(r.WorkerID) == "" {
		return &BenchmarkRunValidationError{Message: "benchmark run requires run_id, job_id, task_id, attempt_id, and worker_id"}
	}
	if strings.TrimSpace(r.BenchmarkCaseID) != BenchmarkCaseGervaisFinalV1 {
		return &BenchmarkRunValidationError{Message: fmt.Sprintf("unsupported benchmark_case_id %q", r.BenchmarkCaseID)}
	}
	if r.CacheMode != BenchmarkCacheCold && r.CacheMode != BenchmarkCacheWarm {
		return &BenchmarkRunValidationError{Message: fmt.Sprintf("unsupported cache_mode %q", r.CacheMode)}
	}
	if strings.TrimSpace(r.Status) == "" {
		return &BenchmarkRunValidationError{Message: "benchmark run status is required"}
	}
	return nil
}

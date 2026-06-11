// Package worker provides stage-aware chunk execution for the GOD workflow.
package worker

import (
	"context"
	"time"
)

// StageType represents a processing stage in the job pipeline.
type StageType string

const (
	StageProbe  StageType = "probe"
	StageAudio  StageType = "audio"
	StageVideo  StageType = "video"
	StageConcat StageType = "concat"
	StageUpload StageType = "upload"
)

// StageOrder defines the canonical execution order of stages.
var StageOrder = []StageType{StageProbe, StageAudio, StageVideo, StageConcat, StageUpload}

// ChunkResult represents the result of a single chunk execution.
type ChunkResult struct {
	Stage     StageType              `json:"stage"`
	ChunkID   string                 `json:"chunk_id"`
	Success   bool                   `json:"success"`
	Output    map[string]interface{} `json:"output,omitempty"`
	Error     string                 `json:"error,omitempty"`
	Duration  time.Duration          `json:"duration"`
	Attempt   int                    `json:"attempt"`
}

// StageResult represents the result of a complete stage execution.
type StageResult struct {
	Stage    StageType              `json:"stage"`
	Success  bool                   `json:"success"`
	Chunks   []ChunkResult          `json:"chunks"`
	Output   map[string]interface{} `json:"output,omitempty"`
	Error    string                 `json:"error,omitempty"`
	Duration time.Duration          `json:"duration"`
}

// ChunkExecutor is a function that executes a single chunk.
type ChunkExecutor func(ctx context.Context, stage StageType, chunkID string, params map[string]interface{}) (map[string]interface{}, error)

// StageExecutorConfig configures the stage executor.
type StageExecutorConfig struct {
	MaxConcurrentChunks int
	ChunkTimeout        time.Duration
	MaxChunkRetries     int
	ChunkRetryDelay     time.Duration
	StageTimeout        time.Duration
}

// DefaultStageExecutorConfig returns default configuration.
func DefaultStageExecutorConfig() *StageExecutorConfig {
	return &StageExecutorConfig{
		MaxConcurrentChunks: 8,
		ChunkTimeout:        5 * time.Minute,
		MaxChunkRetries:     2,
		ChunkRetryDelay:     2 * time.Second,
		StageTimeout:        15 * time.Minute,
	}
}

// StageExecutorStats contains executor statistics.
type StageExecutorStats struct {
	ActiveJobs    int32   `json:"active_jobs"`
	TotalChunks   int64   `json:"total_chunks"`
	FailedChunks  int64   `json:"failed_chunks"`
	RetriedChunks int64   `json:"retried_chunks"`
	SuccessRate   float64 `json:"success_rate"`
}

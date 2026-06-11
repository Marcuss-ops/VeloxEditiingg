// Package worker provides stage-aware chunk execution for the GOD workflow.
//
// This implements Step 2 of the GOD Workflow Implementation Playbook:
// - Stage-aware queue with backpressure
// - Chunk-level retry with isolation
// - CPU worker pool limits
// - Uniform timeout and cancellation propagation
package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"velox-worker-agent/pkg/logger"
)

// StageExecutor executes job stages with chunk-level parallelism and retry.
type StageExecutor struct {
	config        *StageExecutorConfig
	executors     map[StageType]ChunkExecutor
	mu            sync.RWMutex
	activeJobs    int32
	totalChunks   int64
	failedChunks  int64
	retriedChunks int64
}

// NewStageExecutor creates a new stage executor.
func NewStageExecutor(config *StageExecutorConfig) *StageExecutor {
	if config == nil {
		config = DefaultStageExecutorConfig()
	}
	return &StageExecutor{
		config:    config,
		executors: make(map[StageType]ChunkExecutor),
	}
}

// RegisterExecutor registers a chunk executor for a specific stage.
func (se *StageExecutor) RegisterExecutor(stage StageType, executor ChunkExecutor) {
	se.mu.Lock()
	defer se.mu.Unlock()
	se.executors[stage] = executor
}

// ExecuteStages executes all stages in order for a job.
func (se *StageExecutor) ExecuteStages(ctx context.Context, jobID string, params map[string]interface{}) (map[string]interface{}, error) {
	atomic.AddInt32(&se.activeJobs, 1)
	defer atomic.AddInt32(&se.activeJobs, -1)

	combinedOutput := make(map[string]interface{})

	for _, stage := range StageOrder {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("job %s cancelled before stage %s: %w", jobID, stage, ctx.Err())
		}
		logger.Info("[STAGE_EXECUTOR] Job %s: starting stage %s", jobID, stage)

		stageResult, err := se.executeStage(ctx, jobID, stage, params, combinedOutput)
		if err != nil {
			logger.Error("[STAGE_EXECUTOR] Job %s: stage %s failed: %v", jobID, stage, err)
			return nil, fmt.Errorf("stage %s failed: %w", stage, err)
		}

		for k, v := range stageResult.Output {
			prefixedKey := fmt.Sprintf("%s_%s", stage, k)
			combinedOutput[prefixedKey] = v
			if _, exists := combinedOutput[k]; !exists {
				combinedOutput[k] = v
			}
		}
		logger.Info("[STAGE_EXECUTOR] Job %s: stage %s completed in %v", jobID, stage, stageResult.Duration)
	}
	return combinedOutput, nil
}

// executeStage executes a single stage with chunk-level parallelism.
func (se *StageExecutor) executeStage(ctx context.Context, jobID string, stage StageType, params map[string]interface{}, prevOutput map[string]interface{}) (*StageResult, error) {
	stageStart := time.Now()
	stageCtx, stageCancel := context.WithTimeout(ctx, se.config.StageTimeout)
	defer stageCancel()

	se.mu.RLock()
	executor, ok := se.executors[stage]
	se.mu.RUnlock()

	if !ok {
		logger.Debug("[STAGE_EXECUTOR] Job %s: no executor for stage %s, skipping", jobID, stage)
		return &StageResult{Stage: stage, Success: true, Chunks: []ChunkResult{}, Output: make(map[string]interface{}), Duration: time.Since(stageStart)}, nil
	}

	chunks := se.generateChunks(jobID, stage, params, prevOutput)
	if len(chunks) == 0 {
		logger.Debug("[STAGE_EXECUTOR] Job %s: no chunks for stage %s, skipping", jobID, stage)
		return &StageResult{Stage: stage, Success: true, Chunks: []ChunkResult{}, Output: make(map[string]interface{}), Duration: time.Since(stageStart)}, nil
	}

	results := se.executeChunksParallel(stageCtx, jobID, stage, chunks, executor)

	stageResult := &StageResult{Stage: stage, Success: true, Chunks: results, Output: make(map[string]interface{}), Duration: time.Since(stageStart)}
	for _, chunkResult := range results {
		if !chunkResult.Success {
			stageResult.Success = false
			stageResult.Error = chunkResult.Error
			break
		}
		for k, v := range chunkResult.Output {
			stageResult.Output[k] = v
		}
	}
	return stageResult, nil
}

// generateChunks generates chunk definitions for a stage.
func (se *StageExecutor) generateChunks(jobID string, stage StageType, params map[string]interface{}, prevOutput map[string]interface{}) []map[string]interface{} {
	chunk := map[string]interface{}{
		"chunk_id": fmt.Sprintf("%s-%s-0", jobID, stage), "job_id": jobID,
		"stage": string(stage), "params": params, "prev_output": prevOutput,
	}
	return []map[string]interface{}{chunk}
}

// executeChunksParallel executes chunks with bounded concurrency and retry.
func (se *StageExecutor) executeChunksParallel(ctx context.Context, jobID string, stage StageType, chunks []map[string]interface{}, executor ChunkExecutor) []ChunkResult {
	results := make([]ChunkResult, len(chunks))
	semaphore := make(chan struct{}, se.config.MaxConcurrentChunks)
	var wg sync.WaitGroup

	for i, chunk := range chunks {
		wg.Add(1)
		go func(idx int, chunkDef map[string]interface{}) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			chunkID, _ := chunkDef["chunk_id"].(string)
			params, _ := chunkDef["params"].(map[string]interface{})
			result := se.executeChunkWithRetry(ctx, jobID, stage, chunkID, params, executor)
			results[idx] = result

			atomic.AddInt64(&se.totalChunks, 1)
			if !result.Success {
				atomic.AddInt64(&se.failedChunks, 1)
			}
			if result.Attempt > 1 {
				atomic.AddInt64(&se.retriedChunks, 1)
			}
		}(i, chunk)
	}
	wg.Wait()
	return results
}

// executeChunkWithRetry executes a single chunk with retry logic.
func (se *StageExecutor) executeChunkWithRetry(ctx context.Context, jobID string, stage StageType, chunkID string, params map[string]interface{}, executor ChunkExecutor) ChunkResult {
	var lastErr error

	for attempt := 1; attempt <= se.config.MaxChunkRetries+1; attempt++ {
		if ctx.Err() != nil {
			return ChunkResult{Stage: stage, ChunkID: chunkID, Success: false, Error: fmt.Sprintf("context cancelled: %v", ctx.Err()), Attempt: attempt}
		}
		chunkCtx, chunkCancel := context.WithTimeout(ctx, se.config.ChunkTimeout)
		startTime := time.Now()
		output, err := executor(chunkCtx, stage, chunkID, params)
		duration := time.Since(startTime)
		chunkCancel()

		if err == nil {
			return ChunkResult{Stage: stage, ChunkID: chunkID, Success: true, Output: output, Duration: duration, Attempt: attempt}
		}

		lastErr = err
		logger.Warn("[STAGE_EXECUTOR] Job %s: chunk %s attempt %d/%d failed: %v",
			jobID, chunkID, attempt, se.config.MaxChunkRetries+1, err)

		if attempt <= se.config.MaxChunkRetries {
			backoff := time.Duration(attempt) * se.config.ChunkRetryDelay
			select {
			case <-ctx.Done():
				return ChunkResult{Stage: stage, ChunkID: chunkID, Success: false, Error: fmt.Sprintf("context cancelled during retry backoff: %v", ctx.Err()), Attempt: attempt}
			case <-time.After(backoff):
			}
		}
	}
	return ChunkResult{Stage: stage, ChunkID: chunkID, Success: false, Error: fmt.Sprintf("max retries exceeded: %v", lastErr), Attempt: se.config.MaxChunkRetries + 1}
}

// Stats returns current executor statistics.
func (se *StageExecutor) Stats() StageExecutorStats {
	total := atomic.LoadInt64(&se.totalChunks)
	failed := atomic.LoadInt64(&se.failedChunks)
	successRate := float64(0)
	if total > 0 {
		successRate = float64(total-failed) / float64(total) * 100
	}
	return StageExecutorStats{
		ActiveJobs:    atomic.LoadInt32(&se.activeJobs),
		TotalChunks:   total,
		FailedChunks:  failed,
		RetriedChunks: atomic.LoadInt64(&se.retriedChunks),
		SuccessRate:   successRate,
	}
}

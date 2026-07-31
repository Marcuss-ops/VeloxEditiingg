package stageexec

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"velox-worker-agent/internal/telemetry"
)

func TestExecuteChunkWithRetryRecordsFailedAndSuccessfulAttempts(t *testing.T) {
	config := &StageExecutorConfig{
		MaxConcurrentChunks: 1,
		ChunkTimeout:        time.Second,
		MaxChunkRetries:     1,
		ChunkRetryDelay:     0,
		StageTimeout:        time.Second,
	}
	se := NewStageExecutor(config)
	rec := telemetry.NewEventRecorder()
	ctx := telemetry.WithRecorder(context.Background(), rec)
	var calls atomic.Int32

	result := se.executeChunkWithRetry(ctx, "job-1", StageVideo, "chunk-1", nil,
		func(context.Context, StageType, string, map[string]interface{}) (map[string]interface{}, error) {
			if calls.Add(1) == 1 {
				return nil, errors.New("transient")
			}
			return map[string]interface{}{"ok": true}, nil
		})

	if !result.Success || result.Attempt != 2 {
		t.Fatalf("result = %+v, want successful second attempt", result)
	}
	events := rec.Flush()
	if len(events) != 1 {
		t.Fatalf("retry events = %d, want 1 (only the second attempt is a retry)", len(events))
	}
	if events[0].Status != telemetry.StatusOK {
		t.Fatalf("retry event = %+v, want successful retry", events[0])
	}
	if events[0].EventIndex != 0 {
		t.Fatalf("retry event index = %d, want 0", events[0].EventIndex)
	}
}

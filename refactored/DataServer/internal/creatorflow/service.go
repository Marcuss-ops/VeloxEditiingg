package creatorflow

import (
	"context"
	"log"
	"strings"
	"time"

	"velox-server/internal/config"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/queue"
	"velox-server/internal/remoteengine"
)

// Service encapsulates the optional "creator" stage so multiple endpoints can
// reuse the same remote-engine -> worker handoff path without duplicating it.
type Service struct {
	queue        *queue.FileQueue
	client       *remoteengine.Client
	pollInterval time.Duration
}

// New creates a creator-flow service from runtime config.
func New(cfg *config.Config, q *queue.FileQueue) *Service {
	if cfg == nil || q == nil {
		return nil
	}
	if strings.TrimSpace(cfg.RemoteEngineURL) == "" {
		return nil
	}

	return &Service{
		queue: q,
		client: remoteengine.NewClient(remoteengine.Config{
			URL:       cfg.RemoteEngineURL,
			Token:     cfg.RemoteEngineToken,
			TimeoutMS: cfg.RemoteEngineTimeoutMS,
			Retries:   cfg.RemoteEngineRetries,
		}),
		pollInterval: time.Duration(max(cfg.RemoteEnginePollInterval, 5)) * time.Second,
	}
}

// Forward tries the creator stage and, if it returns a complete payload,
// forwards the resulting job to the worker queue.
//
// It returns:
// - response: queue response enriched with creator metadata
// - used: true only when the creator stage fully handled the request
// - error: fatal creator/queue errors that should surface to callers
func (s *Service) Forward(ctx context.Context, rawPayload map[string]interface{}) (map[string]interface{}, bool, error) {
	if s == nil || s.client == nil || !s.client.IsConfigured() {
		return nil, false, nil
	}

	creatorResult, err := s.client.StartPipeline(ctx, rawPayload)
	if err != nil {
		return nil, false, err
	}

	if enqueue.ShouldForwardPipelineResult(creatorResult) {
		workerResponse, err := ForwardCompletedResult(ctx, s.queue, creatorResult)
		if err != nil {
			return nil, false, err
		}

		response := make(map[string]interface{}, len(workerResponse)+4)
		for k, v := range workerResponse {
			response[k] = v
		}
		response["creator_stage"] = "remote_engine"
		response["creator_job_id"] = firstString(creatorResult, "job_id", "trace_id", "id")
		response["creator_status"] = creatorResult["status"]
		response["creator_response"] = creatorResult

		return response, true, nil
	}

	creatorJobID := firstString(creatorResult, "job_id", "trace_id", "id")
	if creatorJobID == "" {
		log.Printf("[CREATOR] remote result incomplete and missing job id, keeping local fallback")
		return nil, false, nil
	}

	s.scheduleCreatorPolling(creatorJobID)
	return map[string]interface{}{
		"ok":             true,
		"creator_stage":  "remote_engine",
		"creator_job_id": creatorJobID,
		"creator_status": creatorResult["status"],
		"creator_polling": true,
		"creator_response": creatorResult,
	}, true, nil
}

func firstString(m map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if v, ok := m[key]; ok {
			if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
				return strings.TrimSpace(s)
			}
		}
	}
	return ""
}

// ForwardCompletedResult converts a completed creator payload into a worker job
// and enqueues it for the remote worker pool.
func ForwardCompletedResult(ctx context.Context, q *queue.FileQueue, result map[string]interface{}) (map[string]interface{}, error) {
	if q == nil {
		return nil, nil
	}
	if !enqueue.ShouldForwardPipelineResult(result) {
		return nil, nil
	}

	workerPayload, err := enqueue.BuildPipelinePayload(result)
	if err != nil {
		return nil, err
	}

	return enqueue.EnqueueSceneVideoJob(ctx, q, workerPayload)
}

func (s *Service) scheduleCreatorPolling(creatorJobID string) {
	if s == nil || s.client == nil || s.queue == nil {
		return
	}
	interval := s.pollInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for i := 0; i < 120; i++ {
			<-ticker.C
			status, err := s.client.GetPipelineStatus(context.Background(), creatorJobID)
			if err != nil {
				log.Printf("[CREATOR] poll failed job_id=%s attempt=%d: %v", creatorJobID, i+1, err)
				continue
			}
			if !isTerminalStatus(status.Status) {
				continue
			}
			if status.Status == "completed" || status.Status == "succeeded" || status.Status == "done" {
				result := map[string]interface{}{
					"ok":       true,
					"status":   status.Status,
					"trace_id": creatorJobID,
					"result":   status.Result,
				}
				if forwarded, err := ForwardCompletedResult(context.Background(), s.queue, result); err != nil {
					log.Printf("[CREATOR] forward after poll failed job_id=%s: %v", creatorJobID, err)
				} else {
					log.Printf("[CREATOR] forward after poll succeeded job_id=%s worker_job_id=%v", creatorJobID, forwarded["job_id"])
				}
			}
			return
		}
		log.Printf("[CREATOR] poll timeout job_id=%s", creatorJobID)
	}()
}

func isTerminalStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "completed", "succeeded", "done", "failed", "error":
		return true
	default:
		return false
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

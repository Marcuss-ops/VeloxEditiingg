package creatorflow

import (
	"context"
	"log"
	"strings"

	"velox-server/internal/config"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/queue"
	"velox-server/internal/remoteengine"
)

// Service encapsulates the optional "creator" stage so multiple endpoints can
// reuse the same remote-engine -> worker handoff path without duplicating it.
type Service struct {
	queue  *queue.FileQueue
	client *remoteengine.Client
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

	if !enqueue.ShouldForwardPipelineResult(creatorResult) {
		log.Printf("[CREATOR] remote result incomplete, keeping local fallback")
		return nil, false, nil
	}

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

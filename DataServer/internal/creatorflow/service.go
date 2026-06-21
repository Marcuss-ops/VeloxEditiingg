package creatorflow

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"strings"
	"time"

	"velox-server/internal/config"
	"velox-server/internal/costmodel"
	remoteansible "velox-server/internal/handlers/remote/ansible"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/remoteengine"
)

// Service encapsulates the optional "creator" stage so multiple endpoints can
// reuse the same remote-engine -> worker handoff path without duplicating it.
//
// PR15.7a: `queue` was removed. The *enqueue.Enqueuer
// holds the JobQueue reference.
// at the composition root if they need the concrete type. This collapses
// two parallel fields that always pointed to the same underlying queue.
type Service struct {
	enqueuer *enqueue.Enqueuer // PR15.7a: drops package-level voiceover global AND the q field; both rewrite + queue live here.
	client   *remoteengine.Client
	pollInterval time.Duration
	dataDir      string
	videosDir    string
	masterURL    string
}

// New creates a creator-flow service from runtime config.
// enqueuer is mandatory (PR15.7a): it owns the voiceover rewrite and the
// The concrete type is no longer needed here —
// callers can construct the Enqueuer (which embeds the JobQueue) once
// at composition-root time and pass it down.
func New(cfg *config.Config, enqueuer *enqueue.Enqueuer) *Service {
	if cfg == nil || enqueuer == nil {
		return nil
	}
	if strings.TrimSpace(cfg.Render.RemoteEngineURL) == "" {
		return nil
	}

	return &Service{
		enqueuer: enqueuer,
		client: remoteengine.NewClient(remoteengine.Config{
			URL:       cfg.Render.RemoteEngineURL,
			Token:     cfg.Render.RemoteEngineToken,
			TimeoutMS: cfg.Render.RemoteEngineTimeoutMS,
			Retries:   cfg.Render.RemoteEngineRetries,
		}),
		pollInterval: time.Duration(max(cfg.Render.RemoteEnginePollInterval, 5)) * time.Second,
		dataDir:      strings.TrimSpace(cfg.Runtime.DataDir),
		videosDir:    strings.TrimSpace(cfg.Runtime.VideosDir),
		masterURL:    resolvePublicMasterURL(cfg),
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
		workerResponse, err := s.forwardCompletedResult(ctx, creatorResult)
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
		"ok":               true,
		"creator_stage":    "remote_engine",
		"creator_job_id":   creatorJobID,
		"creator_status":   creatorResult["status"],
		"creator_polling":  true,
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
//
// PR15.7a: takes *enqueue.Enqueuer (which owns voiceover rewrite + queue),
// not a raw queue + voiceover global. The caller must construct
// the enqueuer once at composition-root time and pass it through.
func ForwardCompletedResult(ctx context.Context, enqueuer *enqueue.Enqueuer, result map[string]interface{}) (map[string]interface{}, error) {
	if enqueuer == nil || enqueuer.Queue == nil {
		return nil, fmt.Errorf("queue unavailable")
	}
	if !enqueue.ShouldForwardPipelineResult(result) {
		return nil, nil
	}

	workerPayload, err := enqueue.BuildPipelinePayload(result)
	if err != nil {
		return nil, err
	}

	// PR-04.5: legacy creator-flow callers do not publish per-job
	// JobRequirements — pass the permissive default so today's FIFO
	// queue routing is preserved. Future slices that decide on
	// concrete requirements can plumb them through here.
	return enqueuer.Enqueue(ctx, workerPayload, costmodel.DefaultRequirements())
}

func (s *Service) forwardCompletedResult(ctx context.Context, result map[string]interface{}) (map[string]interface{}, error) {
	if s == nil {
		return nil, fmt.Errorf("service unavailable")
	}
	if s.enqueuer == nil || s.enqueuer.Queue == nil {
		return nil, fmt.Errorf("queue unavailable")
	}
	if !enqueue.ShouldForwardPipelineResult(result) {
		return nil, nil
	}

	workerPayload, err := enqueue.BuildPipelinePayload(result)
	if err != nil {
		return nil, err
	}

	masterURL := strings.TrimSpace(s.masterURL)
	if masterURL == "" || remoteansible.IsLocalhostURL(masterURL) {
		masterURL = detectPublicMasterURL()
	}
	if s.dataDir != "" && masterURL != "" {
		workerPayload, err = enqueue.BuildSceneImagePayloadForMaster(workerPayload, s.dataDir, s.videosDir, masterURL)
		if err != nil {
			return nil, err
		}
	}

	// PR-04.5: see ForwardCompletedResult comment above.
	return s.enqueuer.Enqueue(ctx, workerPayload, costmodel.DefaultRequirements())
}

func (s *Service) scheduleCreatorPolling(creatorJobID string) {
	if s == nil || s.client == nil || s.enqueuer == nil {
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
				if forwarded, err := s.forwardCompletedResult(context.Background(), result); err != nil {
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

func resolvePublicMasterURL(cfg *config.Config) string {
	if cfg != nil {
		if v := strings.TrimSpace(cfg.Workers.MasterURL); v != "" {
			return v
		}
	}
	if v := strings.TrimSpace(config.GetMasterURL()); v != "" {
		return v
	}
	return detectPublicMasterURL()
}

func detectPublicMasterURL() string {
	out, err := exec.Command("hostname", "-I").Output()
	if err == nil {
		fields := strings.Fields(string(out))
		if len(fields) > 0 {
			ip := strings.TrimSpace(fields[0])
			if ip != "" && !remoteansible.IsLocalhostURL(ip) {
				return "http://" + ip + ":8000"
			}
		}
	}
	return remoteansible.DetectLocalMasterURL()
}

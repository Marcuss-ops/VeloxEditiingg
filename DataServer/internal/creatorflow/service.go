package creatorflow

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"

	"velox-server/internal/config"
	"velox-server/internal/forwardingcontract"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/remoteengine"
	"velox-server/internal/routing"
	"velox-shared/contract/deliveryplan"
)

// Service encapsulates the optional "creator" stage so multiple endpoints can
// reuse the same remote-engine -> worker handoff path without duplicating it.
//
// Blocco 4 step #3 collapsed the public surface to its minimum:
//
//   - New(cfg, enqueuer, forwardRepo, driveResolver) constructs the
//     optional creator stage.
//   - StartOrPersistForwarding runs the remote creator exactly once and
//     routes the result through Resolver.Resolve (sync forward) OR
//     persists a creator_forwardings row (async poll).
//
// The legacy forwarder shim (NewForwarder, Service.ForwardCompleted,
// Service.resolver, forwardCompletedForwarderOnly) is gone. Every
// forward-completed path converges on Resolver.Resolve; the composition
// root (cmd/server/bootstrap_composition.go) builds the Resolver shared
// by the pipeline handler, the script handler, and the
// CreatorForwardingRunner.
//
// The public REST control-plane endpoint is required by the production
// bootstrap. Resolver URL rewriting uses the typed endpoint snapshot and
// never discovers or derives a URL at runtime.
//
// The PR-operation 01 / Fase 2 Job+Task creation surface (RenderPlan,
// CreateJobWithPlan, deriveJobID) lives in the sibling file service_job.go.
type Service struct {
	enqueuer      *enqueue.Enqueuer
	client        *remoteengine.Client
	forwardRepo   ForwardingRepository
	driveResolver enqueue.DriveFolderResolver
	dataDir       string
	videosDir     string
	masterURL     string
}

// New creates a creator-flow service from runtime config.
// enqueuer is mandatory (PR15.7a): it owns the voiceover rewrite.
// forwardRepo is mandatory (PR-forwarding-runner): used to persist
// PENDING creator_forwardings rows for durable polling. driveResolver is
// the Drive master-folder resolver threaded into the one-shot Resolver;
// the two ports are separate because the forwarding extraction moved the
// creator_forwardings SQL/CAS into forwardingstore while Drive resolution
// stayed on store.SQLiteStore.
func New(cfg *config.Config, enqueuer *enqueue.Enqueuer, forwardRepo ForwardingRepository, driveResolver enqueue.DriveFolderResolver) *Service {
	if cfg == nil || enqueuer == nil || forwardRepo == nil {
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
		forwardRepo:   forwardRepo,
		driveResolver: driveResolver,
		dataDir:       strings.TrimSpace(cfg.Runtime.DataDir),
		videosDir:     strings.TrimSpace(cfg.Runtime.VideosDir),
		masterURL:     string(cfg.ControlPlane.RESTPublic),
	}
}

// StartOrPersistForwarding runs the remote creator stage exactly once
// and routes the result through the canonical Resolver pipeline.
//
// Two branches:
//
//   - Remote result complete (enqueue.ShouldForwardPipelineResult):
//     inject the deterministic forwarding key, build a one-shot Resolver
//     from this Service's fields, call Resolve, wrap the response with
//     the creator envelope (stage/job_id/status/creator_response),
//     return (response, true, nil).
//
//   - Remote result incomplete but with job id (async/polling):
//     persist a PENDING creator_forwardings row (durable; the
//     CreatorForwardingRunner picks it up), return a polling-shaped
//     response with creator_polling=true.
//
// Returns (nil, false, nil) when the creator is not configured or the
// result is incomplete without a job id — the caller takes the
// local-fallback path.
func (s *Service) StartOrPersistForwarding(ctx context.Context, rawPayload map[string]interface{}) (map[string]interface{}, bool, error) {
	if s == nil || s.client == nil || !s.client.IsConfigured() {
		return nil, false, nil
	}

	// Creatorflow legacy path: extract the idempotency key from the
	// payload if present, so a timeout after remote job creation does
	// not produce a duplicate on retry.
	idemKey, _ := rawPayload["idempotency_key"].(string)
	creatorResult, err := s.client.StartPipeline(ctx, rawPayload, idemKey)
	if err != nil {
		return nil, false, err
	}

	if enqueue.ShouldForwardPipelineResult(creatorResult) {
		// Area 2: Parse the raw result into the typed DTO and derive
		// the worker payload. The remote result must NOT be passed
		// raw to the worker.
		dto, parseErr := remoteengine.ParseRemotePipelineResult(creatorResult)
		if parseErr != nil {
			return nil, false, fmt.Errorf("creatorflow: parse remote result: %w", parseErr)
		}
		workerPayload, payloadErr := dto.ToWorkerPayloadChecked()
		if payloadErr != nil {
			return nil, false, fmt.Errorf("creatorflow: project worker payload: %w", payloadErr)
		}

		// PR-forwarding-deterministic-id: stamp the forwarding key into
		// the payload so Resolver.Resolve derives the canonical job_id
		// (and the UNIQUE constraint on creator_forwardings converges
		// on one row across retries).
		sourceJobID := firstString(workerPayload, "job_id", "trace_id", "id")
		targetExecID := firstString(workerPayload, "executor_id", "pipeline_id")
		if targetExecID == "" {
			targetExecID = "scene.composite.v1"
		}
		fwdKey := routing.FormatForwardingKey("remote_engine", sourceJobID, targetExecID).String()
		workerPayload[routing.KeyForwardingKey] = fwdKey

		// Build a one-shot Resolver from this Service's wiring graph and
		// delegate. Resolver.Resolve owns idempotency pre-check,
		// (optionally) URL rewrite via BuildSceneImagePayloadForMaster,
		// creator_forwardings row promotion, and the atomic
		// AtomicForwardAndEnqueue that finalises the Job row.
		rs := NewResolverFromDeps(s.enqueuer, s.forwardRepo, s.driveResolver, s.dataDir, s.videosDir, s.masterURL)
		if rs == nil {
			return nil, false, fmt.Errorf("creatorflow: StartOrPersistForwarding: resolver construction failed")
		}
		// Remote creator responses may wrap the completed payload under
		// `result`; flatten before extracting the control-plane envelope so
		// delivery_plan is not lost on the sync forwarding path.
		deliveryPlan := deliveryplan.ExtractEnvelope(enqueue.FlattenPipelineResult(creatorResult))
		out, err := rs.Resolve(ctx, ResolveRequest{
			ForwardingID:     "",
			SourceProvider:   "remote_engine",
			SourceJobID:      sourceJobID,
			TargetExecutorID: targetExecID,
			Payload:          workerPayload,
			DeliveryPlan:     deliveryPlan,
		})
		if err != nil && !errors.Is(err, ErrResolverNotComplete) {
			return nil, false, err
		}

		var workerResponse map[string]interface{}
		if out != nil {
			workerResponse = out.Response
		}

		response := make(map[string]interface{}, len(workerResponse)+4)
		for k, v := range workerResponse {
			response[k] = v
		}
		response["creator_stage"] = "remote_engine"
		response["creator_job_id"] = sourceJobID
		response["creator_status"] = creatorResult["status"]
		response["creator_response"] = creatorResult

		return response, true, nil
	}

	creatorJobID := firstString(creatorResult, "job_id", "trace_id", "id")
	if creatorJobID == "" {
		log.Printf("[CREATOR] remote result incomplete and missing job id, keeping local fallback")
		return nil, false, nil
	}

	// Defense-in-depth: pre-Blocco-4 step #3 the deleted
	// forwardCompletedForwarderOnly shim doubled as a nil-dbStore guard.
	// With the shim gone, a literal `&Service{forwardRepo: nil}{}`
	// construction (e.g. a future unit test) would panic on the
	// InsertCreatorForwarding call. Reject that case loudly with a typed
	// error so the caller sees the cause. Unreachable from `creatorflow.New`
	// (which returns nil when forwardRepo is nil).
	if s.forwardRepo == nil {
		return nil, false, fmt.Errorf("creatorflow: StartOrPersistForwarding: nil forwardRepo (required for durable forwarding row)")
	}

	// PR-forwarding-runner: persist a durable forwarding record instead of
	// spawning a volatile goroutine. The CreatorForwardingRunner picks up
	// PENDING rows on its next tick and handles polling + forwarding.
	targetExecutorID := firstString(creatorResult, "executor_id", "pipeline_id")
	if targetExecutorID == "" {
		targetExecutorID = "scene.composite.v1"
	}
	forwardingID := "cf_" + uuid.NewString()
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := s.forwardRepo.InsertCreatorForwarding(ctx, &forwardingcontract.CreatorForwarding{
		ForwardingID:     forwardingID,
		SourceProvider:   "remote_engine",
		SourceJobID:      creatorJobID,
		TargetExecutorID: targetExecutorID,
		Status:           string(forwardingcontract.CFStatusPending),
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		log.Printf("[CREATOR] failed to insert forwarding row for job_id=%s: %v", creatorJobID, err)
		return nil, false, fmt.Errorf("insert creator forwarding: %w", err)
	}

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

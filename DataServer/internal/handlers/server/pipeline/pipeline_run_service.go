// Package pipeline — pipeline-run creation service.
//
// pipeline_run_service.go owns the PipelineRunService boundary: the thin HTTP
// handler in pipeline_create.go only binds the JSON body and delegates here.
// The service performs validation, durable pipeline_run creation, remote-engine
// submission and forwarding persistence, returning an HTTP status + body that
// the handler writes verbatim.
package pipeline

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"velox-server/internal/creatorflow"
	"velox-server/internal/pipelineruns"
	"velox-server/internal/remoteengine"
	"velox-server/internal/store"
)

// PipelineRunService owns the pipeline-run creation workflow. The handler
// knows only this interface; the concrete implementation is built by the
// composition root and injected into Handlers.
type PipelineRunService interface {
	Create(ctx context.Context, clientID string, req CreatePipelineRunRequest) CreatePipelineRunResponse
}

// CreatePipelineRunResponse is the transport-neutral outcome of a create:
// the HTTP status code and the JSON body the handler writes verbatim.
type CreatePipelineRunResponse struct {
	Status int
	Body   gin.H
}

type pipelineRunService struct {
	store    *store.SQLiteStore
	client   *remoteengine.Client
	resolver *creatorflow.Resolver
}

func newPipelineRunService(db *store.SQLiteStore, client *remoteengine.Client, resolver *creatorflow.Resolver) PipelineRunService {
	return &pipelineRunService{store: db, client: client, resolver: resolver}
}

// Create implements the POST /api/v1/pipeline-runs contract:
//
//  1. idempotency_key is required. Two requests with the same key MUST
//     return the same pipeline_run_id without starting a second remote
//     generation.
//  2. A pipeline_run row is created BEFORE the remote call is made, so
//     the resource exists durably even if the remote engine is slow or
//     the connection drops.
//  3. On success the handler returns HTTP 202 Accepted with a
//     status_url the client can poll.
//
// The remote-engine call delegates to client.StartPipeline and the durable
// forwarding to resolver.PersistPendingRemoteForwarding, converging on the
// same creator_forwardings row the CreatorForwardingRunner picks up.
func (s *pipelineRunService) Create(ctx context.Context, clientID string, req CreatePipelineRunRequest) CreatePipelineRunResponse {
	if s.store == nil {
		return CreatePipelineRunResponse{Status: http.StatusServiceUnavailable, Body: gin.H{
			"ok":    false,
			"error": "pipeline store not wired",
		}}
	}

	req.IdempotencyKey = strings.TrimSpace(req.IdempotencyKey)
	if req.IdempotencyKey == "" {
		return CreatePipelineRunResponse{Status: http.StatusBadRequest, Body: gin.H{
			"ok":    false,
			"error": "idempotency_key is required",
			"code":  "REQUIRED",
			"field": "idempotency_key",
		}}
	}

	// All business-rule checks run here so a rejected request never creates a
	// pipeline_run row. The validator returns a structured *ValidationError
	// with field/code/message for a 400 response.
	if valErr := ValidateCreateRequest(ctx, s.store, &req, DefaultValidationConfig()); valErr != nil {
		pipelineLog("CREATE: validation FAILED idem=%s field=%s code=%s: %s",
			req.IdempotencyKey, valErr.Field, valErr.Code, valErr.Message)
		return CreatePipelineRunResponse{Status: http.StatusBadRequest, Body: gin.H{
			"ok":    false,
			"error": valErr.Message,
			"code":  valErr.Code,
			"field": valErr.Field,
		}}
	}

	requestedJSON, _ := json.Marshal(req)
	if requestedJSON == nil {
		requestedJSON = []byte("{}")
	}

	// A fresh UUID-shaped id. The idempotency_key UNIQUE index is the
	// authoritative dedup; INSERT OR IGNORE + lookup ensures concurrent or
	// retried requests converge on the same row.
	runID := "run_" + uuid.NewString()
	requestID := "req_" + uuid.NewString()
	now := time.Now().UTC()

	insertResult, err := s.store.InsertPipelineRun(ctx, &pipelineruns.PipelineRun{
		ID:                   runID,
		RequestID:            requestID,
		IdempotencyKey:       req.IdempotencyKey,
		UserID:               req.UserID,
		CampaignID:           req.CampaignID,
		CampaignItemID:       req.CampaignItemID,
		Status:               pipelineruns.StatusAccepted,
		RequestedPayloadJSON: string(requestedJSON),
		CreatedAt:            now,
		UpdatedAt:            now,
	})
	if err != nil {
		pipelineLog("CREATE: failed to insert pipeline_run idem=%s: %v", req.IdempotencyKey, err)
		return CreatePipelineRunResponse{Status: http.StatusInternalServerError, Body: gin.H{
			"ok":    false,
			"error": "failed to create pipeline run",
		}}
	}

	// Idempotent duplicate: return the existing row without starting a second
	// remote generation.
	if !insertResult.Created {
		existing := insertResult.Run
		pipelineLog("CREATE: idempotent duplicate idem=%s → run=%s (no new remote call)",
			req.IdempotencyKey, existing.ID)
		return CreatePipelineRunResponse{Status: http.StatusAccepted, Body: buildCreateResponse(existing, true)}
	}

	pr := insertResult.Run
	pipelineLog("CREATE: created pipeline_run id=%s idem=%s", pr.ID, req.IdempotencyKey)

	if s.client == nil || !s.client.IsConfigured() {
		// Mark the run as FAILED — the remote engine is required.
		if err := s.store.UpdatePipelineRunError(ctx, pr.ID,
			"REMOTE_UNCONFIGURED", "remote engine not configured", "REMOTE_SUBMITTING"); err != nil {
			pipelineLog("CREATE: failed to mark REMOTE_UNCONFIGURED run=%s: %v", pr.ID, err)
		}
		return CreatePipelineRunResponse{Status: http.StatusServiceUnavailable, Body: gin.H{
			"ok":              false,
			"pipeline_run_id": pr.ID,
			"request_id":      pr.RequestID,
			"status":          string(pipelineruns.StatusFailed),
			"error":           "remote engine not configured",
			"hint":            "set VELOX_REMOTE_ENGINE_URL",
			"status_url":      "/api/v1/pipeline-runs/" + pr.ID,
		}}
	}

	if err := s.store.UpdatePipelineRunStatus(ctx, pr.ID,
		pipelineruns.StatusRemoteSubmitting, "submitting to remote engine"); err != nil {
		pipelineLog("CREATE: failed to transition to REMOTE_SUBMITTING run=%s: %v", pr.ID, err)
	}

	remotePayload := buildRemotePayload(&req)

	result, err := s.client.StartPipeline(ctx, remotePayload, pr.ID)
	if err != nil {
		pipelineLog("CREATE: remote call FAILED run=%s: %v", pr.ID, err)
		if markErr := s.store.UpdatePipelineRunError(ctx, pr.ID,
			"REMOTE_CALL_FAILED", err.Error(), "REMOTE_SUBMITTING"); markErr != nil {
			pipelineLog("CREATE: failed to mark REMOTE_CALL_FAILED run=%s: %v", pr.ID, markErr)
		}
		return CreatePipelineRunResponse{Status: http.StatusBadGateway, Body: gin.H{
			"ok":              false,
			"pipeline_run_id": pr.ID,
			"request_id":      pr.RequestID,
			"status":          string(pipelineruns.StatusFailed),
			"error":           err.Error(),
			"status_url":      "/api/v1/pipeline-runs/" + pr.ID,
		}}
	}

	// Parse the raw result into the typed DTO and derive the worker payload.
	// The remote result must NOT be passed raw to the worker — it must first
	// be converted to a typed RemotePipelineResult.
	dto, parseErr := remoteengine.ParseRemotePipelineResult(result)
	if parseErr != nil {
		return CreatePipelineRunResponse{Status: http.StatusBadGateway, Body: gin.H{"ok": false, "error": parseErr.Error()}}
	}
	workerPayload, projectionErr := dto.ToWorkerPayloadChecked()
	if projectionErr != nil {
		return CreatePipelineRunResponse{Status: http.StatusBadGateway, Body: gin.H{"ok": false, "error": projectionErr.Error()}}
	}

	jobID := firstStringResolver(workerPayload, "job_id", "trace_id", "id")
	status := firstStringResolver(result, "status")
	if jobID != "" {
		pipelineLog("CREATE: remote response run=%s job_id=%s status=%s", pr.ID, jobID, status)
	}

	if jobID != "" {
		pr.RemoteJobID = jobID
		pr.RemoteProvider = "remote_engine"
		if err := s.store.UpdatePipelineRunRemoteJob(ctx, pr.ID,
			"remote_engine", jobID); err != nil {
			pipelineLog("CREATE: failed to stamp remote_job_id run=%s: %v", pr.ID, err)
		}
	}

	// Async path: persist a PENDING forwarding row. The HTTP layer never polls
	// or forwards synchronously — the CreatorForwardingRunner claims the
	// PENDING row, polls the remote engine, and delegates the forward-completed
	// step to creatorflow.Resolver.Resolve.
	if jobID != "" {
		if s.resolver == nil || !s.resolver.HasDBAccess() {
			pipelineLog("CREATE: durable resolver unavailable run=%s job=%s", pr.ID, jobID)
			if markErr := s.store.UpdatePipelineRunError(ctx, pr.ID,
				"RESOLVER_UNAVAILABLE", "durable forwarding is not configured", "FORWARDING"); markErr != nil {
				pipelineLog("CREATE: failed to mark RESOLVER_UNAVAILABLE run=%s: %v", pr.ID, markErr)
			}
			return CreatePipelineRunResponse{Status: http.StatusServiceUnavailable, Body: gin.H{
				"ok":              false,
				"pipeline_run_id": pr.ID,
				"request_id":      pr.RequestID,
				"status":          string(pipelineruns.StatusFailed),
				"error":           "durable forwarding is not configured",
				"status_url":      "/api/v1/pipeline-runs/" + pr.ID,
			}}
		}

		targetExecutor := firstStringResolver(workerPayload, "executor_id", "pipeline_id")
		forwarding, persistErr := s.resolver.PersistPendingRemoteForwarding(
			ctx, "remote_engine", jobID, targetExecutor, clientID,
		)
		if persistErr != nil {
			pipelineLog("CREATE: failed to persist forwarding run=%s job=%s: %v",
				pr.ID, jobID, persistErr)
			if markErr := s.store.UpdatePipelineRunError(ctx, pr.ID,
				"FORWARDING_PERSIST_FAILED", persistErr.Error(), "FORWARDING"); markErr != nil {
				pipelineLog("CREATE: failed to mark FORWARDING_PERSIST_FAILED run=%s: %v", pr.ID, markErr)
			}
			return CreatePipelineRunResponse{Status: http.StatusInternalServerError, Body: gin.H{
				"ok":              false,
				"pipeline_run_id": pr.ID,
				"request_id":      pr.RequestID,
				"status":          string(pipelineruns.StatusFailed),
				"error":           persistErr.Error(),
				"status_url":      "/api/v1/pipeline-runs/" + pr.ID,
			}}
		}

		pipelineLog("CREATE: persisted forwarding run=%s forwarding_id=%s status=%s",
			pr.ID, forwarding.ForwardingID, forwarding.Status)

		// Stamp forwarding_id + advance to REMOTE_QUEUED.
		pr.ForwardingID = forwarding.ForwardingID
		if err := s.store.UpdatePipelineRunForwarding(ctx, pr.ID,
			forwarding.ForwardingID, pipelineruns.StatusRemoteQueued); err != nil {
			pipelineLog("CREATE: failed to stamp forwarding_id run=%s: %v", pr.ID, err)
		}

		// Update the run with the result JSON for audit.
		if resultJSON, mErr := json.Marshal(result); mErr == nil {
			if err := s.store.UpdatePipelineRunResult(ctx, pr.ID, string(resultJSON)); err != nil {
				pipelineLog("CREATE: failed to stamp result_json run=%s: %v", pr.ID, err)
			}
		}

		return CreatePipelineRunResponse{Status: http.StatusAccepted, Body: gin.H{
			"ok":              true,
			"pipeline_run_id": pr.ID,
			"request_id":      pr.RequestID,
			"remote_job_id":   jobID,
			"forwarding_id":   forwarding.ForwardingID,
			"status":          string(pipelineruns.StatusRemoteQueued),
			"status_url":      "/api/v1/pipeline-runs/" + pr.ID,
		}}
	}

	// No job_id in the response — contract violation.
	pipelineLog("CREATE: remote response missing job_id run=%s", pr.ID)
	if markErr := s.store.UpdatePipelineRunError(ctx, pr.ID,
		"REMOTE_CONTRACT", "remote response missing job_id", "REMOTE_SUBMITTING"); markErr != nil {
		pipelineLog("CREATE: failed to mark REMOTE_CONTRACT run=%s: %v", pr.ID, markErr)
	}
	return CreatePipelineRunResponse{Status: http.StatusBadGateway, Body: gin.H{
		"ok":              false,
		"pipeline_run_id": pr.ID,
		"request_id":      pr.RequestID,
		"status":          string(pipelineruns.StatusFailed),
		"error":           "remote response missing job_id",
		"status_url":      "/api/v1/pipeline-runs/" + pr.ID,
	}}
}

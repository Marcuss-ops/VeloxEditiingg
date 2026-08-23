package pipeline

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/creatorflow"
	"velox-server/internal/remoteengine"
	"velox-server/internal/statusboundary"
	"velox-shared/contract"
	"velox-shared/contract/assembly"
	"velox-shared/contract/deliveryplan"
	"velox-shared/contract/domain"
	"velox-shared/publication"
)

const defaultCreatorSourceProvider = "creator"

// CreatorIntakeSink records the accepted payload count by intake path.
// Nil values are treated as a noop sink (safe default for tests and
// for callers that have not yet wired the metric). The canonical
// implementation lives in velox-server/internal/metrics.
type CreatorIntakeSink interface {
	IncAccepted(path string)
}

// noopCreatorIntakeSink is the safe default when no sink is wired.
// The handler falls back to it so a missing wiring never panics and
// never silently drops a metric event.
type noopCreatorIntakeSink struct{}

func (noopCreatorIntakeSink) IncAccepted(string) {}

// intakeSinkOrNoop returns the wired sink or a noop if not set. This
// is the single observation point the handler uses; the test suite
// asserts against the same accessor.
func (h *Handlers) intakeSinkOrNoop() CreatorIntakeSink {
	if h.intakeSink == nil {
		return noopCreatorIntakeSink{}
	}
	return h.intakeSink
}

type creatorPushRequest struct {
	SourceProvider   string                 `json:"source_provider"`
	SourceJobID      string                 `json:"source_job_id"`
	TargetExecutorID string                 `json:"target_executor_id"`
	Payload          map[string]interface{} `json:"payload"`
}

type normalizedCreatorPush struct {
	SourceProvider   string
	SourceJobID      string
	TargetExecutorID string
	WorkerPayload    map[string]interface{}
	// DeliveryPlan is control-plane routing data. It is never part of
	// WorkerPayload and is passed separately to creatorflow.Resolver.
	DeliveryPlan map[string]interface{}

	// PublicationSpecs are control-plane delivery intents. They are
	// intentionally separate from WorkerPayload and must never be serialized
	// into the renderer task payload sent to the C++ engine.
	PublicationSpecs []publication.Spec

	// StatusDomains is the typed view of status values known at this
	// boundary. The wire payload remains in WorkerPayload unchanged; domains
	// that are not present here stay nil rather than being inferred.
	StatusDomains statusboundary.Domains  `json:"-"`
	Assembly      *assembly.AssemblyJobV1 `json:"-"`
}

// CanonicalCompletedPayload is the typed struct every intake path
// (creator_push, external /api/v1/jobs, and any future producer) MUST
// converge on before the resolver entry point. It binds the dedup
// identity tuple (source_provider, source_job_id, target_executor_id)
// to the canonical worker payload — the map shape that flows through
// ParseRemotePipelineResult → ToWorkerPayloadChecked.
//
// Concretely:
//
//   - SourceProvider + SourceJobID + TargetExecutorID form the UNIQUE
//     row key in creator_forwardings. Two POSTs with the same triple
//     converge on the same Velox job.
//
//   - WorkerPayload is the canonical typed-DTO output that the
//     Resolver consumes. It is NOT the wire format and not the flat// request body; it is what ParseRemotePipelineResult.ToWorkerPayloadChecked
// produces. Callers MUST NOT hand-build this map; the canonical

//	path is the typed DTO.
//
// Created by normalizeCreatorPushRequest (creator_push) and
// NormalizeExternalJobSubmission (/api/v1/jobs). Both paths produce
// the same shape so a future intake (e.g., webhook intake) only needs
// to return one CanonicalCompletedPayload too.
type CanonicalCompletedPayload = normalizedCreatorPush

// normalizeCreatorPushRequest is the canonical typed-DTO adapter
// for POST /api/v1/creator/jobs. It runs the raw creator map through
// ParseRemotePipelineResult, derives the worker payload via
// ToWorkerPayload, and produces the resolver identity tuple
// (source_provider + source_job_id + target_executor_id) with the
// documented fallback chain.
//
// source_provider defaults to defaultCreatorSourceProvider when
// empty. source_job_id falls back to dto.RemoteJobID and then to the
// worker payload's (job_id|trace_id|id) keys. target_executor_id
// falls back to the worker payload's (executor_id|pipeline_id) keys,
// then to the hardcoded "scene.composite.v1" only when both the
// request and the payload are silent — callers SHOULD pass an
// explicit target to avoid silent fallback.
func normalizeCreatorPushRequest(req creatorPushRequest) (*normalizedCreatorPush, error) {
	if req.Payload == nil {
		return nil, domain.NewInvalidPayload("payload", "required", "payload is required")
	}

	// CreatorPush accepts a fully assembled producer handoff. Interpret the
	// overloaded wire field in its input-assembly domain before the payload
	// reaches the resolver; SUCCEEDED is a JobStatus and must never be used
	// to describe this producer-side boundary.
	payload, err := normalizeCreatorInputAssemblyPayload(req.Payload)
	if err != nil {
		return nil, err
	}

	deliveryPlan := deliveryplan.ExtractEnvelope(payload)
	dto, err := remoteengine.ParseRemotePipelineResult(payload)
	if err != nil {
		return nil, fmt.Errorf("parse creator payload: %w", err)
	}
	workerPayload, projectionErr := dto.ToWorkerPayloadChecked()
	if projectionErr != nil {
		return nil, fmt.Errorf("project creator payload: %w", projectionErr)
	}

	sourceProvider := strings.TrimSpace(req.SourceProvider)
	if sourceProvider == "" {
		sourceProvider = defaultCreatorSourceProvider
	}

	sourceJobID := strings.TrimSpace(req.SourceJobID)
	if sourceJobID == "" {
		sourceJobID = strings.TrimSpace(dto.RemoteJobID)
	}
	if sourceJobID == "" {
		sourceJobID = firstStringResolver(workerPayload, "job_id", "trace_id", "id")
	}
	if sourceJobID == "" {
		return nil, domain.NewInvalidPayload("source_job_id", "required", "source_job_id is required (set it in the envelope or payload.job_id)")
	}

	targetExecutorID := strings.TrimSpace(req.TargetExecutorID)
	if targetExecutorID == "" {
		targetExecutorID = firstStringResolver(workerPayload, "executor_id", "pipeline_id")
	}
	if targetExecutorID == "" {
		targetExecutorID = "scene.composite.v1"
	}

	return &normalizedCreatorPush{
		SourceProvider:   sourceProvider,
		SourceJobID:      sourceJobID,
		TargetExecutorID: targetExecutorID,
		WorkerPayload:    workerPayload,
		DeliveryPlan:     deliveryPlan,
		PublicationSpecs: nil,
		StatusDomains:    inputAssemblyStatusDomain(workerPayload),
	}, nil
}

// normalizeCreatorInputAssemblyPayload canonicalizes the CreatorPush wire
// status without changing the established wire value. Missing status remains
// accepted for legacy payloads; the resolver's completeness gate handles that
// case. When present, only InputAssemblyCompleted is a valid completed handoff.
func inputAssemblyStatusDomain(payload map[string]interface{}) statusboundary.Domains {
	status, _ := payload["status"].(string)
	parsed, ok := statusboundary.ParseInputAssembly(status)
	if !ok {
		return statusboundary.Domains{}
	}
	return statusboundary.Domains{InputAssembly: &parsed}
}

func normalizeCreatorInputAssemblyPayload(raw map[string]interface{}) (map[string]interface{}, error) {
	payload := make(map[string]interface{}, len(raw))
	for key, value := range raw {
		payload[key] = value
	}

	rawStatus, present := payload["status"]
	if !present {
		return payload, nil
	}
	statusText, ok := rawStatus.(string)
	if !ok {
		return nil, domain.NewInvalidPayload("payload.status", "invalid", "status must be an input-assembly status")
	}
	status, ok := contract.ParseInputAssemblyStatus(statusText)
	if !ok || status != contract.InputAssemblyCompleted {
		return nil, domain.NewInvalidPayload("payload.status", "invalid", "CreatorPush status must be input-assembly completed, not a job lifecycle status")
	}
	payload["status"] = string(status)
	return payload, nil
}

// resolveCompletedPayload is the canonical HTTP-side adapter into
// creatorflow.Resolver.Resolve. It is intentionally provider-agnostic
// so any future creator intake (creator_push today, plus any new
// producer that wants the same atomic forwarding + Job+Task write
// path) converges on the same code. Previously this lived in
// forwarding.go alongside the now-removed legacy sync-forward path;
// after that path was retired the helper moved here as the single
// remaining Module's resolver entry call.
func (h *Handlers) resolveCompletedPayload(
	ctx context.Context,
	sourceProvider string,
	sourceJobID string,
	targetExecutorID string,
	result map[string]interface{},
	deliveryPlan map[string]interface{},
	publicationSpecs []publication.Spec,
	assemblyJob *assembly.AssemblyJobV1,
	externalClientID string,
	intakeSource string,
) (map[string]interface{}, error) {
	if h.submission != nil {
		out, err := h.submission.Submit(ctx, creatorflow.CanonicalJobSubmission{
			ExternalClientID: externalClientID,
			IntakeSource:     intakeSource,
			SourceProvider:   sourceProvider,
			SourceJobID:      sourceJobID,
			TargetExecutorID: targetExecutorID,
			Payload:          result,
			DeliveryPlan:     deliveryPlan,
			PublicationSpecs: publicationSpecs,
			Assembly:         assemblyJob,
		})
		if err != nil {
			return nil, err
		}
		if out == nil {
			return nil, nil
		}
		return out.Response, nil
	}
	if h.resolver == nil {
		return nil, fmt.Errorf("pipeline handler requires a wired resolver (composition root MUST pass creatorflow.Resolver)")
	}

	sourceProvider = strings.TrimSpace(sourceProvider)
	if sourceProvider == "" {
		return nil, domain.NewInvalidPayload("source_provider", "required", "source_provider is required")
	}
	sourceJobID = strings.TrimSpace(sourceJobID)
	if sourceJobID == "" {
		return nil, domain.NewInvalidPayload("source_job_id", "required", "source_job_id is required")
	}
	if result == nil {
		return nil, domain.NewInvalidPayload("payload", "required", "payload is required")
	}

	out, err := h.resolver.Resolve(ctx, creatorflow.ResolveRequest{
		ForwardingID:     "",
		SourceProvider:   sourceProvider,
		SourceJobID:      sourceJobID,
		TargetExecutorID: strings.TrimSpace(targetExecutorID),
		Payload:          result,
		DeliveryPlan:     deliveryPlan,
		PublicationSpecs: publicationSpecs,
		Assembly:         nil,
		ExternalClientID: externalClientID,
	})
	if err != nil {
		// Defense-in-depth log hygiene: errors from the resolver
		// layer MIGHT embed user-supplied content (e.g., a malformed
		// URL flowing through enqueue-layer validation, or a wrapped
		// error whose Message carries the raw input). Surface the
		// typed error class via %T (server-controlled) and a short
		// hash for log-grep correlation; the full HTTP envelope goes
		// out via WriteResolverError a few lines below.
		pipelineLog(
			"FORWARD: Resolver.Resolve FAILED provider=%s source_job_hash=%s err_class=%T err_hash=%s",
			sourceProvider,
			logHashShort(sourceJobID),
			err,
			logHashShort(err.Error()),
		)
		return nil, err
	}
	if out == nil {
		return nil, nil
	}

	pipelineLog(
		"FORWARD: enqueued via Resolver provider=%s source_job_hash=%s job_id=%s forwarding_id=%s",
		sourceProvider,
		logHashShort(sourceJobID),
		out.JobID,
		out.ForwardingID,
	)
	return out.Response, nil
}

// firstStringResolver reads the first non-empty string value from a
// map across the provided keys. Package-private helper (no tests
// outside this package should depend on it directly).
func firstStringResolver(m map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if v, ok := m[key]; ok {
			if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
				return strings.TrimSpace(s)
			}
		}
	}
	return ""
}

// CreatorPush handles POST /api/v1/creator/jobs.
//
// A creator machine calls this endpoint only after it has assembled the full
// render payload (voiceover, scenes/clips/stock references and delivery plan).
// The master does not initiate or poll the creator. The completed payload enters
// the existing creatorflow.Resolver and is atomically converted into the same
// Job+Task shape used by every other producer.
func (h *Handlers) CreatorPush() gin.HandlerFunc {
	return func(c *gin.Context) {
		var req creatorPushRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			creatorPushError(c, http.StatusBadRequest, "invalid_json", "request body must be valid JSON", nil)
			return
		}

		normalized, err := normalizeCreatorPushRequest(req)
		if err != nil {
			creatorPushError(c, http.StatusUnprocessableEntity, "invalid_payload", err.Error(), nil)
			return
		}

		forwarded, err := h.resolveCompletedPayload(
			c.Request.Context(),
			normalized.SourceProvider,
			normalized.SourceJobID, normalized.TargetExecutorID,
			normalized.WorkerPayload,
			normalized.DeliveryPlan,
			normalized.PublicationSpecs,
			nil,
			ClientIDFromContext(c),
			creatorflow.IntakeSourceCreator,
		)
		if err != nil {
			// P0 contract: every resolver-layer error is mapped to
			// the canonical HTTP envelope by the shared helper in
			// package creatorflow. Inlining the cascade here
			// invited drift each time a new error class surfaced;
			// job_submit.go was historically missing the
			// enqueue.ValidationErrorField branch and silently
			// downgraded those errors to 500. The helper owns
			// the full mapping (enqueue.ValidationErrorField path
			// for typed errors + a fallback to idempotency_key
			// when the 409 carries no typed path).
			creatorflow.WriteResolverError(c, err)
			return
		}
		if forwarded == nil {
			creatorPushError(c, http.StatusInternalServerError, "resolver_failure", "creator payload resolved without an enqueue response", nil)
			return
		}

		response := gin.H{}
		for key, value := range forwarded {
			response[key] = value
		}
		response["ok"] = true
		response["accepted_from"] = "creator_push"
		response["source_provider"] = normalized.SourceProvider
		response["source_job_id"] = normalized.SourceJobID
		response["target_executor_id"] = normalized.TargetExecutorID
		if _, owned := response["dispatch_status"]; !owned {
			response["dispatch_status"] = "queued_for_workers"
		}

		h.intakeSinkOrNoop().IncAccepted("creator_push")
		jobID, _ := response["job_id"].(string)
		pipelineLog(
			"CREATOR_PUSH_ACCEPTED path=creator_push source_provider=%s source_job_id_hash=%s target_executor_id=%s job_id=%s",
			normalized.SourceProvider,
			logHashShort(normalized.SourceJobID),
			normalized.TargetExecutorID,
			jobID,
		)

		c.JSON(http.StatusAccepted, response)
	}
}

func creatorPushError(c *gin.Context, status int, code, message string, detail any) {
	body := gin.H{"ok": false, "error": code, "error_code": code, "message": message}
	if detail != nil {
		body["details"] = []any{detail}
	}
	c.JSON(status, body)
}

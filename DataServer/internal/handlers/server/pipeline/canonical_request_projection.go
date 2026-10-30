// Package pipeline — canonical_request_projection.go owns the typed external intake facade.
package pipeline

import (
	"velox-shared/contract/assembly"
	"velox-shared/contract/deliveryplan"
	"velox-shared/publication"
)

// ExternalAPISourceProvider is the stable source-provider identity for POST /api/v1/jobs.
const ExternalAPISourceProvider = "external_api"

// JobSubmitTargetExecutorID is the sole worker executor for compiled V2
// video plans. The Master validates and routes the plan; it does not render.
const JobSubmitTargetExecutorID = "video.assemble.copy.v1"

func (h *Handlers) NormalizeExternalJobSubmission(req SubmitJobRequest) *CanonicalCompletedPayload {
	rawPayload := submitRequestToRawPayload(&req)
	workerPayload, err := projectWorkerPayload(&req)
	if err != nil {
		return nil
	}

	var assemblyJob *assembly.AssemblyJobV1
	if req.Assembly != nil {
		assemblyJob, _ = req.Assembly.Normalize(req.IdempotencyKey)
	}
	targetExecutorID := targetExecutorForJobType(req.JobType)
	deliveryEnvelope := deliveryplan.ExtractEnvelope(rawPayload)
	if req.RenderOnly {
		// Render-only jobs have no external delivery route. Keep only the
		// opt-out marker in the control-plane envelope, even if a legacy or
		// defaulted intake DTO carried an empty delivery-plan entry.
		deliveryEnvelope = map[string]interface{}{"render_only": true}
	}
	return &CanonicalCompletedPayload{
		SourceProvider:   ExternalAPISourceProvider,
		SourceJobID:      req.IdempotencyKey,
		TargetExecutorID: targetExecutorID,
		WorkerPayload:    workerPayload,
		DeliveryPlan:     deliveryEnvelope,
		PublicationSpecs: projectPublicationSpecs(req.Publications),
		Assembly:         assemblyJob,
		StatusDomains:    inputAssemblyStatusDomain(workerPayload),
	}
}

func targetExecutorForJobType(jobType string) string {
	// Recipe names are decoupled from worker executor IDs. Every admitted
	// video request is normalized to CompiledRenderPlanV2 before dispatch.
	_ = jobType
	return JobSubmitTargetExecutorID
}

// projectPublicationSpecs converts intake DTOs into the canonical control-plane
// contract. These specs are retained on CanonicalCompletedPayload and are never
// merged into WorkerPayload: the renderer only needs render inputs, while the
// delivery pipeline owns publication metadata and destinations.
func projectPublicationSpecs(publications []SubmitPublication) []publication.Spec {
	if len(publications) == 0 {
		return nil
	}
	specs := make([]publication.Spec, 0, len(publications))
	for _, input := range publications {
		spec := submitPublicationToSharedSpec(input)
		if normalized, err := spec.Normalize(); err == nil {
			spec = normalized
		}
		specs = append(specs, spec)
	}
	return specs
}

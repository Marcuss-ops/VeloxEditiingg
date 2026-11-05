// Package pipeline — canonical_request_projection.go owns the typed external intake facade.
package pipeline

import (
	"velox-shared/contract"
	"velox-shared/contract/assembly"
	"velox-shared/contract/deliveryplan"
	"velox-shared/publication"
)

// ExternalAPISourceProvider is the stable source-provider identity for POST /api/v1/jobs.
const ExternalAPISourceProvider = "external_api"

// JobSubmitTargetExecutorID is the default executor for pre-certified plans.
const JobSubmitTargetExecutorID = contract.VideoAssemblyCopyExecutorID

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
	targetExecutorID := contract.RequiredVideoAssemblyExecutor(workerPayload)
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

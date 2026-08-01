// Package pipeline — canonical_request_projection.go owns the typed external intake facade.
package pipeline

import "velox-shared/publication"

// ExternalAPISourceProvider is the stable source-provider identity for POST /api/v1/jobs.
const ExternalAPISourceProvider = "external_api"

// JobSubmitTargetExecutorID is the canonical executor for POST /api/v1/jobs.
const JobSubmitTargetExecutorID = "scene.composite.v1"

func (h *Handlers) NormalizeExternalJobSubmission(req SubmitJobRequest) *CanonicalCompletedPayload {
	h.emitLegacyRequestWarning(req)

	rawPayload := submitRequestToRawPayload(&req)
	workerPayload := projectWorkerPayload(&req)

	return &CanonicalCompletedPayload{
		SourceProvider:   ExternalAPISourceProvider,
		SourceJobID:      req.IdempotencyKey,
		TargetExecutorID: JobSubmitTargetExecutorID,
		WorkerPayload:    workerPayload,
		DeliveryPlan:     extractDeliveryPlanEnvelope(rawPayload),
		PublicationSpecs: projectPublicationSpecs(req.Publications),
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

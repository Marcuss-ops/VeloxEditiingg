// Package pipeline — canonical_request_projection.go owns the typed external intake facade.
package pipeline

// ExternalAPISourceProvider is the stable source-provider identity for POST /api/v1/jobs.
const ExternalAPISourceProvider = "external_api"

// JobSubmitTargetExecutorID is the canonical executor for POST /api/v1/jobs.
const JobSubmitTargetExecutorID = "scene.composite.v1"

func (h *Handlers) NormalizeExternalJobSubmission(req SubmitJobRequest) *CanonicalCompletedPayload {
	h.emitLegacyRequestWarning(req)

	workerPayload := projectWorkerPayload(&req)

	return &CanonicalCompletedPayload{
		SourceProvider:   ExternalAPISourceProvider,
		SourceJobID:      req.IdempotencyKey,
		TargetExecutorID: JobSubmitTargetExecutorID,
		WorkerPayload:    workerPayload,
	}
}

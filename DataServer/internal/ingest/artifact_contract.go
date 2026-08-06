package ingest

import (
	"context"
	"encoding/json"
	"fmt"

	"velox-server/internal/jobs"
)

// jobArtifactContractReader derives the job's artifact contract from the
// persisted control-plane payload. render_only=true is the explicit,
// auditable no-artifact path; every other job is artifact-bound and must pass
// through AWAITING_ARTIFACT before terminal success.
type jobArtifactContractReader struct {
	jobs jobs.Reader
}

func (r jobArtifactContractReader) RequiresArtifact(ctx context.Context, jobID string) (bool, error) {
	if r.jobs == nil {
		return true, fmt.Errorf("job artifact contract reader is not configured")
	}
	job, err := r.jobs.Get(ctx, jobID)
	if err != nil {
		return true, err
	}
	if job == nil {
		return true, fmt.Errorf("job %s not found", jobID)
	}
	if job.Payload == "" {
		return true, nil
	}
	var payload map[string]interface{}
	if err := json.Unmarshal([]byte(job.Payload), &payload); err != nil {
		return true, fmt.Errorf("decode job %s request payload: %w", jobID, err)
	}
	renderOnly, _ := payload["render_only"].(bool)
	return !renderOnly, nil
}

// Ingest never owns verified artifact promotion. A job-bound success must
// arrive through the artifact finalizer, so this method fails closed if a
// caller attempts to use the ingest transition service as that finalizer.
func (jobArtifactContractReader) RequiredArtifactsReady(context.Context, string) (bool, error) {
	return false, fmt.Errorf("artifact readiness is owned by verified finalization, not task-result ingest")
}

var _ jobs.ArtifactReadiness = jobArtifactContractReader{}

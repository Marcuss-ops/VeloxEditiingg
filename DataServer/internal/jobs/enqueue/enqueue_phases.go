package enqueue

import (
	"context"
	"errors"
	"fmt"

	"velox-server/internal/costmodel"
	"velox-server/internal/jobs"
	"velox-server/internal/routing"
	"velox-server/internal/taskgraph"
	"velox-server/internal/telemetry"
)

// EnqueuePhase identifies the logical stage that rejected an enqueue request.
// The values are stable machine-readable labels for API/log classification.
type EnqueuePhase string

const (
	EnqueuePhaseValidateInput     EnqueuePhase = "validate_input"
	EnqueuePhaseResolveAssets     EnqueuePhase = "resolve_assets"
	EnqueuePhaseNormalizePayload  EnqueuePhase = "normalize_scene_video_payload"
	EnqueuePhaseProjectWorker     EnqueuePhase = "project_worker_payload"
	EnqueuePhasePersistJobAndTask EnqueuePhase = "persist_job_task"
)

// PhaseError preserves the original error for errors.Is/errors.As while
// identifying the enqueue phase that produced it.
type PhaseError struct {
	Phase EnqueuePhase
	Err   error
}

func (e *PhaseError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("enqueue phase %s: %v", e.Phase, e.Err)
}

func (e *PhaseError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// EnqueuePhaseOf extracts the phase label from an error returned by enqueue.
func EnqueuePhaseOf(err error) (EnqueuePhase, bool) {
	var phaseErr *PhaseError
	if !errors.As(err, &phaseErr) || phaseErr == nil {
		return "", false
	}
	return phaseErr.Phase, true
}

func wrapEnqueuePhase(phase EnqueuePhase, err error) error {
	if err == nil {
		return nil
	}
	return &PhaseError{Phase: phase, Err: err}
}

func (e *Enqueuer) validateEnqueueInput(payloadMap map[string]interface{}) (string, bool, error) {
	if _, present := payloadMap["subtitle_tracks"]; present {
		return "", false, fmt.Errorf("subtitle_tracks is retired; use scenes[].subtitles")
	}
	if e == nil || e.Creator == nil {
		return "", false, fmt.Errorf("creator unavailable")
	}
	return validateForwardingIdentity(payloadMap)
}

// resolveEnqueueAssets owns all mutating asset preparation. It intentionally
// receives the original map so legacy aliases are resolved exactly once before
// canonical normalization, preserving the existing compatibility behavior.
func (e *Enqueuer) resolveEnqueueAssets(ctx context.Context, payloadMap map[string]interface{}) error {
	if err := e.resolveVoiceoverPayload(ctx, payloadMap); err != nil {
		return err
	}
	if err := e.resolveSceneImagePayload(ctx, payloadMap); err != nil {
		return err
	}
	if hasTimedVideoClipSegmentsContext(ctx, payloadMap) && e.Voiceover == nil {
		return fmt.Errorf("video clip segments require master asset service for trimming")
	}
	if e.Voiceover != nil {
		if err := e.Voiceover.RewriteVideoClipSegments(ctx, payloadMap); err != nil {
			return err
		}
		if err := e.Voiceover.RewriteRemoteInputPayload(ctx, payloadMap); err != nil {
			return err
		}
	}
	return nil
}

func normalizeEnqueuePayload(ctx context.Context, payloadMap map[string]interface{}, forwardingKey string, hasForwardingKey bool) (map[string]interface{}, error) {
	normalized, err := normalizeSceneVideoPayloadContext(ctx, payloadMap)
	if err != nil {
		return nil, err
	}
	if hasForwardingKey {
		// Re-inject the identity after canonical projection so a future
		// payload-field change cannot turn a forwarded retry into a UUID job.
		normalized[routing.KeyForwardingKey] = forwardingKey
	}
	return normalized, nil
}

func projectEnqueueJob(normalized map[string]interface{}, req costmodel.JobRequirements) (*jobs.Job, *taskgraph.TaskSpec, int, error) {
	return projectEnqueueJobContext(context.Background(), normalized, req)
}

func projectEnqueueJobContext(ctx context.Context, normalized map[string]interface{}, req costmodel.JobRequirements) (*jobs.Job, *taskgraph.TaskSpec, int, error) {
	return compileSceneVideoJobContext(ctx, normalized, req)
}

func (e *Enqueuer) persistEnqueueJobTask(ctx context.Context, job *jobs.Job, spec *taskgraph.TaskSpec, priority int) error {
	finishPhase := telemetry.BeginEnqueuePhase(ctx, string(EnqueuePhasePersistJobAndTask))
	defer finishPhase()
	if e == nil || e.Creator == nil {
		return fmt.Errorf("creator unavailable")
	}
	return e.Creator.CreateJobWithTask(ctx, job, spec, priority)
}

func (e *Enqueuer) validateEnqueueDeliveryPlan(ctx context.Context, payloadMap map[string]interface{}) error {
	if e == nil {
		return fmt.Errorf("enqueuer unavailable")
	}
	return validateDeliveryPlanRequires(ctx, payloadMap, e.SocialValidator)
}

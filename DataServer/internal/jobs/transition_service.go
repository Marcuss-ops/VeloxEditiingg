package jobs

import (
	"context"
	"errors"
	"fmt"

	"velox-server/internal/statemachine"
)

// ErrArtifactNotReady is returned when a job success transition is attempted
// before its required artifact has been verified and made durable.
var ErrArtifactNotReady = errors.New("jobs: required artifact is not ready")

// ArtifactReadiness is the narrow read-only contract used by TransitionService
// for the final job gate. Implementations must report the persisted artifact
// state and durable blob presence; callers must not infer readiness from URLs
// or worker-provided metadata.
type ArtifactReadiness interface {
	RequiredArtifactsReady(ctx context.Context, jobID string) (bool, error)
}

// TransitionService is the canonical application boundary for JobState
// mutations. Persistence remains behind jobs.Writer; callers do not perform
// job-state writes directly. The service validates the lifecycle registry and
// applies the final artifact gate before delegating the CAS write.
type TransitionService struct {
	writer    Writer
	readiness ArtifactReadiness
}

// NewTransitionService constructs the sole job-state transition boundary.
func NewTransitionService(writer Writer, readiness ArtifactReadiness) (*TransitionService, error) {
	if writer == nil {
		return nil, fmt.Errorf("jobs: TransitionService requires a writer")
	}
	return &TransitionService{writer: writer, readiness: readiness}, nil
}

// Transition validates and applies one job state transition. The caller must
// identify the canonical actor; an omitted actor is rejected so ownership
// cannot be bypassed by a compatibility adapter. SUCCEEDED is deliberately
// impossible without an injected readiness reader.
func (s *TransitionService) Transition(ctx context.Context, id string, from, to JobState, actor statemachine.Actor) error {
	if s == nil || s.writer == nil {
		return fmt.Errorf("jobs: TransitionService is not configured")
	}
	if id == "" {
		return fmt.Errorf("jobs: empty job id")
	}
	if actor == "" {
		return fmt.Errorf("jobs: transition actor is required")
	}
	if err := statemachine.DefaultRegistry().Validate(statemachine.DomainJob, string(from), string(to), actor); err != nil {
		return err
	}
	if to == StatusSucceeded {
		if s.readiness == nil {
			return ErrArtifactNotReady
		}
		ready, err := s.readiness.RequiredArtifactsReady(ctx, id)
		if err != nil {
			return fmt.Errorf("jobs: check required artifacts for %s: %w", id, err)
		}
		if !ready {
			return ErrArtifactNotReady
		}
	}
	return s.writer.SetStatus(ctx, id, from, to)
}

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

// ArtifactReadiness is the explicit persisted artifact contract used by
// TransitionService. RequiresArtifact distinguishes a render-only job from a
// job whose lifecycle includes a required artifact; RequiredArtifactsReady
// reports the verifier's durable READY/hash/blob result. Callers must not
// infer either fact from worker-provided URLs or metadata.
type ArtifactReadiness interface {
	RequiresArtifact(ctx context.Context, jobID string) (bool, error)
	RequiredArtifactsReady(ctx context.Context, jobID string) (bool, error)
}

// ErrArtifactContractRequiresAwaiting is returned when an artifact-contract
// job attempts to skip the mandatory RUNNING -> AWAITING_ARTIFACT gate.
var ErrArtifactContractRequiresAwaiting = errors.New("jobs: artifact contract requires AWAITING_ARTIFACT")

// ErrArtifactContractMissing is returned when a caller tries to finalize an
// AWAITING_ARTIFACT job without an explicit artifact contract.
var ErrArtifactContractMissing = errors.New("jobs: artifact contract is missing")

// ErrArtifactContractCheck is returned when the contract lookup itself fails.
var ErrArtifactContractCheck = errors.New("jobs: artifact contract check failed")

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
// cannot be bypassed by a compatibility adapter. For SUCCEEDED, the explicit
// no-artifact path permits RUNNING -> SUCCEEDED only when the explicit
// contract reader reports no artifact requirement. Contract-bound jobs must
// pass through AWAITING_ARTIFACT and
// the final transition must observe verified readiness.
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
		// The contract reader is mandatory even for render-only jobs. A nil
		// adapter is a bootstrap/configuration error, not permission to
		// bypass the artifact contract.
		if s.readiness == nil {
			return ErrArtifactNotReady
		}
		requires, err := s.readiness.RequiresArtifact(ctx, id)
		if err != nil {
			return fmt.Errorf("%w for %s: %v", ErrArtifactContractCheck, id, err)
		}
		if from == StatusRunning && requires {
			return ErrArtifactContractRequiresAwaiting
		}
		if from == StatusAwaitingArtifact && !requires {
			return ErrArtifactContractMissing
		}
		if !requires {
			// A non-contract job can use the explicit direct-success path;
			// it must not be accepted merely because a readiness adapter
			// happens to be installed.
			return s.writer.SetStatus(ctx, id, from, to)
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

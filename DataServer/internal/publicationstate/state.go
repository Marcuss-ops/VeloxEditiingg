// Package publicationstate owns the durable publication state machine. A
// publication phase is advanced monotonically and every side effect can be
// retried with the phase-specific idempotency key.
package publicationstate

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// PublicationStatus is the durable lifecycle state of one publication.
// It is distinct from JobStatus and DeliveryStatus; values are converted to
// strings only at storage and API boundaries.
type PublicationStatus string

// State is retained as a source-compatible alias for existing callers.
type State = PublicationStatus

const (
	Pending               State = "PENDING"
	WaitingForRender      State = "WAITING_FOR_RENDER"
	ArtifactBound         State = "ARTIFACT_BOUND"
	Ready                 State = "READY"
	Scheduled             State = "SCHEDULED"
	Uploading             State = "UPLOADING"
	VideoCreated          State = "VIDEO_CREATED"
	MetadataApplying      State = "METADATA_APPLYING"
	LocalizationsApplying State = "LOCALIZATIONS_APPLYING"
	Verifying             State = "VERIFYING"
	Published             State = "PUBLISHED"
	Partial               State = "PARTIAL"
	RetryWait             State = "RETRY_WAIT"
	Failed                State = "FAILED"
	Cancelled             State = "CANCELLED"
)

var (
	ErrInvalidTransition  = errors.New("publicationstate: invalid transition")
	ErrInvalidPublication = errors.New("publicationstate: invalid publication")
)

var transitions = map[PublicationStatus]map[PublicationStatus]struct{}{
	Pending:               {WaitingForRender: {}, Cancelled: {}},
	WaitingForRender:      {ArtifactBound: {}, RetryWait: {}, Failed: {}, Cancelled: {}},
	ArtifactBound:         {Ready: {}, RetryWait: {}, Failed: {}, Cancelled: {}},
	Ready:                 {Scheduled: {}, Uploading: {}, Cancelled: {}},
	Scheduled:             {Uploading: {}, Cancelled: {}},
	Uploading:             {VideoCreated: {}, RetryWait: {}, Failed: {}, Cancelled: {}},
	VideoCreated:          {MetadataApplying: {}, RetryWait: {}, Failed: {}, Cancelled: {}},
	MetadataApplying:      {LocalizationsApplying: {}, Verifying: {}, Partial: {}, RetryWait: {}, Failed: {}},
	LocalizationsApplying: {Verifying: {}, Partial: {}, RetryWait: {}, Failed: {}},
	Verifying:             {Published: {}, Partial: {}, RetryWait: {}, Failed: {}},
	// RetryWait is deliberately broad at the table level for backwards
	// compatibility with callers that only have a state value. Snapshot
	// transitions below enforce the persisted RetryFrom checkpoint and do not
	// allow a retry to jump back to UPLOADING accidentally.
	RetryWait: {WaitingForRender: {}, ArtifactBound: {}, Uploading: {}, MetadataApplying: {}, LocalizationsApplying: {}, Verifying: {}, Cancelled: {}, Failed: {}},
	Partial:   {LocalizationsApplying: {}, MetadataApplying: {}, Verifying: {}, Published: {}, RetryWait: {}, Cancelled: {}},
}

// Valid reports whether s is a known publication state.
func (s PublicationStatus) Valid() bool {
	switch s {
	case Pending, WaitingForRender, ArtifactBound, Ready, Scheduled, Uploading, VideoCreated, MetadataApplying, LocalizationsApplying, Verifying, Published, Partial, RetryWait, Failed, Cancelled:
		return true
	default:
		return false
	}
}

func ValidateTransition(from, to PublicationStatus) error {
	if from == to {
		return nil
	}
	if _, terminal := map[State]bool{Published: true, Failed: true, Cancelled: true}[from]; terminal {
		return fmt.Errorf("%w: %s is terminal", ErrInvalidTransition, from)
	}
	if _, ok := transitions[from][to]; !ok {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, from, to)
	}
	return nil
}

func IdempotencyKey(publicationID string, phase State) string {
	h := sha256.Sum256([]byte(strings.TrimSpace(publicationID) + "\x00" + string(phase)))
	return "publication:" + hex.EncodeToString(h[:])
}

// SideEffectKey returns a stable key for one operation inside one phase. A
// phase may contain more than one remote call (for example metadata and
// localizations), so callers must not reuse the publication-wide key for all
// operations in that phase.
func SideEffectKey(publicationID string, phase State, operation string) string {
	h := sha256.Sum256([]byte(strings.TrimSpace(publicationID) + "\x00" + string(phase) + "\x00" + strings.TrimSpace(operation)))
	return "publication-phase:" + hex.EncodeToString(h[:])
}

// Snapshot is the durable control-plane state for one publication. RetryFrom
// is part of the state, not an in-memory inference: after a restart a retry
// of LOCALIZATIONS_APPLYING must never restart UPLOADING.
type Snapshot struct {
	PublicationID string
	State         PublicationStatus
	RetryFrom     PublicationStatus
	Revision      uint64
}

func NewSnapshot(publicationID string) (Snapshot, error) {
	publicationID = strings.TrimSpace(publicationID)
	if publicationID == "" {
		return Snapshot{}, fmt.Errorf("%w: publication id is required", ErrInvalidPublication)
	}
	return Snapshot{PublicationID: publicationID, State: Pending}, nil
}

func (s Snapshot) Transition(to PublicationStatus) (Snapshot, error) {
	if s.PublicationID == "" {
		return Snapshot{}, fmt.Errorf("%w: publication id is required", ErrInvalidPublication)
	}
	if s.State == "" {
		return Snapshot{}, fmt.Errorf("%w: current state is required", ErrInvalidPublication)
	}
	if s.State == to {
		return s, nil // replaying the same command is idempotent
	}

	if to == RetryWait {
		checkpoint := retryCheckpoint(s.State)
		if checkpoint == "" {
			return Snapshot{}, fmt.Errorf("%w: %s cannot be retried", ErrInvalidTransition, s.State)
		}
		return s.withState(RetryWait, checkpoint), nil
	}
	if err := ValidateTransition(s.State, to); err != nil {
		return Snapshot{}, err
	}
	if s.State == RetryWait && to != s.RetryFrom {
		return Snapshot{}, fmt.Errorf("%w: retry checkpoint is %s, not %s", ErrInvalidTransition, s.RetryFrom, to)
	}
	return s.withState(to, ""), nil
}

// TransitionPartial records the exact phase to retry after a partial result.
// It is used when metadata/video creation succeeded but a later operation did
// not; the next command therefore resumes at the failed phase.
func (s Snapshot) TransitionPartial(retryFrom PublicationStatus) (Snapshot, error) {
	if retryFrom == "" {
		retryFrom = LocalizationsApplying
	}
	if !isRetryCheckpoint(retryFrom) {
		return Snapshot{}, fmt.Errorf("%w: invalid partial checkpoint %s", ErrInvalidTransition, retryFrom)
	}
	if err := ValidateTransition(s.State, Partial); err != nil {
		return Snapshot{}, err
	}
	return s.withState(Partial, retryFrom), nil
}

// ResumeAfterFailure returns the exact persisted checkpoint. The legacy
// one-argument behavior is retained for callers that only have a state; new
// durable callers should use Snapshot.Transition.
func ResumeAfterFailure(state PublicationStatus) PublicationStatus {
	if state == RetryWait {
		return Uploading // legacy fallback; Snapshot never loses RetryFrom
	}
	if checkpoint := retryCheckpoint(state); checkpoint != "" {
		return checkpoint
	}
	return state
}

func (s Snapshot) withState(state, retryFrom PublicationStatus) Snapshot {
	s.State = state
	s.RetryFrom = retryFrom
	s.Revision++
	return s
}

func retryCheckpoint(state PublicationStatus) PublicationStatus {
	switch state {
	case WaitingForRender, ArtifactBound, Uploading, MetadataApplying, LocalizationsApplying, Verifying:
		return state
	case VideoCreated:
		return MetadataApplying
	case Partial:
		return LocalizationsApplying
	default:
		return ""
	}
}

func isRetryCheckpoint(state PublicationStatus) bool {
	return retryCheckpoint(state) != ""
}

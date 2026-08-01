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

type State string

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

var ErrInvalidTransition = errors.New("publicationstate: invalid transition")

var transitions = map[State]map[State]struct{}{
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
	RetryWait:             {Uploading: {}, MetadataApplying: {}, LocalizationsApplying: {}, Verifying: {}, Cancelled: {}, Failed: {}},
	Partial:               {LocalizationsApplying: {}, MetadataApplying: {}, Verifying: {}, Published: {}, RetryWait: {}, Cancelled: {}},
}

func ValidateTransition(from, to State) error {
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

func ResumeAfterFailure(state State) State {
	switch state {
	case Partial:
		return LocalizationsApplying
	case RetryWait:
		return Uploading
	default:
		return state
	}
}

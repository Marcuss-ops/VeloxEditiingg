package store

// COMPATIBILITY:
// Owner:        P0.4 store-facade migration
// Remove after: 2026-09-30
// Read-only:    yes — publication error names delegate to storecore; lifecycle behavior remains here.

import (
	"velox-server/internal/publicationstate"
)

// store_publication_state.go owns the publication_states domain model and
// the shared helpers: the PublicationState row shape, the
// scanPublicationState projection and the phase-name mapping. Lifecycle
// bootstrap lives in store_publication_lifecycle.go, formal state-machine
// transitions in store_publication_transition.go, the side-effect ledger in
// store_publication_effect.go, and the remote-result checkpoints in
// store_publication_remote.go. The sentinel errors
// (ErrPublicationStateNotFound / ErrPublicationPhaseConflict) are re-exported
// from internal/storecore via db_errors.go.

type PublicationState struct {
	PublicationID          string
	JobID                  string
	State                  publicationstate.State
	RetryFrom              publicationstate.State
	ArtifactID             string
	RemoteID               string
	SubmittedRemoteID      string
	VerificationOperation  string
	ReconciliationVerified bool
	RemoteURL              string
	Revision               uint64
	LastErrorCode          string
	CreatedAt              string
	UpdatedAt              string
}

func publicationPhaseName(state publicationstate.State) string {
	switch state {
	case publicationstate.Uploading:
		return "UPLOAD_MEDIA"
	case publicationstate.MetadataApplying:
		return "APPLY_METADATA"
	case publicationstate.LocalizationsApplying:
		return "APPLY_LOCALIZATIONS"
	case publicationstate.Verifying:
		return "VERIFY"
	default:
		return ""
	}
}

type publicationScanner interface{ Scan(...any) error }

func scanPublicationState(row publicationScanner) (*PublicationState, error) {
	var state PublicationState
	var rawState, rawRetry string
	if err := row.Scan(&state.PublicationID, &state.JobID, &rawState, &rawRetry, &state.ArtifactID, &state.RemoteID, &state.SubmittedRemoteID, &state.VerificationOperation, &state.ReconciliationVerified, &state.RemoteURL, &state.Revision, &state.LastErrorCode, &state.CreatedAt, &state.UpdatedAt); err != nil {
		return nil, err
	}
	state.State = publicationstate.State(rawState)
	state.RetryFrom = publicationstate.State(rawRetry)
	return &state, nil
}

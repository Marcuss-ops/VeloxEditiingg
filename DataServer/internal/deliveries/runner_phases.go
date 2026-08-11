package deliveries

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"velox-server/internal/credentials"
	"velox-server/internal/publicationstate"
	"velox-server/internal/store"
)

var errDeliveryRetryScheduled = errors.New("delivery retry scheduled")

// publicationPhaseContext is deliberately private: only the runner can build
// it after hydrating the artifact, destination, and credential lease.
type publicationPhaseContext struct {
	lease           store.DeliveryLease
	publicationID   string
	artifact        *store.Artifact
	destination     *Destination
	credentialLease *credentials.AccessLease
}

func (r *DeliveryRunner) runPublicationPhases(ctx context.Context, input publicationPhaseContext, executor PublicationPhaseExecutor) error {
	state, err := r.dbStore.GetPublicationState(ctx, input.publicationID)
	if err != nil {
		return r.phaseInfrastructureFailure("PUBLICATION_STATE_NOT_FOUND", err)
	}
	if state.State == publicationstate.Published {
		if strings.TrimSpace(state.RemoteID) == "" || strings.TrimSpace(state.SubmittedRemoteID) == "" || strings.TrimSpace(state.RemoteID) == strings.TrimSpace(state.SubmittedRemoteID) {
			return r.phaseInfrastructureFailure("PUBLISHED_WITHOUT_DISTINCT_MEDIA_ID", fmt.Errorf("durable PUBLISHED state lacks distinct submission and final media evidence"))
		}
		verificationOperation := phaseOperation(publicationstate.Verifying, input.artifact, input.destination, state.SubmittedRemoteID)
		if err := r.dbStore.ValidatePublishedAfterReconciliation(ctx, input.publicationID, verificationOperation); err != nil {
			return r.phaseInfrastructureFailure("PUBLISHED_WITHOUT_RECONCILIATION_EVIDENCE", err)
		}
		if err := r.dbStore.MarkDeliverySucceeded(ctx, input.lease.DeliveryID, input.lease.RunnerID, input.lease.LeaseID, state.RemoteID, state.RemoteURL); err != nil {
			return deliveryStatePersistenceError("mark already-published delivery succeeded", err)
		}
		return nil
	}

	capabilities := executor.Capabilities()
	if capabilities == nil {
		capabilities = map[publicationstate.State]bool{}
	}

	deliverCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	heartbeatDone := make(chan struct{})
	go r.renewDeliveryLeaseLoop(deliverCtx, heartbeatDone, input.lease, func(error) { cancel() })
	defer func() { cancel(); <-heartbeatDone }()

	var finalRemoteID, finalRemoteURL string
	phase, err := r.preparePublicationState(ctx, input.publicationID, state)
	if err != nil {
		return r.phaseFailure(ctx, input.lease, input.publicationID, publicationstate.Uploading, "STATE_TRANSITION", err)
	}
	for phase != "" {
		if !capabilities[phase] {
			if phase == publicationstate.Uploading {
				return r.phaseFailure(ctx, input.lease, input.publicationID, phase, "CAPABILITY_MISSING", fmt.Errorf("%w: provider does not support %s", ErrProviderPermanent, phase))
			}
			currentPhase := phase
			phase, err = r.advanceSkippedPhase(ctx, input.publicationID, currentPhase)
			if err != nil {
				// Keep the failed phase for the durable PARTIAL checkpoint;
				// never pass the zero next-phase value into phaseFailure.
				return r.phaseFailure(ctx, input.lease, input.publicationID, currentPhase, "CAPABILITY_MISSING", err)
			}
			continue
		}

		if err := r.enterPublicationPhase(ctx, input.publicationID, phase); err != nil {
			return r.phaseFailure(ctx, input.lease, input.publicationID, phase, "STATE_TRANSITION", err)
		}
		remoteIDForOperation := state.RemoteID
		if state.SubmittedRemoteID != "" {
			remoteIDForOperation = state.SubmittedRemoteID
		}
		operation := phaseOperation(phase, input.artifact, input.destination, remoteIDForOperation)

		key, _, err := r.dbStore.BeginPublicationPhaseEffect(ctx, input.publicationID, phase, operation)
		if err != nil {
			return r.phaseFailure(ctx, input.lease, input.publicationID, phase, "PHASE_RESERVATION", err)
		}
		status, err := r.dbStore.GetPublicationPhaseEffectStatus(ctx, input.publicationID, phase, operation)
		if err != nil {
			return r.phaseFailure(ctx, input.lease, input.publicationID, phase, "PHASE_STATUS", err)
		}
		if status == "FAILED" {
			if err := r.dbStore.RetryPublicationPhaseEffect(ctx, input.publicationID, phase, operation); err != nil {
				return r.phaseFailure(ctx, input.lease, input.publicationID, phase, "PHASE_REOPEN", err)
			}
			status = "RUNNING"
		}

		var result *Result
		verificationReplay := status == "SUCCEEDED" && phase == publicationstate.Verifying
		if verificationReplay {
			// A crash after the provider-side verification effect completed
			// but before the durable state transition leaves no in-memory
			// Result. The durable final media ID is the replay evidence.
			if strings.TrimSpace(state.RemoteID) == "" {
				return r.phaseFailure(ctx, input.lease, input.publicationID, phase, operation, fmt.Errorf("%w: completed verification has no durable final media ID", ErrProviderPermanent))
			}
			result = &Result{Success: true, Status: ResultStatusPublished, RemoteID: state.RemoteID, RemoteURL: state.RemoteURL}
		}
		if status != "SUCCEEDED" {
			if phase == publicationstate.Verifying {
				provider, resolveErr := r.registry.Resolve(input.lease.Provider)
				if resolveErr != nil {
					return r.phaseFailure(ctx, input.lease, input.publicationID, phase, operation, resolveErr)
				}
				reconciler, ok := provider.(DeliveryReconciler)
				if !ok {
					return r.phaseFailure(ctx, input.lease, input.publicationID, phase, operation, fmt.Errorf("%w: verification requires DeliveryReconciler authority", ErrProviderPermanent))
				}
				submittedRemoteID := state.SubmittedRemoteID
				if submittedRemoteID == "" {
					submittedRemoteID = state.RemoteID
				}
				result, err = reconciler.Reconcile(deliverCtx, input.lease.DeliveryID, submittedRemoteID)
			} else {
				result, err = executor.ExecutePhase(deliverCtx, phase, &PublicationPhaseContext{
					Artifact: input.artifact, Destination: input.destination,
					CredentialLease: input.credentialLease, PublicationID: input.publicationID,
					DeliveryID: input.lease.DeliveryID, RemoteID: state.RemoteID, RemoteURL: state.RemoteURL,
					SideEffectKey: key,
				})
			}
			if err != nil {
				return r.phaseFailure(ctx, input.lease, input.publicationID, phase, operation, err)
			}
			if result == nil || !result.Success {
				if phase == publicationstate.Verifying && result != nil {
					switch strings.ToUpper(strings.TrimSpace(result.Status)) {
					case ResultStatusSubmittedToProvider, ResultStatusRemoteProcessing, ResultStatusReconciliation:
						return r.phaseFailure(ctx, input.lease, input.publicationID, phase, operation,
							fmt.Errorf("%w: remote publication status %s", ErrProviderTransient, result.Status))
					case "BLOCKED_AUTH":
						return r.phaseFailure(ctx, input.lease, input.publicationID, phase, operation,
							fmt.Errorf("%w: remote publication requires authentication", ErrProviderAuth))
					case "FAILED", "DEAD_LETTER":
						return r.phaseFailure(ctx, input.lease, input.publicationID, phase, operation,
							fmt.Errorf("%w: remote publication failed with status %s", ErrProviderPermanent, result.Status))
					}
				}
				return r.phaseFailure(ctx, input.lease, input.publicationID, phase, operation, fmt.Errorf("%w: phase returned unsuccessful result", ErrProviderPermanent))
			}
		}

		if phase == publicationstate.Uploading {
			if result == nil || strings.TrimSpace(result.RemoteID) == "" {
				return r.phaseFailure(ctx, input.lease, input.publicationID, phase, operation, fmt.Errorf("%w: upload did not return remote_id", ErrProviderPermanent))
			}
			state, err = r.dbStore.PersistPublicationVideoCreated(ctx, input.publicationID, input.artifact.ID, result.RemoteID, result.RemoteURL)
			if err != nil {
				return r.phaseFailure(ctx, input.lease, input.publicationID, phase, operation, err)
			}
		} else {
			// PUBLISHED is an evidence boundary, not merely a successful
			// phase call. The legacy adapter's Reconcile implementation is
			// the only safe source of this result; a provider must not mark
			// verification successful with SUBMITTED/processing semantics.
			if phase == publicationstate.Verifying && (result == nil || result.Status != ResultStatusPublished ||
				strings.TrimSpace(result.RemoteID) == "" || (!verificationReplay && strings.TrimSpace(result.RemoteID) == strings.TrimSpace(state.RemoteID))) {
				return r.phaseFailure(ctx, input.lease, input.publicationID, phase, operation,
					fmt.Errorf("%w: verification requires PUBLISHED with a final media ID distinct from the submitted operation ID", ErrProviderPermanent))
			}
			if result != nil {
				if strings.TrimSpace(result.RemoteID) != "" {
					finalRemoteID = result.RemoteID
				}
				if strings.TrimSpace(result.RemoteURL) != "" {
					finalRemoteURL = result.RemoteURL
				}
			}
			state, err = r.dbStore.GetPublicationState(ctx, input.publicationID)
			if err != nil {
				return r.phaseInfrastructureFailure("PUBLICATION_STATE_READ", err)
			}
			// Persist the provider's final media identity while the durable
			// state is still VERIFYING. If the process exits after this
			// checkpoint but before the VERIFYING -> PUBLISHED transition,
			// replay cannot promote the submission/operation ID as the
			// published media ID.
			if phase == publicationstate.Verifying && finalRemoteID != "" && finalRemoteID != state.RemoteID {
				if err := r.dbStore.RecordPublicationRemoteResult(ctx, input.publicationID, state.Revision, state.RemoteID, finalRemoteID, finalRemoteURL); err != nil {
					return fmt.Errorf("record final publication result: %w", err)
				}
				state.RemoteID = finalRemoteID
				if finalRemoteURL != "" {
					state.RemoteURL = finalRemoteURL
				}
			}
		}
		var phaseCommitErr error
		if phase == publicationstate.Verifying {
			if !verificationReplay {
				phaseCommitErr = r.dbStore.CompletePublicationReconciliationEffect(ctx, input.publicationID, operation)
			}
		} else {
			phaseCommitErr = r.dbStore.CompletePublicationPhaseEffect(ctx, input.publicationID, phase, operation, true, "")
		}
		if phaseCommitErr != nil {
			return r.phaseInfrastructureFailure("PHASE_COMMIT", phaseCommitErr)
		}

		completedPhase := phase
		nextPhase, transitionErr := r.nextPublicationPhase(ctx, input.publicationID, completedPhase, operation)
		if transitionErr != nil {
			return r.phaseFailure(ctx, input.lease, input.publicationID, completedPhase, operation, transitionErr)
		}
		phase = nextPhase
	}

	finalState, err := r.dbStore.GetPublicationState(ctx, input.publicationID)
	if err != nil {
		return r.phaseInfrastructureFailure("PUBLICATION_STATE_READ", err)
	}
	if finalRemoteID == "" {
		finalRemoteID = finalState.RemoteID
	}
	if finalRemoteURL == "" {
		finalRemoteURL = finalState.RemoteURL
	}
	if finalState.State != publicationstate.Published {
		return r.phaseInfrastructureFailure("PUBLICATION_NOT_PUBLISHED", fmt.Errorf("publication ended in %s", finalState.State))
	}
	if err := r.dbStore.MarkDeliverySucceeded(ctx, input.lease.DeliveryID, input.lease.RunnerID, input.lease.LeaseID, finalRemoteID, finalRemoteURL); err != nil {
		return deliveryStatePersistenceError("mark completed delivery succeeded", err)
	}
	return nil
}

func (r *DeliveryRunner) preparePublicationState(ctx context.Context, publicationID string, state *store.PublicationState) (publicationstate.State, error) {
	if state == nil {
		return "", fmt.Errorf("publication state is nil")
	}
	if state.State == publicationstate.RetryWait || state.State == publicationstate.Partial {
		if state.RetryFrom == "" {
			return "", fmt.Errorf("publication retry checkpoint is empty")
		}
		return state.RetryFrom, nil
	}
	if state.State == publicationstate.Pending {
		if _, err := r.dbStore.TransitionPublicationState(ctx, publicationID, publicationstate.WaitingForRender, ""); err != nil {
			return "", err
		}
		state.State = publicationstate.WaitingForRender
	}
	if state.State == publicationstate.WaitingForRender {
		if _, err := r.dbStore.TransitionPublicationState(ctx, publicationID, publicationstate.ArtifactBound, ""); err != nil {
			return "", err
		}
		state.State = publicationstate.ArtifactBound
	}
	if state.State == publicationstate.ArtifactBound {
		if _, err := r.dbStore.TransitionPublicationState(ctx, publicationID, publicationstate.Ready, ""); err != nil {
			return "", err
		}
		state.State = publicationstate.Ready
	}
	switch state.State {
	case publicationstate.Ready, publicationstate.Scheduled, publicationstate.Uploading:
		return publicationstate.Uploading, nil
	case publicationstate.VideoCreated, publicationstate.MetadataApplying:
		return publicationstate.MetadataApplying, nil
	case publicationstate.LocalizationsApplying, publicationstate.Verifying:
		return state.State, nil
	default:
		return "", fmt.Errorf("%w: cannot resume from %s", ErrProviderPermanent, state.State)
	}
}

func (r *DeliveryRunner) enterPublicationPhase(ctx context.Context, publicationID string, phase publicationstate.State) error {
	state, err := r.dbStore.GetPublicationState(ctx, publicationID)
	if err != nil {
		return err
	}
	if state.State == phase {
		return nil
	}
	if state.State == publicationstate.RetryWait || state.State == publicationstate.Partial {
		_, err = r.dbStore.TransitionPublicationState(ctx, publicationID, phase, "")
		return err
	}
	if phase == publicationstate.Uploading && (state.State == publicationstate.Ready || state.State == publicationstate.Scheduled) {
		_, err = r.dbStore.TransitionPublicationState(ctx, publicationID, phase, "")
		return err
	}
	if phase == publicationstate.MetadataApplying && state.State == publicationstate.VideoCreated {
		_, err = r.dbStore.TransitionPublicationState(ctx, publicationID, phase, "")
		return err
	}
	if phase == publicationstate.Verifying && (state.State == publicationstate.MetadataApplying || state.State == publicationstate.LocalizationsApplying) {
		_, err = r.dbStore.TransitionPublicationState(ctx, publicationID, phase, "")
		return err
	}
	return fmt.Errorf("%w: cannot enter %s from %s", ErrProviderPermanent, phase, state.State)
}

func (r *DeliveryRunner) advanceSkippedPhase(ctx context.Context, publicationID string, phase publicationstate.State) (publicationstate.State, error) {
	switch phase {
	case publicationstate.MetadataApplying:
		// Metadata is part of the provider's publication contract. Never
		// skip it: a provider that cannot execute this checkpoint must be
		// rejected rather than allowed to reach verification/published.
		return phase, fmt.Errorf("%w: provider cannot execute required phase %s", ErrProviderPermanent, phase)
	case publicationstate.Verifying:
		// Verification is the evidence boundary for asynchronous or
		// resumable publication. A provider that cannot execute it must
		// fail closed; skipping it would promote an accepted/submitted
		// operation to PUBLISHED without remote proof.
		return publicationstate.Verifying, fmt.Errorf("%w: provider cannot verify remote publication", ErrProviderPermanent)
	default:
		return "", fmt.Errorf("unsupported phase %s", phase)
	}
}

func (r *DeliveryRunner) nextPublicationPhase(ctx context.Context, publicationID string, phase publicationstate.State, verificationOperation string) (publicationstate.State, error) {
	switch phase {
	case publicationstate.Uploading:
		if _, err := r.dbStore.TransitionPublicationState(ctx, publicationID, publicationstate.MetadataApplying, ""); err != nil {
			return "", err
		}
		return publicationstate.MetadataApplying, nil
	case publicationstate.MetadataApplying:
		if _, err := r.dbStore.TransitionPublicationState(ctx, publicationID, publicationstate.Verifying, ""); err != nil {
			return "", err
		}
		return publicationstate.Verifying, nil
	case publicationstate.Verifying:
		if _, err := r.dbStore.CompletePublicationAfterReconciliation(ctx, publicationID, verificationOperation); err != nil {
			return "", err
		}
		return "", nil
	default:
		return "", fmt.Errorf("unsupported completed phase %s", phase)
	}
}

func phaseOperation(phase publicationstate.State, artifact *store.Artifact, destination *Destination, remoteID string) string {
	value := remoteID
	destinationID := ""
	if destination != nil {
		destinationID = destination.DestinationID
	}
	if phase == publicationstate.Uploading && artifact != nil {
		value = artifact.SHA256
	}
	if phase == publicationstate.MetadataApplying && destination != nil {
		value = destination.DeliveryMetadataJSON
	}
	if value == "" {
		value = "empty"
	}
	hash := sha256.Sum256([]byte(destinationID + "\x00" + value))
	name := map[publicationstate.State]string{
		publicationstate.Uploading:             "upload_media",
		publicationstate.MetadataApplying:      "apply_metadata",
		publicationstate.Verifying:             "verify",
		publicationstate.LocalizationsApplying: "apply_localizations",
	}[phase]
	if name == "" {
		name = strings.ToLower(string(phase))
	}
	return name + ":" + hex.EncodeToString(hash[:])
}

func (r *DeliveryRunner) phaseFailure(ctx context.Context, lease store.DeliveryLease, publicationID string, phase publicationstate.State, operation string, runErr error) error {
	code := classifyErrorCode(runErr)
	var persistenceErrors []error
	if operation != "STATE_TRANSITION" && operation != "PUBLICATION_STATE_NOT_FOUND" {
		if err := r.dbStore.CompletePublicationPhaseEffect(ctx, publicationID, phase, operation, false, code); err != nil {
			persistenceErrors = append(persistenceErrors, deliveryStatePersistenceError("complete failed publication phase effect", err))
		}
	}
	if ClassifyError(runErr) == ErrorClassTransient || ClassifyError(runErr) == ErrorClassRateLimit {
		if _, err := r.dbStore.TransitionPublicationState(ctx, publicationID, publicationstate.RetryWait, code); err != nil {
			persistenceErrors = append(persistenceErrors, deliveryStatePersistenceError("transition publication to retry wait", err))
		}
		next := time.Now().UTC().Add(r.cfg.backoffForAttempt(lease.AttemptNumber))
		if retryAfter := r.resolveRetryAfter(runErr); !retryAfter.IsZero() && retryAfter.After(time.Now().UTC()) {
			next = retryAfter
		}
		if err := r.dbStore.MarkDeliveryRetry(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, code, runErr.Error(), next); err != nil {
			persistenceErrors = append(persistenceErrors, deliveryStatePersistenceError("mark delivery retry", err))
		}
		if len(persistenceErrors) > 0 {
			return joinDeliveryErrors(runErr, persistenceErrors...)
		}
		return errDeliveryRetryScheduled
	}
	if phase == publicationstate.Uploading {
		if _, err := r.dbStore.TransitionPublicationState(ctx, publicationID, publicationstate.Failed, code); err != nil {
			persistenceErrors = append(persistenceErrors, deliveryStatePersistenceError("transition publication to failed", err))
		}
	} else {
		if _, err := r.dbStore.TransitionPublicationPartial(ctx, publicationID, phase, code); err != nil {
			persistenceErrors = append(persistenceErrors, deliveryStatePersistenceError("transition publication to partial", err))
		}
	}
	if ClassifyError(runErr) == ErrorClassAuth {
		if err := r.dbStore.MarkDeliveryBlockedAuth(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, code, runErr.Error()); err != nil {
			persistenceErrors = append(persistenceErrors, deliveryStatePersistenceError("mark delivery blocked auth", err))
		}
	} else {
		if err := r.dbStore.MarkDeliveryFailed(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, code, runErr.Error()); err != nil {
			persistenceErrors = append(persistenceErrors, deliveryStatePersistenceError("mark delivery failed", err))
		}
	}
	return joinDeliveryErrors(runErr, persistenceErrors...)
}

func (r *DeliveryRunner) phaseInfrastructureFailure(code string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", code, err)
}

func publicationIDFromMetadata(raw string) string {
	var metadata map[string]any
	if json.Unmarshal([]byte(raw), &metadata) == nil {
		if value, ok := metadata["publication_id"].(string); ok {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

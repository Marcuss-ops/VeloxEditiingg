package deliveries

// runner_process.go: per-lease processing for the DeliveryRunner.
// Split out of runner.go; the lifecycle lives in runner.go, config in
// runner_config.go, pure helpers in runner_helpers.go, credential
// resolution + lease issuance in runner_process_credentials.go, and the
// lease-renewal loop + timeout classification in runner_process_lease.go.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"velox-server/internal/logging"
	"velox-server/internal/publicationstate"
	"velox-server/internal/store"
	"velox-server/internal/telemetry"

	"go.opentelemetry.io/otel/attribute"
)

// processLease resolves the provider for a claimed delivery and runs
// Deliver with a heartbeat goroutine that renews the lease every
// leaseDuration/3. If the renewal fails, the deliver context is
// cancelled to interrupt the upload.
//
// Phase 5.5: per-delivery retry_budget. The lease carries
// MaxAttempts (stamped from job_deliveries.max_attempts at
// claim time, which itself was set from
// job_delivery_plans.retry_budget at INSERT time). The runner
// overrides its runner-wide MaxAttempts on a per-delivery
// basis so a destination with a tighter or looser retry
// budget takes effect without a runner restart. A 0
// MaxAttempts falls back to r.cfg.MaxAttempts (the historical
// behavior).
func (r *DeliveryRunner) processLease(ctx context.Context, lease store.DeliveryLease) error {
	providerName := canonicalProviderName(lease.Provider)
	ctx, span := telemetry.StartSpan(ctx, "deliver_lease",
		attribute.String("velox.provider", providerName),
		attribute.String("velox.delivery_id", lease.DeliveryID),
	)
	defer span.End()

	processStarted := time.Now()
	providerStarted := processStarted
	providerRan := false
	var artifactBytes int64
	status := "failed"
	defer func() {
		if r.telemetry == nil {
			return
		}
		queueMS := 0.0
		if !lease.QueuedAt.IsZero() && processStarted.After(lease.QueuedAt) {
			queueMS = float64(processStarted.Sub(lease.QueuedAt).Microseconds()) / 1000
		}
		uploadMS := 0.0
		if providerRan {
			uploadMS = float64(time.Since(providerStarted).Microseconds()) / 1000
		}
		totalMS := float64(time.Since(processStarted).Microseconds()) / 1000
		r.telemetry.ObserveDelivery(providerName, queueMS, uploadMS, totalMS, status)
		if artifactBytes > 0 && uploadMS > 0 && status == "succeeded" {
			mbps := float64(artifactBytes*8) / uploadMS / 1000
			r.telemetry.RecordDeliveryUpload(providerName, artifactBytes, mbps)
		}
	}()

	// Phase 5.5: per-delivery retry_budget override. The lease
	// carries MaxAttempts from job_deliveries.max_attempts (set
	// from job_delivery_plans.retry_budget at INSERT time). Zero is
	// an explicit no-retry budget; migrated rows use the schema default
	// rather than zero.
	maxAttempts := r.cfg.MaxAttempts
	if lease.MaxAttempts >= 0 {
		maxAttempts = lease.MaxAttempts
	}
	provider, err := r.registry.Resolve(providerName)
	if err != nil {
		// Provider not configured → permanent failure.
		if markErr := r.dbStore.MarkDeliveryFailed(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, "PROVIDER_NOT_CONFIGURED", err.Error()); markErr != nil {
			r.logError(ctx, logging.CodeDeliveryMarkFailed, logging.F("delivery", lease.DeliveryID, "err", markErr))
			return joinDeliveryErrors(err, deliveryStatePersistenceError("mark provider-not-configured failure", markErr))
		}
		return err
	}
	// Validate the asynchronous provider contract before destination
	// hydration, artifact reads, or credential issuance. A reconciler
	// provider without a phase executor has no safe synchronous Success
	// meaning; reject it before allocating any short-lived credential.
	if _, reconciles := provider.(DeliveryReconciler); reconciles {
		if _, registered := r.registry.ResolvePhaseExecutor(providerName); !registered {
			err := fmt.Errorf("%w: provider %q implements DeliveryReconciler but has no phase executor", ErrProviderPermanent, providerName)
			if markErr := r.dbStore.MarkDeliveryFailed(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, "RECONCILIATION_REQUIRED", err.Error()); markErr != nil {
				r.logError(ctx, logging.CodeDeliveryMarkReconcileFail, logging.F("delivery", lease.DeliveryID, "err", markErr))
				return joinDeliveryErrors(err, deliveryStatePersistenceError("mark reconciliation-required failure", markErr))
			}
			return err
		}
	}

	dest, err := r.hydrateDestination(ctx, lease.DestinationID)
	if err != nil {
		// Distinguish DESTINATION_NOT_FOUND (no row) from
		// DESTINATION_UNMAPPED (row exists but external_destination_id is
		// empty — opaque-mode fail-closed contract, see provider.go).
		code := "DESTINATION_NOT_FOUND"
		if errors.Is(err, ErrDestinationUnmapped) {
			code = "DESTINATION_UNMAPPED"
		}
		if mErr := r.dbStore.MarkDeliveryFailed(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, code, err.Error()); mErr != nil {
			r.logError(ctx, logging.CodeDeliveryMarkFailed, logging.F("delivery", lease.DeliveryID, "err", mErr))
			return joinDeliveryErrors(fmt.Errorf("hydrate destination: %w", err), deliveryStatePersistenceError("mark destination hydration failure", mErr))
		}
		return fmt.Errorf("hydrate destination: %w", err)
	}
	if metadata, metadataErr := r.dbStore.GetDeliveryPlanMetadata(ctx, lease.ArtifactID, lease.DestinationID); metadataErr != nil {
		return fmt.Errorf("hydrate delivery metadata: %w", metadataErr)
	} else {
		dest.DeliveryMetadataJSON = metadata
	}

	// Idempotency preflight: a completed publication must not resolve
	// credentials or hydrate the artifact before we discover that there is
	// no provider work left. This is intentionally before credential_ref
	// validation, token lease issuance, and provider dispatch (which may
	// perform account lookup, channel binding, and signed-URL generation).
	// The phase runner keeps its own state check as a defense in depth for
	// callers that enter it directly.
	publicationID := publicationIDFromMetadata(dest.DeliveryMetadataJSON)
	if publicationID == "" {
		resolvedID, lookupErr := r.dbStore.GetPublicationIDForArtifact(ctx, lease.ArtifactID)
		if lookupErr != nil && !errors.Is(lookupErr, store.ErrPublicationStateNotFound) {
			return fmt.Errorf("resolve publication state: %w", lookupErr)
		}
		publicationID = resolvedID
	}
	if publicationID != "" {
		state, stateErr := r.dbStore.GetPublicationState(ctx, publicationID)
		if stateErr != nil && !errors.Is(stateErr, store.ErrPublicationStateNotFound) {
			return fmt.Errorf("read publication state: %w", stateErr)
		}
		if stateErr == nil && state != nil && state.State == publicationstate.Published {
			markErr := r.dbStore.MarkDeliverySucceeded(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, state.RemoteID, state.RemoteURL)
			if markErr == nil {
				status = "succeeded"
			} else {
				return deliveryStatePersistenceError("mark already-published delivery succeeded", markErr)
			}
			return nil
		}
	}

	credentialRef, refErr := resolveCredentialReference(dest.DeliveryMetadataJSON, dest.ConfigurationJSON)
	if refErr != nil {
		if markErr := r.dbStore.MarkDeliveryFailed(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, "CREDENTIAL_REF_INVALID", refErr.Error()); markErr != nil {
			r.logError(ctx, logging.CodeDeliveryCredentialRefFail, logging.F("delivery", lease.DeliveryID, "err", markErr))
			return joinDeliveryErrors(fmt.Errorf("credential reference: %w", refErr), deliveryStatePersistenceError("mark credential-reference failure", markErr))
		}
		return fmt.Errorf("credential reference: %w", refErr)
	}
	dest.CredentialRef = credentialRef
	artifact, err := r.hydrateArtifact(ctx, lease.ArtifactID)
	if err != nil {
		if markErr := r.dbStore.MarkDeliveryFailed(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, "ARTIFACT_NOT_FOUND", err.Error()); markErr != nil {
			r.logError(ctx, logging.CodeDeliveryMarkFailed, logging.F("delivery", lease.DeliveryID, "err", markErr))
			return joinDeliveryErrors(fmt.Errorf("hydrate artifact: %w", err), deliveryStatePersistenceError("mark artifact hydration failure", markErr))
		}
		return fmt.Errorf("hydrate artifact: %w", err)
	}
	artifactBytes = artifact.SizeBytes
	credentialLease, credentialErr := r.issueCredentialLease(ctx, provider, dest, lease)
	if credentialErr != nil {
		if markErr := r.dbStore.MarkDeliveryBlockedAuth(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, credentialErrorCode(credentialErr), credentialErr.Error()); markErr != nil {
			r.logError(ctx, logging.CodeDeliveryCredentialAuthFail, logging.F("delivery", lease.DeliveryID, "err", markErr))
			return joinDeliveryErrors(credentialErr, deliveryStatePersistenceError("mark credential authentication failure", markErr))
		}
		return credentialErr
	}

	// Resumable providers enter the phase executor after all durable inputs and
	// the short-lived credential lease have been resolved. Legacy providers
	// without reconciliation continue through Deliver below and remain
	// non-resumable; their Success contract is synchronous.
	if executor, ok := r.registry.ResolvePhaseExecutor(providerName); ok {
		if publicationID == "" {
			err := fmt.Errorf("%w: publication_id is required for resumable delivery", ErrProviderPermanent)
			if markErr := r.dbStore.MarkDeliveryFailed(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, "PUBLICATION_ID_REQUIRED", err.Error()); markErr != nil {
				return joinDeliveryErrors(err, deliveryStatePersistenceError("mark missing publication id", markErr))
			}
			return err
		}
		providerRan = true
		providerStarted = time.Now()
		phaseErr := r.runPublicationPhases(ctx, publicationPhaseContext{
			lease: lease, publicationID: publicationID, artifact: artifact,
			destination: dest, credentialLease: credentialLease,
		}, executor)
		retryScheduled := errors.Is(phaseErr, errDeliveryRetryScheduled)
		if retryScheduled {
			if r.telemetry != nil {
				r.telemetry.RecordDeliveryRetry(providerName)
			}
			phaseErr = nil
		} else if phaseErr != nil && r.telemetry != nil {
			r.telemetry.RecordDeliveryProviderError(providerName, classifyErrorCode(phaseErr))
			if isDeliveryTimeout(phaseErr) {
				r.telemetry.RecordDeliveryTimeout(providerName)
			}
		}
		if credentialLease != nil && r.vault != nil {
			success := phaseErr == nil && !retryScheduled
			code := ""
			if !success {
				if retryScheduled {
					code = "RETRY_SCHEDULED"
				} else {
					code = classifyErrorCode(phaseErr)
				}
			}
			if auditErr := r.vault.RecordLeaseResult(ctx, credentialLease, success, code); auditErr != nil {
				r.logWarn(ctx, logging.CodeDeliveryCredentialAuditFail, logging.F("delivery", lease.DeliveryID, "err", auditErr))
			}
		}
		if phaseErr == nil && !retryScheduled {
			status = "succeeded"
		}
		return phaseErr
	}

	// Start a heartbeat goroutine to renew the lease periodically while
	// provider.Deliver is executing. If renewal fails (CAS conflict, e.g.
	// another runner reclaimed the lease), cancel the deliver context to
	// interrupt the upload.
	deliverCtx, cancelDeliver := context.WithCancel(ctx)
	defer cancelDeliver()

	heartbeatDone := make(chan struct{})
	go r.renewDeliveryLeaseLoop(deliverCtx, heartbeatDone, lease,
		func(err error) {
			r.logWarn(ctx, logging.CodeDeliveryLeaseRenewalFail, logging.F("delivery", lease.DeliveryID, "err", err))
			cancelDeliver()
		})

	var res *Result
	var runErr error
	providerRan = true
	providerStarted = time.Now()
	if credentialProvider, ok := provider.(CredentialLeaseProvider); ok {
		res, runErr = credentialProvider.DeliverWithCredential(deliverCtx, artifact, dest, lease.DeliveryID, lease.DeliveryID, credentialLease)
	} else {
		res, runErr = provider.Deliver(deliverCtx, artifact, dest, lease.DeliveryID, lease.DeliveryID)
	}

	// Stop the heartbeat goroutine and wait for it to exit.
	cancelDeliver()
	<-heartbeatDone
	if credentialLease != nil && r.vault != nil {
		success := runErr == nil && res != nil && res.Success
		errorCode := ""
		if !success {
			errorCode = classifyErrorCode(runErr)
		}
		if auditErr := r.vault.RecordLeaseResult(ctx, credentialLease, success, errorCode); auditErr != nil {
			r.logWarn(ctx, logging.CodeDeliveryCredentialAuditFail, logging.F("delivery", lease.DeliveryID, "err", auditErr))
		}
	}

	// ── Success ──
	if runErr == nil && res != nil && res.Success {
		// Canonical asynchronous statuses are meaningful only through the
		// DeliveryReconciler/phase path above. A monolithic provider cannot
		// claim SUBMITTED, REMOTE_PROCESSING, RECONCILIATION, or PUBLISHED
		// and then complete a delivery from an operation ID alone.
		switch res.Status {
		case ResultStatusSubmittedToProvider, ResultStatusRemoteProcessing, ResultStatusReconciliation, ResultStatusPublished:
			err := fmt.Errorf("%w: status %q requires reconciler authority", ErrProviderPermanent, res.Status)
			if markErr := r.dbStore.MarkDeliveryFailed(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, "RECONCILIATION_REQUIRED", err.Error()); markErr != nil {
				return joinDeliveryErrors(err, deliveryStatePersistenceError("mark synchronous reconciliation-required failure", markErr))
			}
			return err
		}
		// Validate the provider result carries verifiable evidence.
		// A Success:true without a remote ID or URL is a programming
		// error in the provider adapter — treat as permanent failure.
		if err := validateProviderResult(res); err != nil {
			r.logError(ctx, logging.CodeDeliveryResultValidationFail, logging.F("delivery", lease.DeliveryID, "err", err))
			if merr := r.dbStore.MarkDeliveryFailed(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, "INVALID_RESULT", err.Error()); merr != nil {
				r.logError(ctx, logging.CodeDeliveryMarkFailed, logging.F("delivery", lease.DeliveryID, "err", merr))
			}
			return err
		}
		if err := r.dbStore.MarkDeliverySucceeded(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, res.RemoteID, res.RemoteURL); err != nil {
			return deliveryStatePersistenceError("mark provider delivery succeeded", err)
		}
		status = "succeeded"
		return nil
	}

	// ── Failure: classify + dispatch ──
	errClass := ClassifyError(runErr)
	errCode := classifyErrorCode(runErr)
	if r.telemetry != nil {
		r.telemetry.RecordDeliveryProviderError(providerName, errCode)
		if isDeliveryTimeout(runErr) {
			r.telemetry.RecordDeliveryTimeout(providerName)
		}
	}
	errMsg := ""
	if runErr != nil {
		errMsg = runErr.Error()
	}

	switch errClass {
	case ErrorClassPermanent:
		if err := r.dbStore.MarkDeliveryFailed(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, errCode, errMsg); err != nil {
			r.logError(ctx, logging.CodeDeliveryMarkFailed, logging.F("delivery", lease.DeliveryID, "err", err))
			return joinDeliveryErrors(runErr, deliveryStatePersistenceError("mark permanent provider failure", err))
		}
		return runErr

	case ErrorClassAuth:
		if err := r.dbStore.MarkDeliveryBlockedAuth(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, errCode, errMsg); err != nil {
			r.logError(ctx, logging.CodeDeliveryMarkBlockedAuth, logging.F("delivery", lease.DeliveryID, "err", err))
			return joinDeliveryErrors(runErr, deliveryStatePersistenceError("mark provider authentication failure", err))
		}
		return runErr

	case ErrorClassRateLimit:
		retryAfter := r.resolveRetryAfter(runErr)
		if lease.AttemptNumber >= maxAttempts {
			if err := r.dbStore.MarkDeliveryFailed(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, errCode, "max attempts reached: "+errMsg); err != nil {
				r.logError(ctx, logging.CodeDeliveryMarkFailed, logging.F("delivery", lease.DeliveryID, "err", err))
				return joinDeliveryErrors(fmt.Errorf("max attempts reached: %w", runErr), deliveryStatePersistenceError("mark rate-limit exhaustion", err))
			}
			return fmt.Errorf("max attempts reached: %w", runErr)
		}
		backoff := retryAfter.Sub(time.Now().UTC())
		if backoff <= 0 {
			backoff = r.cfg.backoffForAttempt(lease.AttemptNumber)
		}
		nextAttempt := time.Now().UTC().Add(backoff)
		if err := r.dbStore.MarkDeliveryRetry(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, errCode, errMsg, nextAttempt); err != nil {
			r.logError(ctx, logging.CodeDeliveryMarkRetry, logging.F("delivery", lease.DeliveryID, "err", err))
			return deliveryStatePersistenceError("mark rate-limit retry", err)
		}
		if r.telemetry != nil {
			r.telemetry.RecordDeliveryRetry(providerName)
		}
		return nil

	default: // ErrorClassTransient
		if lease.AttemptNumber >= maxAttempts {
			if err := r.dbStore.MarkDeliveryFailed(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, errCode, "max attempts reached: "+errMsg); err != nil {
				r.logError(ctx, logging.CodeDeliveryMarkFailed, logging.F("delivery", lease.DeliveryID, "err", err))
				return joinDeliveryErrors(fmt.Errorf("max attempts reached: %w", runErr), deliveryStatePersistenceError("mark transient exhaustion", err))
			}
			return fmt.Errorf("max attempts reached: %w", runErr)
		}
		backoff := r.cfg.backoffForAttempt(lease.AttemptNumber)
		nextAttempt := time.Now().UTC().Add(backoff)
		if err := r.dbStore.MarkDeliveryRetry(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, errCode, errMsg, nextAttempt); err != nil {
			r.logError(ctx, logging.CodeDeliveryMarkRetry, logging.F("delivery", lease.DeliveryID, "err", err))
			return deliveryStatePersistenceError("mark transient retry", err)
		}
		if r.telemetry != nil {
			r.telemetry.RecordDeliveryRetry(providerName)
		}
		return nil
	}
}

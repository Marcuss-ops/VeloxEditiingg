package deliveries

// runner_process.go: per-lease processing + lease renewal loop for the
// DeliveryRunner. Split out of runner.go; the lifecycle lives in
// runner.go, config in runner_config.go, helpers in runner_helpers.go.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"velox-server/internal/credentials"
	"velox-server/internal/store"
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
	// Phase 5.5: per-delivery retry_budget override. The lease
	// carries MaxAttempts from job_deliveries.max_attempts (set
	// from job_delivery_plans.retry_budget at INSERT time). A 0
	// falls back to the runner-wide default for back-compat with
	// rows stamped before Phase 5.5.
	maxAttempts := r.cfg.MaxAttempts
	if lease.MaxAttempts > 0 {
		maxAttempts = lease.MaxAttempts
	}
	provider, err := r.registry.Resolve(lease.Provider)
	if err != nil {
		// Provider not configured → permanent failure.
		if err := r.dbStore.MarkDeliveryFailed(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, "PROVIDER_NOT_CONFIGURED", err.Error()); err != nil {
			log.Printf("[DELIVERY] mark failed for %s: %v", lease.DeliveryID, err)
		}
		return err
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
			log.Printf("[DELIVERY] mark failed for %s: %v", lease.DeliveryID, mErr)
		}
		return fmt.Errorf("hydrate destination: %w", err)
	}
	if metadata, metadataErr := r.dbStore.GetDeliveryPlanMetadata(ctx, lease.ArtifactID, lease.DestinationID); metadataErr != nil {
		return fmt.Errorf("hydrate delivery metadata: %w", metadataErr)
	} else {
		dest.DeliveryMetadataJSON = metadata
	}
	credentialRef, refErr := credentials.ReferenceFromJSON(dest.DeliveryMetadataJSON)
	if refErr != nil {
		if markErr := r.dbStore.MarkDeliveryFailed(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, "CREDENTIAL_REF_INVALID", refErr.Error()); markErr != nil {
			log.Printf("[DELIVERY] mark credential reference failure for %s: %v", lease.DeliveryID, markErr)
		}
		return fmt.Errorf("credential reference: %w", refErr)
	}
	dest.CredentialRef = credentialRef
	artifact, err := r.hydrateArtifact(ctx, lease.ArtifactID)
	if err != nil {
		if err := r.dbStore.MarkDeliveryFailed(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, "ARTIFACT_NOT_FOUND", err.Error()); err != nil {
			log.Printf("[DELIVERY] mark failed for %s: %v", lease.DeliveryID, err)
		}
		return fmt.Errorf("hydrate artifact: %w", err)
	}
	credentialLease, credentialErr := r.issueCredentialLease(ctx, provider, dest, lease)
	if credentialErr != nil {
		if markErr := r.dbStore.MarkDeliveryBlockedAuth(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, credentialErrorCode(credentialErr), credentialErr.Error()); markErr != nil {
			log.Printf("[DELIVERY] mark credential auth failure for %s: %v", lease.DeliveryID, markErr)
		}
		return credentialErr
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
			log.Printf("[DELIVERY] lease renewal failed for %s: %v; interrupting upload", lease.DeliveryID, err)
			cancelDeliver()
		})

	var res *Result
	var runErr error
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
			log.Printf("[DELIVERY] credential usage audit failed for %s: %v", lease.DeliveryID, auditErr)
		}
	}

	// ── Success ──
	if runErr == nil && res != nil && res.Success {
		// Validate the provider result carries verifiable evidence.
		// A Success:true without a remote ID or URL is a programming
		// error in the provider adapter — treat as permanent failure.
		if err := validateProviderResult(res); err != nil {
			log.Printf("[DELIVERY] provider result validation failed for %s: %v", lease.DeliveryID, err)
			if merr := r.dbStore.MarkDeliveryFailed(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, "INVALID_RESULT", err.Error()); merr != nil {
				log.Printf("[DELIVERY] mark failed for %s: %v", lease.DeliveryID, merr)
			}
			return err
		}
		if err := r.dbStore.MarkDeliverySucceeded(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, res.RemoteID, res.RemoteURL); err != nil {
			return fmt.Errorf("mark succeeded: %w", err)
		}
		return nil
	}

	// ── Failure: classify + dispatch ──
	errClass := ClassifyError(runErr)
	errCode := classifyErrorCode(runErr)
	errMsg := ""
	if runErr != nil {
		errMsg = runErr.Error()
	}

	switch errClass {
	case ErrorClassPermanent:
		if err := r.dbStore.MarkDeliveryFailed(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, errCode, errMsg); err != nil {
			log.Printf("[DELIVERY] mark failed for %s: %v", lease.DeliveryID, err)
		}
		return runErr

	case ErrorClassAuth:
		if err := r.dbStore.MarkDeliveryBlockedAuth(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, errCode, errMsg); err != nil {
			log.Printf("[DELIVERY] mark blocked_auth for %s: %v", lease.DeliveryID, err)
		}
		return runErr

	case ErrorClassRateLimit:
		retryAfter := r.resolveRetryAfter(runErr)
		if lease.AttemptNumber >= maxAttempts {
			if err := r.dbStore.MarkDeliveryFailed(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, errCode, "max attempts reached: "+errMsg); err != nil {
				log.Printf("[DELIVERY] mark failed for %s: %v", lease.DeliveryID, err)
			}
			return fmt.Errorf("max attempts reached: %w", runErr)
		}
		backoff := retryAfter.Sub(time.Now().UTC())
		if backoff <= 0 {
			backoff = r.cfg.backoffForAttempt(lease.AttemptNumber)
		}
		nextAttempt := time.Now().UTC().Add(backoff)
		if err := r.dbStore.MarkDeliveryRetry(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, errCode, errMsg, nextAttempt); err != nil {
			log.Printf("[DELIVERY] mark retry for %s: %v", lease.DeliveryID, err)
		}
		return nil

	default: // ErrorClassTransient
		if lease.AttemptNumber >= maxAttempts {
			if err := r.dbStore.MarkDeliveryFailed(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, errCode, "max attempts reached: "+errMsg); err != nil {
				log.Printf("[DELIVERY] mark failed for %s: %v", lease.DeliveryID, err)
			}
			return fmt.Errorf("max attempts reached: %w", runErr)
		}
		backoff := r.cfg.backoffForAttempt(lease.AttemptNumber)
		nextAttempt := time.Now().UTC().Add(backoff)
		if err := r.dbStore.MarkDeliveryRetry(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, errCode, errMsg, nextAttempt); err != nil {
			log.Printf("[DELIVERY] mark retry for %s: %v", lease.DeliveryID, err)
		}
		return nil
	}
}

func (r *DeliveryRunner) issueCredentialLease(ctx context.Context, provider Provider, destination *Destination, lease store.DeliveryLease) (*credentials.AccessLease, error) {
	credentialProvider, needsCredential := provider.(CredentialLeaseProvider)
	if !needsCredential {
		return nil, nil
	}
	if destination == nil || destination.CredentialRef == "" {
		return nil, fmt.Errorf("%w: credential_ref is required", ErrProviderAuth)
	}
	if r.vault == nil {
		return nil, fmt.Errorf("%w: credential vault unavailable", ErrProviderAuth)
	}
	scopes := []string{"publish"}
	if scoped, ok := credentialProvider.(CredentialScopeProvider); ok {
		scopes = scoped.RequiredCredentialScopes()
	}
	publicationID := lease.DeliveryID
	var metadata map[string]any
	if err := json.Unmarshal([]byte(destination.DeliveryMetadataJSON), &metadata); err == nil {
		if value, ok := metadata["publication_id"].(string); ok && value != "" {
			publicationID = value
		}
	}
	accessLease, err := r.vault.IssueAccessLease(ctx, destination.CredentialRef, r.identity, publicationID, scopes)
	if err != nil {
		return nil, fmt.Errorf("%w: issue credential lease: %v", ErrProviderAuth, err)
	}
	return accessLease, nil
}

func credentialErrorCode(err error) string {
	if err == nil {
		return "CREDENTIAL_AUTH"
	}
	message := strings.ToLower(err.Error())
	for _, item := range []struct {
		marker string
		code   string
	}{
		{"credential_ref is required", "CREDENTIAL_REF_REQUIRED"},
		{"vault unavailable", "CREDENTIAL_VAULT_UNAVAILABLE"},
		{"credential revoked", "CREDENTIAL_REVOKED"},
		{"credential expired", "CREDENTIAL_EXPIRED"},
		{"scope", "CREDENTIAL_SCOPE_DENIED"},
	} {
		if strings.Contains(message, item.marker) {
			return item.code
		}
	}
	return "CREDENTIAL_AUTH"
}

// renewDeliveryLeaseLoop extends the lease periodically (every
// leaseDuration/3) while provider.Deliver is running. When the deliver
// context is cancelled, the goroutine exits. When a renewal fails (e.g.
// CAS conflict from another runner reclaiming the lease), the onFailure
// callback is invoked so the upload can be interrupted.
func (r *DeliveryRunner) renewDeliveryLeaseLoop(ctx context.Context, done chan<- struct{}, lease store.DeliveryLease, onFailure func(error)) {
	defer close(done)

	interval := r.cfg.LeaseDuration / 3
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			newExpiry := time.Now().UTC().Add(r.cfg.LeaseDuration)
			if err := r.dbStore.RenewDeliveryLease(
				context.Background(), // intentionally detached from request ctx
				lease.DeliveryID, lease.RunnerID, lease.LeaseID, newExpiry,
			); err != nil {
				onFailure(err)
				return
			}
		}
	}
}

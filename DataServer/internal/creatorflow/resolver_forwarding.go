package creatorflow

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"

	"velox-server/internal/forwardingcontract"
	"velox-server/internal/storecore"
	"velox-shared/contract/domain"
)

// PersistPendingRemoteForwarding records an incomplete remote-engine result
// for the durable CreatorForwardingRunner. The operation is idempotent on
// (sourceProvider, sourceJobID, targetExecutorID), so retries of the HTTP
// request or concurrent callers converge on the same forwarding row.
//
// The remote payload is intentionally not stored here: the runner polls the
// remote job by sourceJobID and persists the authoritative completed payload
// through Resolver.Resolve. This keeps the PENDING row small and prevents an
// incomplete response from being mistaken for a forwardable worker payload.
func (r *Resolver) PersistPendingRemoteForwarding(
	ctx context.Context,
	sourceProvider, sourceJobID, targetExecutorID, externalClientID, intakeSource string,
) (*forwardingcontract.CreatorForwarding, error) {
	if r == nil || r.forwardRepo == nil {
		return nil, domain.NewInvalidPayload("resolver", "unavailable", "resolver database access is required")
	}

	sourceProvider = strings.TrimSpace(sourceProvider)
	sourceJobID = strings.TrimSpace(sourceJobID)
	targetExecutorID = strings.TrimSpace(targetExecutorID)
	if sourceProvider == "" || sourceJobID == "" {
		return nil, domain.NewInvalidPayload("source_provider/source_job_id", "required", "source provider and source job id are required")
	}
	if targetExecutorID == "" {
		targetExecutorID = "scene.composite.v1"
	}

	if existing, err := r.forwardRepo.GetCreatorForwardingBySource(ctx, sourceProvider, sourceJobID, targetExecutorID); err != nil {
		if !errors.Is(err, storecore.ErrCreatorForwardingNoRow) {
			return nil, fmt.Errorf("creatorflow: lookup pending forwarding: %w", err)
		}
	} else if existing != nil {
		if clientID := strings.TrimSpace(externalClientID); clientID != "" && strings.TrimSpace(existing.ExternalClientID) != clientID {
			return nil, storecore.ErrCreatorForwardingOwnershipConflict
		}
		return existing, nil
	}

	inserted, err := r.forwardRepo.InsertCreatorForwarding(ctx, &forwardingcontract.CreatorForwarding{
		ForwardingID:     "cf_" + uuid.NewString(),
		ExternalClientID: externalClientID,
		SourceProvider:   sourceProvider,
		SourceJobID:      sourceJobID,
		TargetExecutorID: targetExecutorID,
		IntakeSource:     intakeSource,
		Status:           string(forwardingcontract.CFStatusPending),
	})
	if err != nil {
		return nil, fmt.Errorf("creatorflow: persist pending forwarding: %w", err)
	}
	if inserted == nil || inserted.Forwarding == nil {
		return nil, fmt.Errorf("creatorflow: persist pending forwarding: store returned no forwarding")
	}
	return inserted.Forwarding, nil
}

// ensureReadyForwarding either
//
//	(a) reuses the existing ForwardingID from the request (runner path) and
//	    stamps payload + source_status via the leasable guard, or
//	(b) INSERTs a fresh PENDING row and promotes it to READY_TO_FORWARD via
//	    the leaseless MarkCreatorForwardingReadySync (handler sync path).
//
// If the insert is an idempotent duplicate, the persisted row is authoritative:
// an already READY_TO_FORWARD row is reused without attempting a second
// transition. This is required when the request crashed after the forwarding
// row was committed but before the job enqueue transaction completed.
//
// The payload is JSON-serialized here so both paths pass the same shape
// into the atomic write. A marshal failure is treated as a fatal input
// error (the caller decides whether to surface it to the user).
func (r *Resolver) ensureReadyForwarding(ctx context.Context, req ResolveRequest, targetExecutor string, workerPayload map[string]interface{}) (string, error) {
	payloadJSON, payloadSHA256 := resolverMarshalPayload(workerPayload)
	if payloadJSON == "" && payloadSHA256 == "" {
		return "", fmt.Errorf("creatorflow: Resolve: worker payload is not JSON-serializable")
	}

	// (a) Runner path.
	if req.ForwardingID != "" {
		if err := r.forwardRepo.UpsertCreatorForwardingPayload(ctx, req.ForwardingID, payloadJSON, payloadSHA256); err != nil {
			return "", fmt.Errorf("creatorflow: Resolve upsert payload: %w", err)
		}
		return req.ForwardingID, nil
	}

	// (b) Handler sync path: INSERT PENDING, then promote.
	now := time.Now().UTC().Format(time.RFC3339)
	cf := &forwardingcontract.CreatorForwarding{
		ForwardingID:     "cf_" + uuid.NewString(),
		SourceProvider:   req.SourceProvider,
		SourceJobID:      req.SourceJobID,
		ExternalClientID: req.ExternalClientID,
		TargetExecutorID: targetExecutor,
		PayloadJSON:      payloadJSON,
		PayloadSHA256:    payloadSHA256,
		Status:           string(forwardingcontract.CFStatusPending),
		AttemptCount:     0,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	inserted, err := r.forwardRepo.InsertCreatorForwarding(ctx, cf)
	if err != nil {
		return "", fmt.Errorf("creatorflow: Resolve insert forwarding: %w", err)
	}
	if inserted == nil || inserted.Forwarding == nil || inserted.Forwarding.ForwardingID == "" {
		return "", fmt.Errorf("creatorflow: Resolve: insert returned empty row")
	}

	persisted := inserted.Forwarding
	if !inserted.Created {
		// The unique source key makes this the canonical row for the retry.
		// Never allow a different payload to reuse the same idempotency key.
		if persisted.PayloadSHA256 != "" && persisted.PayloadSHA256 != payloadSHA256 {
			return "", ErrIdempotencyKeyReused
		}

		switch forwardingcontract.CreatorForwardingStatus(persisted.Status) {
		case forwardingcontract.CFStatusReadyToForward:
			log.Printf("[CREATORFLOW] sync handler path: reusing %s already READY_TO_FORWARD (source=%s source_job=%s target_executor=%s)",
				persisted.ForwardingID, req.SourceProvider, req.SourceJobID, targetExecutor)
			return persisted.ForwardingID, nil
		case forwardingcontract.CFStatusPending, forwardingcontract.CFStatusPolling:
			// Continue below and promote the existing row. These are the only
			// non-terminal states accepted by the synchronous promotion CAS.
		default:
			return "", fmt.Errorf("creatorflow: Resolve: existing forwarding %s is in non-promotable status %s",
				persisted.ForwardingID, persisted.Status)
		}
	}

	// Promote PENDING → READY_TO_FORWARD via the leaseless sync method.
	if err := r.forwardRepo.MarkCreatorForwardingReadySync(ctx, persisted.ForwardingID, payloadJSON, payloadSHA256); err != nil {
		return "", fmt.Errorf("creatorflow: Resolve mark READY_TO_FORWARD: %w", err)
	}
	log.Printf("[CREATORFLOW] sync handler path: promoted %s to READY_TO_FORWARD (source=%s source_job=%s target_executor=%s)",
		persisted.ForwardingID, req.SourceProvider, req.SourceJobID, targetExecutor)
	return persisted.ForwardingID, nil
}

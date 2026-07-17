package creatorflow

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"

	"velox-server/internal/store"
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
	sourceProvider, sourceJobID, targetExecutorID string,
) (*store.CreatorForwarding, error) {
	if r == nil || r.dbStore == nil {
		return nil, fmt.Errorf("creatorflow: persist pending forwarding: resolver database access is required")
	}

	sourceProvider = strings.TrimSpace(sourceProvider)
	sourceJobID = strings.TrimSpace(sourceJobID)
	targetExecutorID = strings.TrimSpace(targetExecutorID)
	if sourceProvider == "" || sourceJobID == "" {
		return nil, fmt.Errorf("creatorflow: persist pending forwarding: source provider and source job id are required")
	}
	if targetExecutorID == "" {
		targetExecutorID = "scene.composite.v1"
	}

	if existing, err := r.dbStore.GetCreatorForwardingBySource(ctx, sourceProvider, sourceJobID, targetExecutorID); err != nil {
		if !errors.Is(err, store.ErrCreatorForwardingNoRow) {
			return nil, fmt.Errorf("creatorflow: lookup pending forwarding: %w", err)
		}
	} else if existing != nil {
		return existing, nil
	}

	inserted, err := r.dbStore.InsertCreatorForwarding(ctx, &store.CreatorForwarding{
		ForwardingID:     "cf_" + uuid.NewString(),
		SourceProvider:   sourceProvider,
		SourceJobID:      sourceJobID,
		TargetExecutorID: targetExecutorID,
		Status:           string(store.CFStatusPending),
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
		if err := r.dbStore.UpsertCreatorForwardingPayload(ctx, req.ForwardingID, payloadJSON, payloadSHA256); err != nil {
			return "", fmt.Errorf("creatorflow: Resolve upsert payload: %w", err)
		}
		return req.ForwardingID, nil
	}

	// (b) Handler sync path: INSERT PENDING, then promote.
	now := time.Now().UTC().Format(time.RFC3339)
	cf := &store.CreatorForwarding{
		ForwardingID:     "cf_" + uuid.NewString(),
		SourceProvider:   req.SourceProvider,
		SourceJobID:      req.SourceJobID,
		TargetExecutorID: targetExecutor,
		PayloadJSON:      payloadJSON,
		PayloadSHA256:    payloadSHA256,
		Status:           string(store.CFStatusPending),
		AttemptCount:     0,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	inserted, err := r.dbStore.InsertCreatorForwarding(ctx, cf)
	if err != nil {
		return "", fmt.Errorf("creatorflow: Resolve insert forwarding: %w", err)
	}
	if inserted == nil || inserted.Forwarding == nil || inserted.Forwarding.ForwardingID == "" {
		return "", fmt.Errorf("creatorflow: Resolve: insert returned empty row")
	}

	// Promote PENDING → READY_TO_FORWARD via the leaseless sync method.
	if err := r.dbStore.MarkCreatorForwardingReadySync(ctx, inserted.Forwarding.ForwardingID, payloadJSON, payloadSHA256); err != nil {
		return "", fmt.Errorf("creatorflow: Resolve mark READY_TO_FORWARD: %w", err)
	}
	log.Printf("[CREATORFLOW] sync handler path: promoted %s to READY_TO_FORWARD (source=%s source_job=%s target_executor=%s)",
		inserted.Forwarding.ForwardingID, req.SourceProvider, req.SourceJobID, targetExecutor)
	return inserted.Forwarding.ForwardingID, nil
}

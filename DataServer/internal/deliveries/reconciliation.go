package deliveries

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

func (r *DeliveryRunner) reconcileRecent(ctx context.Context) error {
	if r == nil || r.dbStore == nil || r.registry == nil {
		return nil
	}
	rows, err := r.dbStore.ListDeliveryReconciliationCandidates(ctx, 100)
	if err != nil {
		return err
	}
	for _, row := range rows {
		destination, err := r.hydrateDestination(ctx, row.DestinationID)
		if err != nil {
			continue
		}
		provider, err := r.registry.Resolve(destination.Provider)
		if err != nil {
			continue
		}
		// Providers with a phase executor own reconciliation through VERIFYING;
		// applying a second direct terminal projection here could mark only the
		// delivery row and leave publication state behind. Legacy providers
		// without a phase executor retain this recovery path.
		if _, ok := r.registry.ResolvePhaseExecutor(destination.Provider); ok {
			continue
		}
		reconciler, ok := provider.(DeliveryReconciler)
		if !ok {
			continue
		}
		result, err := reconciler.Reconcile(ctx, row.DeliveryID, row.RemoteID)
		if err != nil {
			if errors.Is(err, ErrProviderAuth) || errors.Is(err, ErrProviderPermanent) {
				continue
			}
			return fmt.Errorf("delivery %s: %w", row.DeliveryID, err)
		}
		if result == nil || result.Status == "" {
			continue
		}
		status := reconciliationStatus(result.Status)
		errCode, errMessage := "", ""
		if meta, ok := result.ProviderMeta["error_code"].(string); ok {
			errCode = meta
		}
		if meta, ok := result.ProviderMeta["error_message"].(string); ok {
			errMessage = meta
		}
		// The SQLite TEXT column stays a plain string; convert the typed
		// status at the store boundary.
		if err := r.dbStore.ApplyReconciledDelivery(ctx, row.DeliveryID, string(status), result.RemoteID, result.RemoteURL, errCode, errMessage); err != nil {
			return err
		}
	}
	return nil
}

// reconciliationStatus maps a provider's remote status observation to the
// canonical delivery lifecycle status. The provider string is untrusted and
// case-insensitive; the returned value is the typed DeliveryStatus so callers
// and persistence compare against the delivery vocabulary instead of bare
// "SUCCEEDED"/"BLOCKED_AUTH"/"RETRY_WAIT" string literals.
func reconciliationStatus(status string) DeliveryStatus {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "published", "completed":
		return DeliverySucceeded
	case "failed", "dead_letter":
		return DeliveryFailed
	case "blocked_auth":
		return DeliveryBlockedAuth
	case "retry_wait", "rate_limited":
		return DeliveryRetryWait
	default:
		return DeliveryRunning
	}
}

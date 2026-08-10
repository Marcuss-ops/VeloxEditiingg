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
		if err := r.dbStore.ApplyReconciledDelivery(ctx, row.DeliveryID, status, result.RemoteID, result.RemoteURL, errCode, errMessage); err != nil {
			return err
		}
	}
	return nil
}

func reconciliationStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "published", "completed":
		return "SUCCEEDED"
	case "failed", "dead_letter":
		return "FAILED"
	case "blocked_auth":
		return "BLOCKED_AUTH"
	case "retry_wait", "rate_limited":
		return "RETRY_WAIT"
	default:
		return "RUNNING"
	}
}

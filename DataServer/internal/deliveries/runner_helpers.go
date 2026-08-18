package deliveries

// runner_helpers.go: small pure helpers for the DeliveryRunner —
// RetryAfter resolution, provider result validation, error-code
// classification and the destination/artifact hydrators. Split out
// of runner.go.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"velox-server/internal/store"
	"velox-shared/contract/domain"
)

// resolveRetryAfter extracts the RetryAfter time from a ProviderError.
// Returns a zero time if the error does not carry RetryAfter.
func (r *DeliveryRunner) resolveRetryAfter(err error) time.Time {
	var pe *ProviderError
	if errors.As(err, &pe) && !pe.RetryAfter.IsZero() {
		return pe.RetryAfter
	}
	return time.Time{}
}

// validateProviderResult checks that a successful provider result carries
// verifiable evidence that the remote side actually created the output.
// A Success:true without at least one of RemoteID or RemoteURL is treated
// as a permanent failure — there is no proof the delivery completed.
func validateProviderResult(res *Result) error {
	if res == nil {
		return fmt.Errorf("%w: result is nil", ErrProviderPermanent)
	}
	if !res.Success {
		return fmt.Errorf("%w: result.Success is false", ErrProviderPermanent)
	}
	if strings.TrimSpace(res.Status) != "" {
		return fmt.Errorf("%w: status %q requires reconciler authority", ErrProviderPermanent, res.Status)
	}
	if res.RemoteID == "" && res.RemoteURL == "" {
		return fmt.Errorf("%w: both RemoteID and RemoteURL are empty after Success=true — no verifiable output", ErrProviderPermanent)
	}
	return nil
}

// validatePublishedProviderResult is reserved for the reconciliation
// authority. It accepts only a positive PUBLISHED observation with final
// evidence; submit/processing statuses must never pass this boundary.
func validatePublishedProviderResult(res *Result) error {
	if res == nil || !res.Success || res.Status != ResultStatusPublished {
		return fmt.Errorf("%w: reconciliation did not produce PUBLISHED", ErrProviderPermanent)
	}
	if res.RemoteID == "" && res.RemoteURL == "" {
		return fmt.Errorf("%w: published reconciliation has no final media evidence", ErrProviderPermanent)
	}
	return nil
}

// classifyErrorCode produces a short machine-readable code for the error.
func classifyErrorCode(err error) string {
	if err == nil {
		return ""
	}
	if code := domain.FailureCodeOf(err); code != "" {
		return code
	}
	if errors.Is(err, ErrProviderNotConfigured) {
		return "PROVIDER_NOT_CONFIGURED"
	}
	if errors.Is(err, ErrProviderPermanent) {
		return "PERMANENT"
	}
	if errors.Is(err, ErrProviderAuth) {
		return "AUTH"
	}
	if errors.Is(err, ErrProviderRateLimit) {
		return "RATE_LIMIT"
	}
	return "TRANSIENT"
}

// hydrateDestination reads delivery_destinations by id and converts the
// internal store type to the deliveries package's Destination shape that
// provider adapters consume.
//
// Opaque-mode fail-closed contract (Residuo 2 of YouTube → Social closure,
// migration 091):
//   - the YouTube-specific fields (AccountID, ChannelID, Language) are gone
//     from the typed Destination;
//   - ExternalDestinationID (canonical, opaque to Velox) is the social_repo
//     identifier resolved server-side; the runner propagates it verbatim;
//   - if ExternalDestinationID is empty / whitespace-only, hydrate MUST
//     fail closed with ErrDestinationUnmapped so the runner records
//     DESTINATION_UNMAPPED on the delivery row (operators backfill).
//
// Residuo 5 (this commit): the deprecated ABI-safe alias for the opaque
// identifier has been removed; the typed Destination struct now carries
// only `ExternalDestinationID` as the opaque identifier.
func (r *DeliveryRunner) hydrateDestination(ctx context.Context, destID string) (*Destination, error) {
	d, err := r.deliveryStore.GetDeliveryDestination(ctx, destID)
	if err != nil {
		return nil, err
	}
	if d == nil {
		return nil, fmt.Errorf("deliveries: destination %s not found", destID)
	}
	providerName := canonicalProviderName(d.Provider)
	if providerName == "social_gateway" && strings.TrimSpace(d.ExternalDestinationID) == "" {
		return nil, fmt.Errorf("deliveries: destination %s: %w", destID, ErrDestinationUnmapped)
	}
	cfg := d.ConfigurationJSON
	if cfg == "" {
		cfg = "{}"
	}
	return &Destination{
		DestinationID:         d.DestinationID,
		Provider:              providerName,
		ExternalDestinationID: d.ExternalDestinationID,
		FolderID:              d.FolderID,
		Name:                  d.Name,
		Enabled:               d.Enabled,
		ConfigurationJSON:     d.ConfigurationJSON,
		Configuration:         []byte(cfg),
	}, nil
}

func canonicalProviderName(provider string) string {
	if strings.EqualFold(strings.TrimSpace(provider), "google_drive") {
		return "drive"
	}
	return strings.TrimSpace(provider)
}

// hydrateArtifact reads artifacts by id.
func (r *DeliveryRunner) hydrateArtifact(ctx context.Context, artID string) (*store.Artifact, error) {
	a, err := r.store.GetArtifact(artID)
	if err != nil {
		return nil, err
	}
	return a, nil
}

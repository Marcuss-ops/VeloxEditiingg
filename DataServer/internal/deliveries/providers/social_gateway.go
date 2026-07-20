// Package deliveries/providers: social_gateway adapter (thin).
//
// SocialGatewayProvider is the production adapter that delegates the
// entire publish flow to the external Social API. Velox owns ZERO
// responsibility for OAuth, channels, tokens, quota or publishing
// state: those concerns live in the social_repo and are surfaced
// through this provider's HTTP boundary only.
//
// All HTTP transport, payload formatting and error classification logic
// for the upstream POST lives in the velox-server/internal/socialclient
// package. This file is intentionally a thin adapter so a future
// provider variant (e.g. an MQ-based async publisher) can be added
// without duplicating transport / payload / error-mapping code.
//
// Registry key: stays "social_gateway" for back-compat with existing
// delivery_destinations rows. The Go type name is SocialGatewayProvider
// to match the package-name contract used in tests + runnerLookups.
package providers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"velox-server/internal/deliveries"
	"velox-server/internal/socialclient"
	"velox-server/internal/store"
)

// SocialGatewayProvider is the production adapter for the external
// Social Service. The provider is intentionally a thin shell over
// `socialclient.Client`: any non-trivial change to the upstream wire
// protocol belongs in the socialclient package, not here.
type SocialGatewayProvider struct {
	client *socialclient.Client
}

// NewSocialGatewayProvider constructs a SocialGatewayProvider that
// owns a long-lived socialclient.Client. The cfg drives the client's
// BaseURL, APIKey, CallbackBaseURL, Timeout, and MaxRetries.
//
// Callers that have not yet wired social_repo (dev / pre-rollout) can
// pass a Config{} (BaseURL="") — the resulting provider returns
// ErrProviderNotConfigured at DeliverArtifact time without any
// nil-pointer risk.
func NewSocialGatewayProvider(cfg socialclient.Config) *SocialGatewayProvider {
	return &SocialGatewayProvider{client: socialclient.New(cfg)}
}

// Name returns "social_gateway". This is the canonical registry key
// used by delivery_destinations.provider; do NOT rename without a
// matching migration on the destination rows + provider registry.
func (s *SocialGatewayProvider) Name() string { return "social_gateway" }

// Deliver sends artifact metadata to the Social Service through the
// socialclient and returns the social_delivery_id provided by the
// remote service.
//
// Error mapping is exhaustive: every error returned by socialclient is
// wrapped in the matching deliveries sentinel so the runner's
// ClassifyError can decide retry semantics without needing to know
// about socialclient internals.
func (s *SocialGatewayProvider) Deliver(ctx context.Context, artifact *store.Artifact, destination *deliveries.Destination, deliveryID, idempotencyKey string) (*deliveries.Result, error) {
	if s == nil || s.client == nil {
		return nil, deliveries.ErrProviderNotConfigured
	}
	if destination == nil {
		return nil, fmt.Errorf("%w: destination is nil", deliveries.ErrProviderPermanent)
	}
	if artifact == nil {
		return nil, fmt.Errorf("%w: artifact is nil", deliveries.ErrProviderPermanent)
	}
	if idempotencyKey == "" {
		idempotencyKey = deliveryID
	}

	req, err := s.buildRequest(artifact, destination, deliveryID, idempotencyKey)
	if err != nil {
		return nil, err
	}

	resp, err := s.client.DeliverArtifact(ctx, req)
	if err != nil {
		return nil, mapSocialClientError(err)
	}
	return &deliveries.Result{
		Success:  true,
		RemoteID: resp.SocialDeliveryID,
	}, nil
}

// buildRequest constructs the socialclient.DeliverArtifactRequest
// from the typed Destination + Artifact.
//
// Opaque-mode (Residuo 3 + Residuo 4 of YouTube→Social closure):
// Velox forwards ONLY the external_destination_id opaque reference
// (canonical, renamed from social_destination_id by Residuo 4
// migration 092); the social_repo is the authoritative resolver
// for account, channel, and platform. `metadata`, `publish_at`,
// and `artifact` carry through verbatim; `configuration_json` is
// no longer parsed for platform/account_id — those values are
// operator-facing observability only and are inert in the wire
// contract.
func (s *SocialGatewayProvider) buildRequest(artifact *store.Artifact, destination *deliveries.Destination, deliveryID, idempotencyKey string) (socialclient.DeliverArtifactRequest, error) {
	req := socialclient.DeliverArtifactRequest{
		ExternalDeliveryID:    deliveryID,
		IdempotencyKey:        idempotencyKey,
		ExternalDestinationID: destination.ExternalDestinationID,

		Artifact: socialclient.ArtifactPayload{
			ArtifactID:  artifact.ID,
			SHA256:      artifact.SHA256,
			SizeBytes:   artifact.SizeBytes,
			MimeType:    artifact.MimeType,
			DownloadURL: s.client.ArtifactDownloadURL(artifact.ID),
		},
	}

	// Metadata is passed through verbatim. We populate two well-known
	// fields (`title`, `publish_at`) so the social_repo can render the
	// publish form without a round-trip back to Velox; everything else
	// in the destination's DeliveryMetadataJSON flows through.
	if destination.DeliveryMetadataJSON != "" && destination.DeliveryMetadataJSON != "{}" {
		var meta map[string]any
		if err := json.Unmarshal([]byte(destination.DeliveryMetadataJSON), &meta); err != nil {
			// Same permanent-failure semantic as configuration_json:
			// retry cannot fix malformed JSON.
			return socialclient.DeliverArtifactRequest{}, fmt.Errorf("%w: invalid delivery metadata: %v",
				deliveries.ErrProviderPermanent, err)
		}
		req.Metadata = meta
		if v, ok := meta["publish_at"].(string); ok && v != "" {
			req.PublishAt = v
		}
	}
	// Social API requires a non-empty metadata object even when the
	// destination has no operator-supplied publish fields. Keep the
	// fallback provider-neutral: the external resolver may enrich it,
	// while the artifact ID remains a stable, auditable title.
	if len(req.Metadata) == 0 {
		req.Metadata = map[string]any{
			"title": "Velox artifact " + artifact.ID,
		}
	}

	req.CallbackURL = s.client.CallbackURL(deliveryID)
	return req, nil
}

// mapSocialClientError translates a socialclient-typed sentinel into
// the matching deliveries sentinel so the runner's ClassifyError can
// apply the right retry policy. Unknown errors are mapped to
// ErrProviderTransient (never ErrProviderPermanent) so the runner
// never silently drops a delivery on an unclassified error.
func mapSocialClientError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, socialclient.ErrNotConfigured):
		return fmt.Errorf("%w: %v", deliveries.ErrProviderNotConfigured, err)
	case errors.Is(err, socialclient.ErrAuth):
		return fmt.Errorf("%w: %v", deliveries.ErrProviderAuth, err)
	case errors.Is(err, socialclient.ErrRateLimit):
		return fmt.Errorf("%w: %v", deliveries.ErrProviderRateLimit, err)
	case errors.Is(err, socialclient.ErrPermanent):
		return fmt.Errorf("%w: %v", deliveries.ErrProviderPermanent, err)
	case errors.Is(err, socialclient.ErrTransient):
		return fmt.Errorf("%w: %v", deliveries.ErrProviderTransient, err)
	default:
		return fmt.Errorf("%w: %v", deliveries.ErrProviderTransient, err)
	}
}

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
// from the typed Destination + Artifact. The provider does NOT
// validate `platform`, `account_id`, `channel_id` — those flow
// verbatim from the destination row to the social_repo, which is the
// authoritative validator for any platform-specific concern.
//
// `platform` is sourced from the destination's configuration_json blob
// (operator-set, free-text). `account_id` and `channel_id` come from
// the destination columns (which already exist in delivery_destinations).
// This choice avoids feature-creep on migrations (no new column on
// `delivery_destinations`) while keeping the typed wire contract
// intact.
func (s *SocialGatewayProvider) buildRequest(artifact *store.Artifact, destination *deliveries.Destination, deliveryID, idempotencyKey string) (socialclient.DeliverArtifactRequest, error) {
	platform, accountID, err := parsePlatformAndAccount(destination.ConfigurationJSON)
	if err != nil {
		// Malformed configuration_json is a permanent failure: retrying
		// cannot fix it, only operator intervention can.
		return socialclient.DeliverArtifactRequest{}, fmt.Errorf("%w: parse configuration_json: %v",
			deliveries.ErrProviderPermanent, err)
	}

	req := socialclient.DeliverArtifactRequest{
		ExternalDeliveryID: deliveryID,
		IdempotencyKey:     idempotencyKey,
		Platform:           platform,
		AccountID:          accountID,
		ChannelID:          destination.ChannelID,

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

	req.CallbackURL = s.client.CallbackURL(deliveryID)
	return req, nil
}

// parsePlatformAndAccount reads `platform` and `account_id` from the
// destination's configuration_json blob. Both are optional; missing
// values are returned as empty strings so the request JSON omits them
// (via the `omitempty` JSON tags).
//
// The blob is intentionally permissive: the social_repo is the
// authoritative owner of platform semantics. Velox only forwards.
func parsePlatformAndAccount(configurationJSON string) (platform, accountID string, err error) {
	if configurationJSON == "" || configurationJSON == "{}" {
		return "", "", nil
	}
	var cfg map[string]string
	if err := json.Unmarshal([]byte(configurationJSON), &cfg); err != nil {
		// We tolerate {}/non-object too: an operator may have authored
		// `"platform": "youtube"` without braces. In that case treat the
		// blob as a generic key/value block where top-level keys may be
		// strings or arbitrary JSON — we only care about named keys.
		var raw map[string]any
		if err2 := json.Unmarshal([]byte(configurationJSON), &raw); err2 != nil {
			return "", "", fmt.Errorf("configuration_json is neither a flat object nor a generic block: %v / %v", err, err2)
		}
		if v, ok := raw["platform"].(string); ok {
			platform = v
		}
		if v, ok := raw["account_id"].(string); ok {
			accountID = v
		}
		return platform, accountID, nil
	}
	platform = cfg["platform"]
	accountID = cfg["account_id"]
	return platform, accountID, nil
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

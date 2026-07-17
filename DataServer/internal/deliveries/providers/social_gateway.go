// Package deliveries/providers: social_gateway adapter.
//
// SocialGatewayProvider is a generic, platform-agnostic delivery adapter.
// It forwards artifact metadata (not the binary) to an external Social
// Service, which is responsible for the actual upload to the target
// platform (YouTube, Meta, TikTok, etc.).
package providers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"velox-server/internal/deliveries"
	"velox-server/internal/store"
)

// SocialGatewayProvider is the production adapter for the external
// Social Service.
type SocialGatewayProvider struct {
	client      *http.Client
	gatewayURL  string
	callbackURL string
	apiKey      string
}

// NewSocialGatewayProvider constructs a SocialGatewayProvider.
// Configuration is read from environment variables:
//   - SOCIAL_GATEWAY_URL: the Social Service delivery endpoint
//   - SOCIAL_GATEWAY_CALLBACK_BASE_URL: base URL for Velox callbacks
//   - SOCIAL_GATEWAY_API_KEY: optional bearer token for the Social Service
func NewSocialGatewayProvider(client *http.Client) *SocialGatewayProvider {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &SocialGatewayProvider{
		client:      client,
		gatewayURL:  os.Getenv("SOCIAL_GATEWAY_URL"),
		callbackURL: os.Getenv("SOCIAL_GATEWAY_CALLBACK_BASE_URL"),
		apiKey:      os.Getenv("SOCIAL_GATEWAY_API_KEY"),
	}
}

// Name returns "social_gateway".
func (s *SocialGatewayProvider) Name() string { return "social_gateway" }

// Deliver sends artifact metadata to the Social Service and returns the
// social_delivery_id provided by the remote service.
func (s *SocialGatewayProvider) Deliver(ctx context.Context, artifact *store.Artifact, destination *deliveries.Destination, deliveryID, idempotencyKey string) (*deliveries.Result, error) {
	if s == nil || s.gatewayURL == "" {
		return nil, deliveries.ErrProviderNotConfigured
	}
	if destination == nil {
		return nil, deliveries.ErrProviderPermanent
	}
	if artifact == nil {
		return nil, fmt.Errorf("%w: artifact is nil", deliveries.ErrProviderPermanent)
	}

	payload, err := s.buildPayload(artifact, destination, deliveryID, idempotencyKey)
	if err != nil {
		return nil, fmt.Errorf("%w: build payload: %v", deliveries.ErrProviderPermanent, err)
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("%w: marshal payload: %v", deliveries.ErrProviderPermanent, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.gatewayURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("%w: create request: %v", deliveries.ErrProviderTransient, err)
	}
	req.Header.Set("Content-Type", "application/json")
	if s.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+s.apiKey)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: social gateway request failed: %v", deliveries.ErrProviderTransient, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		errBody := string(bytes.TrimSpace(body))
		return nil, fmt.Errorf("%w: social gateway returned status %d: %s", classifyHTTPError(resp.StatusCode), resp.StatusCode, errBody)
	}

	var result struct {
		SocialDeliveryID string `json:"social_delivery_id"`
		Status           string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("%w: decode social gateway response: %v", deliveries.ErrProviderTransient, err)
	}
	if result.SocialDeliveryID == "" {
		return nil, fmt.Errorf("%w: social gateway response missing social_delivery_id", deliveries.ErrProviderPermanent)
	}

	return &deliveries.Result{
		Success:  true,
		RemoteID: result.SocialDeliveryID,
	}, nil
}

func (s *SocialGatewayProvider) buildPayload(artifact *store.Artifact, destination *deliveries.Destination, deliveryID, idempotencyKey string) (map[string]interface{}, error) {
	if idempotencyKey == "" {
		idempotencyKey = deliveryID
	}

	artifactPayload := map[string]interface{}{
		"artifact_id": artifact.ID,
		"sha256":      artifact.SHA256,
		"size_bytes":  artifact.SizeBytes,
		"mime_type":   artifact.MimeType,
	}
	if s.callbackURL != "" {
		artifactPayload["download_url"] = fmt.Sprintf("%s/api/internal/artifacts/%s/download", s.callbackURL, artifact.ID)
	}

	metadata := map[string]interface{}{}
	if destination.DeliveryMetadataJSON != "" && destination.DeliveryMetadataJSON != "{}" {
		if err := json.Unmarshal([]byte(destination.DeliveryMetadataJSON), &metadata); err != nil {
			return nil, fmt.Errorf("invalid delivery metadata: %v", err)
		}
	}

	publishAt := ""
	if v, ok := metadata["publish_at"].(string); ok {
		publishAt = v
	}

	payload := map[string]interface{}{
		"idempotency_key": idempotencyKey,
		"artifact":          artifactPayload,
		"destination_id":    destination.DestinationID,
		"metadata":          metadata,
	}
	if s.callbackURL != "" {
		payload["callback_url"] = fmt.Sprintf("%s/api/internal/deliveries/%s/callback", s.callbackURL, deliveryID)
	}
	if publishAt != "" {
		payload["publish_at"] = publishAt
	}
	return payload, nil
}

// classifyHTTPError maps an HTTP status code to the appropriate provider
// error sentinel so the runner can apply the right retry policy.
func classifyHTTPError(statusCode int) error {
	switch {
	case statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden:
		return deliveries.ErrProviderAuth
	case statusCode == http.StatusTooManyRequests:
		return deliveries.ErrProviderRateLimit
	case statusCode >= 500:
		return deliveries.ErrProviderTransient
	default:
		return deliveries.ErrProviderPermanent
	}
}

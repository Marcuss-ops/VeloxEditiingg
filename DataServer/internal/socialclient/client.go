package socialclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// Client is the typed Velox-side boundary against the social_repo.
// Construct it via New(cfg). One Client is safe for concurrent use by
// multiple DeliveryRunner goroutines; the underlying *http.Client is
// goroutine-safe per Go stdlib docs.
type Client struct {
	cfg    Config
	client *http.Client
}

// New builds a Client from a validated Config. If the Config has
// BaseURL == "", New still returns a non-nil Client so the caller can
// decide what to do at DeliverArtifact time (typically: return
// ErrNotConfigured). The empty-BaseURL semantic is the dev/off switch
// the bootstrap can use without nil-pointer guards.
func New(cfg Config) *Client {
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 30 * 1_000_000_000 // 30s in ns; time.Duration units
	}
	return &Client{
		cfg:    cfg,
		client: &http.Client{Timeout: timeout},
	}
}

// BaseURL exposes the configured endpoint (used by callers that need
// to log the destination of an outbound publish).
func (c *Client) BaseURL() string {
	if c == nil {
		return ""
	}
	return c.cfg.BaseURL
}

// DeliverArtifact POSTs the DeliverArtifactRequest to the social_repo
// and parses the response. Single-attempt: the caller (typically the
// deliveries.DeliveryRunner) handles retry via the runner's
// BackoffSchedule. Honors ctx cancellation between request building
// and the HTTP Do call.
//
// Mapping of upstream status codes:
//
//	200/2xx                   → returns (DeliverArtifactResponse, nil)
//	401, 403                  → ErrAuth
//	429                       → ErrRateLimit (Retry-After NOT parsed here)
//	5xx                       → ErrUpstreamTransient
//	other 4xx                 → ErrUpstreamPermanent
//	network/timeout/cancelled → ErrUpstreamTransient
//	missing social_delivery_id → ErrUpstreamPermanent
func (c *Client) DeliverArtifact(ctx context.Context, req DeliverArtifactRequest) (*DeliverArtifactResponse, error) {
	if c == nil {
		return nil, ErrNotConfigured
	}
	if c.cfg.BaseURL == "" {
		return nil, ErrNotConfigured
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("%w: marshal request: %v", ErrPermanent, err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.BaseURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("%w: build request: %v", ErrTransient, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.cfg.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	}

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("%w: social api request failed: %v", ErrTransient, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, classifyStatusError(resp.StatusCode, string(bytes.TrimSpace(respBody)))
	}

	var out DeliverArtifactResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("%w: decode response: %v", ErrTransient, err)
	}
	if out.SocialDeliveryID == "" {
		return nil, fmt.Errorf("%w: response missing social_delivery_id", ErrPermanent)
	}
	return &out, nil
}

// ArtifactDownloadURL builds the canonical Velox-side artifact
// download URL given an artifact ID. Returns empty string when
// CallbackBaseURL is unset (the caller must omit DownloadURL from the
// request in that case so the social_repo knows to download via its
// own alternative).
func (c *Client) ArtifactDownloadURL(artifactID string) string {
	if c == nil || c.cfg.CallbackBaseURL == "" || artifactID == "" {
		return ""
	}
	return c.cfg.CallbackBaseURL + "/api/internal/artifacts/" + artifactID + "/download"
}

// CallbackURL builds the post-publish callback URL. Returns empty when
// CallbackBaseURL is unset.
func (c *Client) CallbackURL(deliveryID string) string {
	if c == nil || c.cfg.CallbackBaseURL == "" || deliveryID == "" {
		return ""
	}
	return c.cfg.CallbackBaseURL + "/api/internal/deliveries/" + deliveryID + "/callback"
}

// classifyStatusError maps an HTTP status code to the canonical error
// sentinel so the SocialGatewayProvider can wrap it with the matching
// deliveries.* sentinel for the runner's retry classification.
func classifyStatusError(status int, body string) error {
	msg := fmt.Sprintf("social api returned status %d: %s", status, body)
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return fmt.Errorf("%w: %s", ErrAuth, msg)
	case status == http.StatusTooManyRequests:
		return fmt.Errorf("%w: %s", ErrRateLimit, msg)
	case status >= 500:
		return fmt.Errorf("%w: %s", ErrTransient, msg)
	default:
		return fmt.Errorf("%w: %s", ErrPermanent, msg)
	}
}

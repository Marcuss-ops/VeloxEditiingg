package socialclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
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

// endpoint joins a path to the configured BaseURL, normalizing any
// trailing slash so callers do not need to know whether BaseURL ends
// in "/" or not. It is the single place where Social API URLs are
// built; all outbound requests methods must use it.
func (c *Client) endpoint(path string) string {
	return strings.TrimRight(c.cfg.BaseURL, "/") + path
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

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint("/internal/v1/deliveries"), bytes.NewReader(body))
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

// ValidateDestination pre-flights a social destination against the
// social_repo's `POST /internal/v1/destinations/:id/validate` endpoint.
// Used by the enqueue-layer validator (jobs/enqueue/delivery_plan_validator.go)
// to delegate destination validation to the social_repo before Velox
// commits a Job + delivery_plan row. Single-attempt; the caller is
// responsible for any soft-fail continuation policy.
//
// Mapping of upstream status codes mirrors DeliverArtifact:
//
//	200/2xx                   → returns (nil)
//	401, 403                  → ErrAuth
//	429                       → ErrRateLimit
//	5xx                       → ErrTransient
//	other 4xx                 → ErrPermanent
//	network/timeout/cancelled → ErrTransient
//	missing BaseURL           → ErrNotConfigured
//	nil receiver              → ErrNotConfigured
//
// The endpoint contract is OWNED by the social_repo: documented as
// `POST {BaseURL}/internal/v1/destinations/{id}/validate`. A 2xx
// response means the destination is routable (channel bound, token
// valid, platform enabled); 4xx means hard-rejectable; 5xx means
// retry-able by the runner.
func (c *Client) ValidateDestination(ctx context.Context, socialDestID string) error {
	if c == nil {
		return ErrNotConfigured
	}
	if c.cfg.BaseURL == "" {
		return ErrNotConfigured
	}
	if socialDestID == "" {
		// Refuse an empty path-segment rather than POST
		// `/internal/v1/destinations//validate`. Empty
		// social_destination_id is treated as a permanent caller error.
		return fmt.Errorf("%w: empty social_destination_id", ErrPermanent)
	}

	endpoint := c.endpoint("/internal/v1/destinations/" + url.PathEscape(socialDestID) + "/validate")
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return fmt.Errorf("%w: build validate request: %v", ErrTransient, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.cfg.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	}

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("%w: validate request failed: %v", ErrTransient, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return classifyStatusError(resp.StatusCode, string(bytes.TrimSpace(respBody)))
	}
	return nil
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

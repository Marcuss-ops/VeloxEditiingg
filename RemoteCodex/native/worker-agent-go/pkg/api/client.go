// Package api provides HTTP client for communicating with the Velox Master server.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/pkg/logger"
)

// Canonical API endpoint paths.
//
// The worker agent operates over a gRPC control plane (see internal/transport).
// The legacy HTTP control endpoints (`/api/workers/{register,unregister,heartbeat,
// commands,ack,status}`, `/api/jobs/{get,result,complete,lease}`) have been
// removed from the master, so the worker no longer hits them. We keep the small
// set of v2 endpoints used by upload helpers, and `/health` for readiness probes.

// Client is an HTTP client for the Velox Master API.
type Client struct {
	baseURL        string
	httpClient     *http.Client
	headers        map[string]string
	retryCount     int
	retryInterval  time.Duration
	circuitBreaker *CircuitBreaker

	// Auth token obtained during registration, sent as Bearer token on subsequent requests.
	// Registration and long-lived pollers run concurrently, so token state is
	// protected independently from the request configuration.
	authMu    sync.RWMutex
	authToken string
	// adminAuthToken is used only for legacy admin-protected artifact upload
	// endpoints. Worker asset reads continue using authToken.
	adminAuthToken string
}

// ClientOption is a functional option for configuring the Client.
type ClientOption func(*Client)

// NewClient creates a new API client with the given base URL.
func NewClient(baseURL string, opts ...ClientOption) *Client {
	c := &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		headers:        make(map[string]string),
		retryCount:     0,
		retryInterval:  5 * time.Second,
		circuitBreaker: NewCircuitBreaker(5, 3, 60*time.Second),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// WithTimeout sets the HTTP client timeout.
func WithTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		if timeout > 0 {
			c.httpClient.Timeout = timeout
		}
	}
}

// WithHeader sets a default header for all requests.
func WithHeader(key, value string) ClientOption {
	return func(c *Client) { c.headers[key] = value }
}

// WithWorkerID sets the X-Worker-ID header for all requests.
func WithWorkerID(workerID string) ClientOption {
	return WithHeader("X-Worker-ID", workerID)
}

// WithRetry enables retry on transient failures.
func WithRetry(count int, interval time.Duration) ClientOption {
	return func(c *Client) {
		if count < 0 {
			count = 0
		}
		c.retryCount = count
		if interval > 0 {
			c.retryInterval = interval
		}
	}
}

// WithCircuitBreaker configures the circuit breaker.
func WithCircuitBreaker(failureThreshold, successThreshold int, timeout time.Duration) ClientOption {
	return func(c *Client) { c.circuitBreaker = NewCircuitBreaker(failureThreshold, successThreshold, timeout) }
}

// SetAuthToken sets the bearer token for authenticated requests.
// This token is obtained from the registration response and sent as
// "Authorization: Bearer <token>" on all subsequent API calls.
func (c *Client) SetAuthToken(token string) {
	c.authMu.Lock()
	c.authToken = strings.TrimSpace(token)
	c.authMu.Unlock()
}

// AuthToken returns the current auth token, if any.
func (c *Client) AuthToken() string {
	c.authMu.RLock()
	defer c.authMu.RUnlock()
	return c.authToken
}

// ClearAuthToken invalidates the worker session token when the control
// session is disconnected. A reconnect must install a fresh credential
// before protected-assets polling can resume.
func (c *Client) ClearAuthToken() {
	c.authMu.Lock()
	c.authToken = ""
	c.authMu.Unlock()
}

// SetAdminAuthToken configures the operator token used by artifact upload
// endpoints while preserving the worker session token for asset reads.
func (c *Client) SetAdminAuthToken(token string) {
	c.authMu.Lock()
	c.adminAuthToken = strings.TrimSpace(token)
	c.authMu.Unlock()
}

// isRetryableError is retained as the package-local test seam while the
// classification itself lives in the shared downloader vocabulary.
func isRetryableError(err error) bool {
	return downloader.IsRetryableError(err)
}

func (c *Client) doRequest(ctx context.Context, method, path string, body interface{}) ([]byte, error) {
	if !c.circuitBreaker.CanExecute() {
		logger.Warn("[CIRCUIT_BREAKER] Request rejected - circuit is open (endpoint: %s)", path)
		return nil, fmt.Errorf("circuit breaker is open - master unavailable")
	}

	backoffs := downloader.BackoffSchedule(c.retryCount+1, c.retryInterval, downloader.DefaultJitter)
	var responseBody []byte
	var lastErr error
	var successfulAttempt int
	err := downloader.Retry(ctx, downloader.RetryConfig{
		MaxAttempts: c.retryCount + 1,
		Backoff:     backoffs,
		BeforeWait: func(attempt int, err error, delay time.Duration) {
			logger.Warn("[%s] Retrying request (attempt %d/%d, endpoint: %s, backoff: %v, error: %v)",
				EventAPIRetry, attempt, c.retryCount, path, delay.Round(time.Millisecond), err)
		},
	}, func(attempt int) downloader.AttemptResult {
		responseBody, lastErr = c.doSingleRequest(ctx, method, path, body)
		if lastErr == nil {
			c.circuitBreaker.RecordSuccess()
			successfulAttempt = attempt
			return downloader.AttemptResult{}
		}
		c.circuitBreaker.RecordFailure()
		logger.Debug("[%s] Request failed (endpoint: %s, error: %v, circuit: %s)",
			EventAPIError, path, lastErr, c.circuitBreaker.GetState())
		return downloader.AttemptResult{
			Err:        lastErr,
			Retry:      isRetryableError(lastErr),
			RetryAfter: downloader.RetryAfterError(lastErr),
		}
	})
	if err != nil {
		logger.Error("[%s] Request failed after %d retries (endpoint: %s, error: %v)",
			EventAPIError, c.retryCount, path, err)
		return nil, fmt.Errorf("request failed after %d retries: %w", c.retryCount, err)
	}
	if successfulAttempt > 0 {
		logger.Info("[%s] Request succeeded after %d retries (endpoint: %s)",
			EventAPISuccess, successfulAttempt, path)
	}
	return responseBody, nil
}

func (c *Client) doSingleRequest(ctx context.Context, method, path string, body interface{}) ([]byte, error) {
	var reqBody io.Reader
	if body != nil {
		jsonBody, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
		reqBody = bytes.NewReader(jsonBody)
	}

	// Parse the path to separate base path from query parameters.
	// url.JoinPath(c.baseURL, path) would URL-encode '?' as '%3F', breaking query params.
	// Instead, use url.Parse on the full URL.
	base, err := url.Parse(c.baseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse base URL: %w", err)
	}
	rel, err := url.Parse(path)
	if err != nil {
		return nil, fmt.Errorf("failed to parse request path: %w", err)
	}
	fullURL := base.ResolveReference(rel).String()

	req, err := http.NewRequestWithContext(ctx, method, fullURL, reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Add auth token as Bearer token if available. Read it through the
	// same synchronization used by registration and poller readiness.
	if authToken := c.AuthToken(); authToken != "" {
		req.Header.Set("Authorization", "Bearer "+authToken)
	}

	for key, value := range c.headers {
		req.Header.Set(key, value)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}
	if resp.StatusCode >= 400 {
		return nil, downloader.NewHTTPStatusError(resp.StatusCode, string(respBody), downloader.RetryAfter(resp))
	}
	return respBody, nil
}

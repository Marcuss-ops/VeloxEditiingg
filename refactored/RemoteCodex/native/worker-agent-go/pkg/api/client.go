// Package api provides HTTP client for communicating with the Velox Master server.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"syscall"
	"time"

	"velox-worker-agent/pkg/logger"
)

// Canonical API endpoint paths.
const (
	endpointRegisterWorker   = "/api/workers/register"
	endpointUnregisterWorker = "/api/workers/unregister"
	endpointHeartbeat        = "/api/workers/heartbeat"
	endpointGetJob           = "/api/jobs/get"
	endpointSubmitResult     = "/api/jobs/result"
	endpointCompleteJob      = "/api/jobs/complete"
	endpointRenewLease       = "/api/jobs/lease"
	endpointHealthCheck      = "/health"
	endpointGetCommands      = "/api/workers/commands"
	endpointAckCommand       = "/api/workers/commands/ack"
	endpointAckCommandByID   = "/api/workers/commands/ack"
	endpointUpdateStatus     = "/api/workers/status"

	// V2 canonical endpoints
	endpointV2GetJob      = "/api/v1/queue/job"
	endpointV2SubmitResult = "/api/v1/jobs/%s/result"
	endpointV2CompleteJob = "/api/v1/jobs/%s/complete"
	endpointV2RenewLease  = "/api/v1/jobs/%s/lease"
)

// Client is an HTTP client for the Velox Master API.
type Client struct {
	baseURL        string
	httpClient     *http.Client
	headers        map[string]string
	retryCount     int
	retryInterval  time.Duration
	circuitBreaker *CircuitBreaker

	// Auth token obtained during registration, sent as Bearer token on subsequent requests.
	authToken string
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
	return func(c *Client) { c.httpClient.Timeout = timeout }
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
	return func(c *Client) { c.retryCount = count; c.retryInterval = interval }
}

// WithCircuitBreaker configures the circuit breaker.
func WithCircuitBreaker(failureThreshold, successThreshold int, timeout time.Duration) ClientOption {
	return func(c *Client) { c.circuitBreaker = NewCircuitBreaker(failureThreshold, successThreshold, timeout) }
}

// SetAuthToken sets the bearer token for authenticated requests.
// This token is obtained from the registration response and sent as
// "Authorization: Bearer <token>" on all subsequent API calls.
func (c *Client) SetAuthToken(token string) {
	c.authToken = token
}

// AuthToken returns the current auth token, if any.
func (c *Client) AuthToken() string {
	return c.authToken
}

func retryBackoff(attempt int, baseInterval time.Duration) time.Duration {
	backoff := float64(baseInterval) * math.Pow(2, float64(attempt))
	maxBackoff := 5 * time.Minute
	if backoff > float64(maxBackoff) {
		backoff = float64(maxBackoff)
	}
	minJitter := 100 * time.Millisecond
	jitter := rand.Float64() * backoff
	if jitter < float64(minJitter) {
		jitter = float64(minJitter)
	}
	return time.Duration(jitter)
}

func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var sysErr *os.SyscallError
	if errors.As(err, &sysErr) {
		switch sysErr.Err {
		case syscall.ECONNREFUSED, syscall.ECONNRESET, syscall.ETIMEDOUT, syscall.EHOSTUNREACH:
			return true
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	errStr := err.Error()
	if strings.Contains(errStr, "status 5") || strings.Contains(errStr, "status 429") {
		return true
	}
	if strings.Contains(errStr, "status 4") {
		return false
	}
	return false
}

func (c *Client) doRequest(ctx context.Context, method, path string, body interface{}) ([]byte, error) {
	if !c.circuitBreaker.CanExecute() {
		logger.Warn("[CIRCUIT_BREAKER] Request rejected - circuit is open (endpoint: %s)", path)
		return nil, fmt.Errorf("circuit breaker is open - master unavailable")
	}

	var lastErr error
	for attempt := 0; attempt <= c.retryCount; attempt++ {
		if attempt > 0 {
			backoff := retryBackoff(attempt-1, c.retryInterval)
			logger.Warn("[%s] Retrying request (attempt %d/%d, endpoint: %s, backoff: %v, error: %v)",
				EventAPIRetry, attempt, c.retryCount, path, backoff.Round(time.Millisecond), lastErr)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}
		respBody, err := c.doSingleRequest(ctx, method, path, body)
		if err == nil {
			c.circuitBreaker.RecordSuccess()
			if attempt > 0 {
				logger.Info("[%s] Request succeeded after %d retries (endpoint: %s)",
					EventAPISuccess, attempt, path)
			}
			return respBody, nil
		}
		lastErr = err
		c.circuitBreaker.RecordFailure()
		logger.Debug("[%s] Request failed (endpoint: %s, error: %v, circuit: %s)",
			EventAPIError, path, err, c.circuitBreaker.GetState())
		if ctx.Err() != nil || !isRetryableError(err) {
			return nil, err
		}
	}
	logger.Error("[%s] Request failed after %d retries (endpoint: %s, error: %v)",
		EventAPIError, c.retryCount, path, lastErr)
	return nil, fmt.Errorf("request failed after %d retries: %w", c.retryCount, lastErr)
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

	// Add auth token as Bearer token if available
	if c.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.authToken)
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
		return nil, fmt.Errorf("API error (status %d): %s", resp.StatusCode, string(respBody))
	}
	return respBody, nil
}

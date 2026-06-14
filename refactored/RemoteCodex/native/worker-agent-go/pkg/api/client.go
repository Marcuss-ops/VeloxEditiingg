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

// Client is an HTTP client for the Velox Master API.
type Client struct {
	baseURL        string
	httpClient     *http.Client
	headers        map[string]string
	retryCount     int
	retryInterval  time.Duration
	adapter        *EndpointAdapter
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
		adapter:        NewEndpointAdapter(),
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

// registerResponse is used to parse token and other fields from registration response.
type registerResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
	Token   string `json:"token,omitempty"`
	Error   string `json:"error,omitempty"`
}

// RegisterWorker registers this worker with the master server.
// If the server returns a token, it is stored and used for subsequent authenticated requests.
func (c *Client) RegisterWorker(ctx context.Context, info *WorkerInfo) error {
	respBody, err := c.doRequest(ctx, "POST", c.adapter.RegisterWorker(), info)
	if err != nil {
		return err
	}

	// Parse response to extract optional auth token
	var resp registerResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		// Response body isn't valid JSON or doesn't have the expected shape;
		// this is not fatal — registration succeeded (we got 200).
		// Just log and continue without a token.
		logger.Debug("[%s] Could not parse registration response for token: %v", EventAPIRequest, err)
		return nil
	}

	if resp.Token != "" {
		c.authToken = resp.Token
		logger.Debug("[%s] Auth token received and stored (length: %d)", EventAPIRequest, len(resp.Token))
	}

	return nil
}

// UnregisterWorker unregisters this worker from the master server.
func (c *Client) UnregisterWorker(ctx context.Context, workerID string) error {
	_, err := c.doRequest(ctx, "POST", c.adapter.UnregisterWorker(), map[string]string{"worker_id": workerID})
	return err
}

// GetJob fetches the next available job from the master.
func (c *Client) GetJob(ctx context.Context, workerID string) (*Job, error) {
	respBody, err := c.doRequest(ctx, "POST", c.adapter.GetJob(), &JobRequest{WorkerID: workerID})
	if err != nil {
		return nil, err
	}
	var apiResp APIResponse
	if err := json.Unmarshal(respBody, &apiResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	if !apiResp.Success {
		return nil, nil
	}
	jobData, err := json.Marshal(apiResp.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal job data: %w", err)
	}
	var job Job
	if err := json.Unmarshal(jobData, &job); err != nil {
		return nil, fmt.Errorf("failed to parse job: %w", err)
	}
	return &job, nil
}

// SubmitJobResult submits the result of a completed job.
func (c *Client) SubmitJobResult(ctx context.Context, result *JobResult) error {
	_, err := c.doRequest(ctx, "POST", c.adapter.SubmitResult(), result)
	return err
}

// CompleteJob notifies the master that a job has completed successfully.
func (c *Client) CompleteJob(ctx context.Context, jobID, workerID string) error {
	_, err := c.doRequest(ctx, "POST", c.adapter.CompleteJob(), map[string]string{
		"job_id":    jobID,
		"worker_id": workerID,
	})
	return err
}

// SendHeartbeat sends a heartbeat to the master server.
func (c *Client) SendHeartbeat(ctx context.Context, payload *HeartbeatPayload) error {
	_, err := c.doRequest(ctx, "POST", c.adapter.Heartbeat(), payload)
	return err
}

// HealthCheck checks if the master server is healthy.
func (c *Client) HealthCheck(ctx context.Context) error {
	_, err := c.doRequest(ctx, "GET", c.adapter.HealthCheck(), nil)
	return err
}

// GetCommands fetches pending commands for this worker from the master.
func (c *Client) GetCommands(ctx context.Context, workerID string) ([]WorkerCommand, error) {
	respBody, err := c.doRequest(ctx, "GET", c.adapter.GetCommands()+"?worker_id="+url.QueryEscape(workerID), nil)
	if err != nil {
		return nil, err
	}
	var apiResp APIResponse
	if err := json.Unmarshal(respBody, &apiResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	if !apiResp.Success {
		return nil, nil
	}
	commandsData, err := json.Marshal(apiResp.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal commands data: %w", err)
	}
	var commands []WorkerCommand
	if err := json.Unmarshal(commandsData, &commands); err != nil {
		var singleCmd WorkerCommand
		if err := json.Unmarshal(commandsData, &singleCmd); err != nil {
			return nil, fmt.Errorf("failed to parse commands: %w", err)
		}
		commands = []WorkerCommand{singleCmd}
	}
	return commands, nil
}

// AckCommand acknowledges a command has been processed.
func (c *Client) AckCommand(ctx context.Context, workerID, command string) error {
	_, err := c.doRequest(ctx, "POST", c.adapter.AckCommand(), map[string]string{
		"worker_id": workerID, "command": command,
	})
	return err
}

// UpdateStatus sends a status update to the master (for command responses).
func (c *Client) UpdateStatus(ctx context.Context, workerID, status string, details map[string]interface{}) error {
	_, err := c.doRequest(ctx, "POST", c.adapter.UpdateStatus(), map[string]interface{}{
		"worker_id": workerID, "status": status, "details": details,
	})
	return err
}

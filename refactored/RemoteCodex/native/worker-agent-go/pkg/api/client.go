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

// API event names for structured logging.
const (
	EventAPIRequest  = "API_REQUEST"
	EventAPIRetry    = "API_RETRY"
	EventAPIError    = "API_ERROR"
	EventAPISuccess  = "API_SUCCESS"
	EventAPIFallback = "API_FALLBACK"
)

// Client is an HTTP client for the Velox Master API.
type Client struct {
	baseURL    string
	httpClient *http.Client
	headers    map[string]string
	// Retry configuration
	retryCount    int
	retryInterval time.Duration
	// Endpoint adapter for API version support
	adapter *EndpointAdapter
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
		headers:       make(map[string]string),
		retryCount:    0, // Default: no retry
		retryInterval: 5 * time.Second,
		adapter:       NewEndpointAdapter(),
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// WithTimeout sets the HTTP client timeout.
func WithTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.httpClient.Timeout = timeout
	}
}

// WithHeader sets a default header for all requests.
func WithHeader(key, value string) ClientOption {
	return func(c *Client) {
		c.headers[key] = value
	}
}

// WithWorkerID sets the X-Worker-ID header for all requests.
func WithWorkerID(workerID string) ClientOption {
	return WithHeader("X-Worker-ID", workerID)
}

// WithRetry enables retry on transient failures.
func WithRetry(count int, interval time.Duration) ClientOption {
	return func(c *Client) {
		c.retryCount = count
		c.retryInterval = interval
	}
}

// WorkerInfo represents worker identification sent to the master.
type WorkerInfo struct {
	WorkerID     string          `json:"worker_id"`
	WorkerName   string          `json:"worker_name"`
	Capabilities map[string]bool `json:"capabilities"`
	Hostname     string          `json:"hostname"`
	IP           string          `json:"ip"`
	Version      string          `json:"version"`
}

// JobRequest represents a request to get a job from the master.
type JobRequest struct {
	WorkerID string `json:"worker_id"`
}

// Job represents a job returned by the master.
type Job struct {
	JobID       string                 `json:"job_id"`
	JobRunID    string                 `json:"job_run_id"` // REQUIRED: lifecycle enforcement
	JobType     string                 `json:"job_type"`
	Priority    int                    `json:"priority"`
	Parameters  map[string]interface{} `json:"parameters"`
	CreatedAt   string                 `json:"created_at"`
	TimeoutSecs int                    `json:"timeout_secs"`
}

// JobResult represents the result of a job execution.
type JobResult struct {
	JobID     string                 `json:"job_id"`
	JobRunID  string                 `json:"job_run_id"` // REQUIRED: lifecycle enforcement
	WorkerID  string                 `json:"worker_id"`
	Status    string                 `json:"status"` // success, failed, timeout
	Output    map[string]interface{} `json:"output"`
	Error     string                 `json:"error,omitempty"`
	StartTime string                 `json:"start_time"`
	EndTime   string                 `json:"end_time"`
}

// HeartbeatPayload represents a heartbeat message.
type HeartbeatPayload struct {
	WorkerID   string                 `json:"worker_id"`
	WorkerName string                 `json:"worker_name,omitempty"`
	Status     string                 `json:"status"` // idle, busy, error
	JobID      string                 `json:"job_id,omitempty"`
	CurrentJob string                 `json:"current_job,omitempty"`
	Extra      map[string]interface{} `json:"extra,omitempty"`
}

// APIResponse represents a generic API response.
type APIResponse struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// retryBackoff calculates exponential backoff with jitter.
// Returns the duration to wait before the next retry attempt.
func retryBackoff(attempt int, baseInterval time.Duration) time.Duration {
	// Exponential backoff: base * 2^attempt
	backoff := float64(baseInterval) * math.Pow(2, float64(attempt))

	// Cap at 5 minutes
	maxBackoff := 5 * time.Minute
	if backoff > float64(maxBackoff) {
		backoff = float64(maxBackoff)
	}

	// Add jitter: random between 100ms and backoff
	// Ensures minimum jitter to prevent zero-delay retries
	minJitter := 100 * time.Millisecond
	jitter := rand.Float64() * backoff
	if jitter < float64(minJitter) {
		jitter = float64(minJitter)
	}

	return time.Duration(jitter)
}

// isRetryableError determines if an error is transient and worth retrying.
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Check for network errors (timeout, connection refused, etc.)
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	// Check for specific syscall errors
	var sysErr *os.SyscallError
	if errors.As(err, &sysErr) {
		// Retry on connection refused, reset, timeout
		switch sysErr.Err {
		case syscall.ECONNREFUSED, syscall.ECONNRESET, syscall.ETIMEDOUT, syscall.EHOSTUNREACH:
			return true
		}
	}

	// Context errors are not retryable
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	// Check for 5xx errors (these are already classified in doSingleRequest)
	errStr := err.Error()
	if strings.Contains(errStr, "status 5") || strings.Contains(errStr, "status 429") {
		return true // 5xx or 429 (rate limit) are retryable
	}

	// 4xx errors are generally not retryable (client errors)
	if strings.Contains(errStr, "status 4") {
		return false
	}

	// Default: don't retry unknown errors
	return false
}

// doRequest performs an HTTP request with optional retry support using exponential backoff.
func (c *Client) doRequest(ctx context.Context, method, path string, body interface{}) ([]byte, error) {
	var lastErr error

	for attempt := 0; attempt <= c.retryCount; attempt++ {
		if attempt > 0 {
			// Calculate backoff with exponential + jitter
			backoff := retryBackoff(attempt-1, c.retryInterval)

			logger.Warn("[%s] Retrying request after failure (attempt %d/%d, endpoint: %s, api_mode: %s, backoff: %v, error: %v)",
				EventAPIRetry, attempt, c.retryCount, path, "new_api", backoff.Round(time.Millisecond), lastErr)

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
				// Continue with retry
			}
		}

		respBody, err := c.doSingleRequest(ctx, method, path, body)
		if err == nil {
			if attempt > 0 {
				logger.Info("[%s] Request succeeded after %d retries (endpoint: %s, api_mode: %s)",
					EventAPISuccess, attempt, path, "new_api")
			}
			return respBody, nil
		}

		lastErr = err

		// Log API error with structured format
		logger.Debug("[%s] Request failed (endpoint: %s, api_mode: %s, error: %v)",
			EventAPIError, path, "new_api", err)

		// Don't retry if context is cancelled or error is not retryable
		if ctx.Err() != nil || !isRetryableError(err) {
			return nil, err
		}
	}

	logger.Error("[%s] Request failed after %d retries (endpoint: %s, api_mode: %s, error: %v)",
		EventAPIError, c.retryCount, path, "new_api", lastErr)
	return nil, fmt.Errorf("request failed after %d retries: %w", c.retryCount, lastErr)
}

// doSingleRequest performs a single HTTP request without retry.
func (c *Client) doSingleRequest(ctx context.Context, method, path string, body interface{}) ([]byte, error) {
	var reqBody io.Reader
	if body != nil {
		jsonBody, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
		reqBody = bytes.NewReader(jsonBody)
	}

	fullURL, err := url.JoinPath(c.baseURL, path)
	if err != nil {
		return nil, fmt.Errorf("failed to join URL path: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, method, fullURL, reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
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

// RegisterWorker registers this worker with the master server.
func (c *Client) RegisterWorker(ctx context.Context, info *WorkerInfo) error {
	_, err := c.doRequest(ctx, "POST", c.adapter.RegisterWorker(), info)
	return err
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
		// No job available or error
		return nil, nil
	}

	// Parse the job data
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

// WorkerCommand represents a command from the master to the worker.
type WorkerCommand struct {
	Command   string                 `json:"command"`
	Timestamp string                 `json:"timestamp"`
	Payload   map[string]interface{} `json:"payload,omitempty"`
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

	// Parse commands
	commandsData, err := json.Marshal(apiResp.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal commands data: %w", err)
	}

	var commands []WorkerCommand
	if err := json.Unmarshal(commandsData, &commands); err != nil {
		// Try single command
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
		"worker_id": workerID,
		"command":   command,
	})
	return err
}

// UpdateStatus sends a status update to the master (for command responses).
func (c *Client) UpdateStatus(ctx context.Context, workerID, status string, details map[string]interface{}) error {
	_, err := c.doRequest(ctx, "POST", c.adapter.UpdateStatus(), map[string]interface{}{
		"worker_id": workerID,
		"status":    status,
		"details":   details,
	})
	return err
}

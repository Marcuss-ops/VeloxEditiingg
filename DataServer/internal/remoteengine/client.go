// Package remoteengine is the adapter to the external script/pipeline
// generation service.
//
// Area 2 — Rigorous adapter contract:
//   - Every HTTP failure, network timeout, and malformed response is
//     wrapped into a *RemoteError so callers can branch on Class without
//     string-matching.
//   - The retry loops use IsRetryable() / IsPermanent() instead of
//     strings.Contains(err.Error(), "4").
//   - StartPipeline sends an Idempotency-Key header so a timeout after
//     remote job creation does not produce a duplicate.

package remoteengine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// DefaultConfig returns config from environment.
func DefaultConfig() Config {
	timeoutMS := 60000 // default 60s
	if v := os.Getenv("VELOX_REMOTE_ENGINE_TIMEOUT_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			timeoutMS = n
		}
	}
	retries := 3
	if v := os.Getenv("VELOX_REMOTE_ENGINE_RETRIES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			retries = n
		}
	}
	return Config{
		URL:       os.Getenv("VELOX_REMOTE_ENGINE_URL"),
		Token:     os.Getenv("VELOX_REMOTE_ENGINE_TOKEN"),
		TimeoutMS: timeoutMS,
		Retries:   retries,
	}
}

// NewClient creates a new remote engine client.
func NewClient(cfg Config) *Client {
	timeout := time.Duration(cfg.TimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	return &Client{
		config: cfg,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

// IsConfigured returns true if remote engine is configured.
func (c *Client) IsConfigured() bool {
	return c.config.URL != ""
}

// ── Shared retry helper ──────────────────────────────────────────────────────

// withRetry executes fn up to MaxRetries times. The retry policy is:
//
//   - VALIDATION, AUTHENTICATION, PERMANENT → break immediately (no retry).
//   - RATE_LIMIT, TRANSIENT → retry with backoff up to MaxRetries.
//   - MALFORMED_RESPONSE → retry up to MaxMalformedRetries, then promote
//     to PERMANENT (limited retry, then permanent).
//
// The backoff schedule follows RetrySchedule (1s, 5s, 15s, 30s, 60s, 5m)
// with ±20% jitter. For RATE_LIMIT errors, RetryAfter is honoured if
// the remote service provided it.
func (c *Client) withRetry(ctx context.Context, fn func(attempt int) error) error {
	policy := DefaultRetryPolicy(c.config.Retries)
	var lastErr error
	malformedAttempts := 0

	for attempt := 0; attempt < policy.MaxRetries; attempt++ {
		lastErr = fn(attempt)
		if lastErr == nil {
			return nil
		}

		// Track malformed-specific attempts.
		var re *RemoteError
		if errors.As(lastErr, &re) && re.Class == RemoteErrorMalformed {
			malformedAttempts++
		}

		// Ask the policy whether to stop.
		var stop bool
		lastErr, stop = policy.ShouldStop(lastErr, malformedAttempts)
		if stop {
			if errors.As(lastErr, &re) {
				log.Printf("Remote engine stopping (attempt %d/%d): %s", attempt+1, policy.MaxRetries, re)
			}
			return lastErr
		}

		// Log the retryable error.
		if errors.As(lastErr, &re) {
			log.Printf("Remote engine retryable error (attempt %d/%d, malformed %d/%d): %s",
				attempt+1, policy.MaxRetries, malformedAttempts, policy.MaxMalformedRetries, re)
		} else {
			log.Printf("Remote engine error (attempt %d/%d): %v", attempt+1, policy.MaxRetries, lastErr)
		}

		// Compute backoff.
		backoff := RetrySchedule(attempt)
		if re != nil && re.RetryAfter > 0 {
			backoff = re.RetryAfter
		}
		backoff = AddJitter(backoff, int64(attempt)+time.Now().UnixNano())

		if attempt < policy.MaxRetries-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
	}

	return lastErr
}

// ── Simple script ────────────────────────────────────────────────────────────

// GenerateSimpleScript generates a single script from a topic.
func (c *Client) GenerateSimpleScript(ctx context.Context, req SimpleScriptRequest) (*SimpleScriptResponse, error) {
	if !c.IsConfigured() {
		return nil, ErrNotConfigured
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, &RemoteError{
			Class:   RemoteErrorValidation,
			Code:    "MARSHAL",
			Message: fmt.Sprintf("failed to marshal request: %v", err),
			Cause:   err,
		}
	}

	var resp *SimpleScriptResponse

	retryErr := c.withRetry(ctx, func(attempt int) error {
		r, e := c.doSimpleScriptRequest(ctx, body)
		if e != nil {
			return e
		}
		resp = r
		return nil
	})

	if retryErr != nil {
		return nil, retryErr
	}
	return resp, nil
}

func (c *Client) doSimpleScriptRequest(ctx context.Context, body []byte) (*SimpleScriptResponse, error) {
	url := strings.TrimSuffix(c.config.URL, "/") + "/api/script-simple"

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, &RemoteError{
			Class:   RemoteErrorPermanent,
			Code:    "REQUEST_BUILD",
			Message: fmt.Sprintf("failed to create request: %v", err),
			Cause:   err,
		}
	}

	httpReq.Header.Set("Content-Type", "application/json")
	if c.config.Token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.config.Token)
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, ClassifyNetworkError(err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, ClassifyNetworkError(err)
	}

	if resp.StatusCode >= 400 {
		return nil, classifyHTTPResponse(resp, respBody, nil)
	}

	var result SimpleScriptResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, ClassifyDecodeError(err, string(respBody))
	}

	return &result, nil
}

// ── Batch scripts ────────────────────────────────────────────────────────────

// GenerateBatchScripts generates multiple scripts from topics.
func (c *Client) GenerateBatchScripts(ctx context.Context, req BatchScriptRequest) (*BatchScriptResponse, error) {
	if !c.IsConfigured() {
		return nil, ErrNotConfigured
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, &RemoteError{
			Class:   RemoteErrorValidation,
			Code:    "MARSHAL",
			Message: fmt.Sprintf("failed to marshal request: %v", err),
			Cause:   err,
		}
	}

	var resp *BatchScriptResponse

	retryErr := c.withRetry(ctx, func(attempt int) error {
		r, e := c.doBatchScriptRequest(ctx, body)
		if e != nil {
			return e
		}
		resp = r
		return nil
	})

	if retryErr != nil {
		return nil, retryErr
	}
	return resp, nil
}

func (c *Client) doBatchScriptRequest(ctx context.Context, body []byte) (*BatchScriptResponse, error) {
	url := strings.TrimSuffix(c.config.URL, "/") + "/api/script-multiple"

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, &RemoteError{
			Class:   RemoteErrorPermanent,
			Code:    "REQUEST_BUILD",
			Message: fmt.Sprintf("failed to create request: %v", err),
			Cause:   err,
		}
	}

	httpReq.Header.Set("Content-Type", "application/json")
	if c.config.Token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.config.Token)
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, ClassifyNetworkError(err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, ClassifyNetworkError(err)
	}

	if resp.StatusCode >= 400 {
		return nil, classifyHTTPResponse(resp, respBody, nil)
	}

	var result BatchScriptResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, ClassifyDecodeError(err, string(respBody))
	}

	return &result, nil
}

// ── Pipeline status ──────────────────────────────────────────────────────────

// GetPipelineStatus gets the status of a pipeline job.
func (c *Client) GetPipelineStatus(ctx context.Context, traceID string) (*PipelineStatusResponse, error) {
	if !c.IsConfigured() {
		return nil, ErrNotConfigured
	}

	var resp *PipelineStatusResponse

	retryErr := c.withRetry(ctx, func(attempt int) error {
		r, e := c.doPipelineStatusRequest(ctx, traceID)
		if e != nil {
			return e
		}
		resp = r
		return nil
	})

	if retryErr != nil {
		return nil, retryErr
	}
	return resp, nil
}

func (c *Client) doPipelineStatusRequest(ctx context.Context, traceID string) (*PipelineStatusResponse, error) {
	url := fmt.Sprintf("%s/api/jobs/%s", strings.TrimSuffix(c.config.URL, "/"), traceID)
	log.Printf("[CLIENT] GetPipelineStatus GET %s", url)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, &RemoteError{
			Class:   RemoteErrorPermanent,
			Code:    "REQUEST_BUILD",
			Message: fmt.Sprintf("failed to create request: %v", err),
			Cause:   err,
		}
	}

	httpReq.Header.Set("Content-Type", "application/json")
	if c.config.Token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.config.Token)
	}

	startTime := time.Now()
	resp, err := c.httpClient.Do(httpReq)
	elapsed := time.Since(startTime).Round(time.Millisecond)
	if err != nil {
		log.Printf("[CLIENT] GetPipelineStatus FAILED after %s: %v", elapsed, err)
		return nil, ClassifyNetworkError(err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, ClassifyNetworkError(err)
	}

	if resp.StatusCode >= 400 {
		log.Printf("[CLIENT] GetPipelineStatus HTTP %d after %s: %s", resp.StatusCode, elapsed, string(respBody))
		return nil, classifyHTTPResponse(resp, respBody, nil)
	}

	// The remote engine wraps the job in {"job": {...}}
	var wrapper remoteJobResponse
	if err := json.Unmarshal(respBody, &wrapper); err != nil {
		return nil, ClassifyDecodeError(err, string(respBody))
	}

	j := wrapper.Job
	status := j.Status
	ok := status == "completed" || status == "running" || status == "queued"

	var createdAt, updatedAt time.Time
	if j.CreatedAt != "" {
		createdAt, _ = time.Parse(time.RFC3339, j.CreatedAt)
	}
	if j.UpdatedAt != "" {
		updatedAt, _ = time.Parse(time.RFC3339, j.UpdatedAt)
	}

	// Use Result (output fields: title, script_text, scenes_json, voiceover_path)
	// falling back to Payload (input params) if Result is nil
	resultData := j.Result
	if resultData == nil {
		resultData = j.Payload
	}

	result := &PipelineStatusResponse{
		OK:        ok,
		TraceID:   j.ID,
		Status:    j.Status,
		Progress:  float64(j.Progress),
		Result:    resultData,
		Error:     j.Error,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}

	log.Printf("[CLIENT] GetPipelineStatus OK job_id=%s status=%s progress=%d elapsed=%s", j.ID, j.Status, j.Progress, elapsed)
	return result, nil
}

// ── Start pipeline ───────────────────────────────────────────────────────────

// StartPipeline starts a new pipeline job.
//
// The Idempotency-Key header is set to idempotencyKey when non-empty, so
// a timeout after the remote service has already created the job does not
// produce a duplicate on retry. The remote service must return the same
// remote_job_id for the same key.
//
// idempotencyKey should be the pipeline_run_id (e.g. "run_...").
func (c *Client) StartPipeline(ctx context.Context, payload map[string]interface{}, idempotencyKey string) (map[string]interface{}, error) {
	if !c.IsConfigured() {
		return nil, ErrNotConfigured
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, &RemoteError{
			Class:   RemoteErrorValidation,
			Code:    "MARSHAL",
			Message: fmt.Sprintf("failed to marshal request: %v", err),
			Cause:   err,
		}
	}

	url := strings.TrimSuffix(c.config.URL, "/") + "/api/script/generate-with-images"
	log.Printf("[CLIENT] StartPipeline POST %s body=%d bytes idempotency_key=%s", url, len(body), idempotencyKey)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, &RemoteError{
			Class:   RemoteErrorPermanent,
			Code:    "REQUEST_BUILD",
			Message: fmt.Sprintf("failed to create request: %v", err),
			Cause:   err,
		}
	}

	httpReq.Header.Set("Content-Type", "application/json")
	if c.config.Token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.config.Token)
	}
	if idempotencyKey != "" {
		httpReq.Header.Set("Idempotency-Key", idempotencyKey)
	}

	startTime := time.Now()
	resp, err := c.httpClient.Do(httpReq)
	elapsed := time.Since(startTime).Round(time.Millisecond)
	if err != nil {
		log.Printf("[CLIENT] StartPipeline FAILED after %s: %v", elapsed, err)
		return nil, ClassifyNetworkError(err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, ClassifyNetworkError(err)
	}

	if resp.StatusCode >= 400 {
		log.Printf("[CLIENT] StartPipeline HTTP %d after %s: %s", resp.StatusCode, elapsed, string(respBody))
		return nil, classifyHTTPResponse(resp, respBody, nil)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, ClassifyDecodeError(err, string(respBody))
	}

	// Area 2: Validate the initial response — the remote engine must
	// return at least a job_id (with fallback to trace_id/id) and a
	// known status (queued, running, completed, failed, cancelled).
	// A contract violation is a PERMANENT error — no retry.
	initial, valErr := ValidateInitialResponse(result)
	if valErr != nil {
		log.Printf("[CLIENT] StartPipeline CONTRACT VIOLATION after %s: %s", elapsed, valErr)
		return nil, valErr
	}

	log.Printf("[CLIENT] StartPipeline OK job_id=%s status=%s elapsed=%s", initial.JobID, initial.Status, elapsed)

	return result, nil
}

// ── Cancel pipeline ──────────────────────────────────────────────────────────

// CancelPipeline cancels/deletes a running pipeline job.
func (c *Client) CancelPipeline(ctx context.Context, traceID string) error {
	if !c.IsConfigured() {
		return ErrNotConfigured
	}

	url := fmt.Sprintf("%s/api/jobs/%s", strings.TrimSuffix(c.config.URL, "/"), traceID)
	log.Printf("[CLIENT] CancelPipeline DELETE %s", url)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return &RemoteError{
			Class:   RemoteErrorPermanent,
			Code:    "REQUEST_BUILD",
			Message: fmt.Sprintf("failed to create request: %v", err),
			Cause:   err,
		}
	}

	httpReq.Header.Set("Content-Type", "application/json")
	if c.config.Token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.config.Token)
	}

	startTime := time.Now()
	resp, err := c.httpClient.Do(httpReq)
	elapsed := time.Since(startTime).Round(time.Millisecond)
	if err != nil {
		log.Printf("[CLIENT] CancelPipeline FAILED after %s: %v", elapsed, err)
		return ClassifyNetworkError(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		log.Printf("[CLIENT] CancelPipeline HTTP %d after %s: %s", resp.StatusCode, elapsed, string(respBody))
		return classifyHTTPResponse(resp, respBody, nil)
	}

	log.Printf("[CLIENT] CancelPipeline OK job_id=%s elapsed=%s", traceID, elapsed)
	return nil
}

// Close closes the client.
func (c *Client) Close() error {
	c.httpClient.CloseIdleConnections()
	return nil
}

// ── Internal HTTP classification ─────────────────────────────────────────────

// classifyHTTPResponse builds a *RemoteError from an HTTP error response,
// parsing the Retry-After header when present (for 429 responses).
func classifyHTTPResponse(resp *http.Response, respBody []byte, cause error) *RemoteError {
	re := ClassifyHTTPError(resp.StatusCode, string(respBody), cause)

	// Parse Retry-After for 429 responses.
	if resp.StatusCode == 429 {
		re.RetryAfter = ParseRetryAfter(resp.Header.Get("Retry-After"))
	}

	return re
}

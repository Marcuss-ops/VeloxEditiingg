package remoteengine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// DefaultConfig returns config from environment
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

// NewClient creates a new remote engine client
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

// IsConfigured returns true if remote engine is configured
func (c *Client) IsConfigured() bool {
	return c.config.URL != ""
}

// GenerateSimpleScript generates a single script from a topic
func (c *Client) GenerateSimpleScript(ctx context.Context, req SimpleScriptRequest) (*SimpleScriptResponse, error) {
	if !c.IsConfigured() {
		return nil, fmt.Errorf("remote engine not configured (set VELOX_REMOTE_ENGINE_URL)")
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	var resp *SimpleScriptResponse
	var lastErr error

	for attempt := 0; attempt < c.config.Retries; attempt++ {
		resp, lastErr = c.doSimpleScriptRequest(ctx, body)
		if lastErr == nil {
			return resp, nil
		}

		// Don't retry on client errors (4xx)
		if strings.Contains(lastErr.Error(), "4") {
			break
		}

		log.Printf("Remote engine request failed (attempt %d/%d): %v", attempt+1, c.config.Retries, lastErr)

		// Exponential backoff
		if attempt < c.config.Retries-1 {
			backoff := time.Duration(100*(1<<attempt)) * time.Millisecond
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}
	}

	return nil, lastErr
}

func (c *Client) doSimpleScriptRequest(ctx context.Context, body []byte) (*SimpleScriptResponse, error) {
	url := strings.TrimSuffix(c.config.URL, "/") + "/api/script-simple"

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	if c.config.Token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.config.Token)
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("remote engine error: %s - %s", resp.Status, string(respBody))
	}

	var result SimpleScriptResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

// GenerateBatchScripts generates multiple scripts from topics
func (c *Client) GenerateBatchScripts(ctx context.Context, req BatchScriptRequest) (*BatchScriptResponse, error) {
	if !c.IsConfigured() {
		return nil, fmt.Errorf("remote engine not configured (set VELOX_REMOTE_ENGINE_URL)")
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	var resp *BatchScriptResponse
	var lastErr error

	for attempt := 0; attempt < c.config.Retries; attempt++ {
		resp, lastErr = c.doBatchScriptRequest(ctx, body)
		if lastErr == nil {
			return resp, nil
		}

		if strings.Contains(lastErr.Error(), "4") {
			break
		}

		log.Printf("Remote engine request failed (attempt %d/%d): %v", attempt+1, c.config.Retries, lastErr)

		if attempt < c.config.Retries-1 {
			backoff := time.Duration(100*(1<<attempt)) * time.Millisecond
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}
	}

	return nil, lastErr
}

func (c *Client) doBatchScriptRequest(ctx context.Context, body []byte) (*BatchScriptResponse, error) {
	url := strings.TrimSuffix(c.config.URL, "/") + "/api/script-multiple"

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	if c.config.Token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.config.Token)
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("remote engine error: %s - %s", resp.Status, string(respBody))
	}

	var result BatchScriptResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

// GetPipelineStatus gets the status of a pipeline job
func (c *Client) GetPipelineStatus(ctx context.Context, traceID string) (*PipelineStatusResponse, error) {
	if !c.IsConfigured() {
		return nil, fmt.Errorf("remote engine not configured (set VELOX_REMOTE_ENGINE_URL)")
	}

	var resp *PipelineStatusResponse
	var lastErr error

	for attempt := 0; attempt < c.config.Retries; attempt++ {
		resp, lastErr = c.doPipelineStatusRequest(ctx, traceID)
		if lastErr == nil {
			return resp, nil
		}

		if strings.Contains(lastErr.Error(), "4") {
			break
		}

		log.Printf("Remote engine request failed (attempt %d/%d): %v", attempt+1, c.config.Retries, lastErr)

		if attempt < c.config.Retries-1 {
			backoff := time.Duration(100*(1<<attempt)) * time.Millisecond
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}
	}

	return nil, lastErr
}

func (c *Client) doPipelineStatusRequest(ctx context.Context, traceID string) (*PipelineStatusResponse, error) {
	url := fmt.Sprintf("%s/api/jobs/%s", strings.TrimSuffix(c.config.URL, "/"), traceID)
	log.Printf("[CLIENT] GetPipelineStatus GET %s", url)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
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
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		log.Printf("[CLIENT] GetPipelineStatus HTTP %d after %s: %s", resp.StatusCode, elapsed, string(respBody))
		return nil, fmt.Errorf("remote engine error: %s - %s", resp.Status, string(respBody))
	}

	// The remote engine wraps the job in {"job": {...}}
	var wrapper remoteJobResponse
	if err := json.Unmarshal(respBody, &wrapper); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
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

// StartPipeline starts a new pipeline job
func (c *Client) StartPipeline(ctx context.Context, payload map[string]interface{}) (map[string]interface{}, error) {
	if !c.IsConfigured() {
		return nil, fmt.Errorf("remote engine not configured (set VELOX_REMOTE_ENGINE_URL)")
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	url := strings.TrimSuffix(c.config.URL, "/") + "/api/script/generate-with-images"
	log.Printf("[CLIENT] StartPipeline POST %s body=%d bytes", url, len(body))

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	if c.config.Token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.config.Token)
	}

	startTime := time.Now()
	resp, err := c.httpClient.Do(httpReq)
	elapsed := time.Since(startTime).Round(time.Millisecond)
	if err != nil {
		log.Printf("[CLIENT] StartPipeline FAILED after %s: %v", elapsed, err)
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		log.Printf("[CLIENT] StartPipeline HTTP %d after %s: %s", resp.StatusCode, elapsed, string(respBody))
		return nil, fmt.Errorf("remote engine error: %s - %s", resp.Status, string(respBody))
	}

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	jobID, _ := result["job_id"].(string)
	status, _ := result["status"].(string)
	log.Printf("[CLIENT] StartPipeline OK job_id=%s status=%s elapsed=%s", jobID, status, elapsed)

	return result, nil
}

// CancelPipeline cancels/deletes a running pipeline job
func (c *Client) CancelPipeline(ctx context.Context, traceID string) error {
	if !c.IsConfigured() {
		return fmt.Errorf("remote engine not configured (set VELOX_REMOTE_ENGINE_URL)")
	}

	url := fmt.Sprintf("%s/api/jobs/%s", strings.TrimSuffix(c.config.URL, "/"), traceID)
	log.Printf("[CLIENT] CancelPipeline DELETE %s", url)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
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
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		log.Printf("[CLIENT] CancelPipeline HTTP %d after %s: %s", resp.StatusCode, elapsed, string(respBody))
		return fmt.Errorf("remote engine error: %s - %s", resp.Status, string(respBody))
	}

	log.Printf("[CLIENT] CancelPipeline OK job_id=%s elapsed=%s", traceID, elapsed)
	return nil
}

// Close closes the client
func (c *Client) Close() error {
	c.httpClient.CloseIdleConnections()
	return nil
}

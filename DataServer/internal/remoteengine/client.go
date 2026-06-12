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
	"sync"
	"time"
)

// Config holds remote engine configuration
type Config struct {
	URL       string
	Token     string
	TimeoutMS int
	Retries   int
}

// Client is the remote engine client
type Client struct {
	config     Config
	httpClient *http.Client
	mu         sync.RWMutex
}

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

// SimpleScriptRequest is the input for simple script generation
type SimpleScriptRequest struct {
	Topic     string            `json:"topic"`
	Language  string            `json:"language,omitempty"`
	Style     string            `json:"style,omitempty"`
	Duration  int               `json:"duration,omitempty"` // seconds
	Variables map[string]string `json:"variables,omitempty"`
}

// SimpleScriptResponse is the output
type SimpleScriptResponse struct {
	OK       bool   `json:"ok"`
	Script   string `json:"script"`
	Title    string `json:"title,omitempty"`
	Error    string `json:"error,omitempty"`
	TraceID  string `json:"trace_id,omitempty"`
	Provider string `json:"provider,omitempty"`
}

// BatchScriptRequest is the input for batch generation
type BatchScriptRequest struct {
	Topics    []string          `json:"topics"`
	Language  string            `json:"language,omitempty"`
	Style     string            `json:"style,omitempty"`
	Variables map[string]string `json:"variables,omitempty"`
}

// BatchScriptResponse is the output
type BatchScriptResponse struct {
	OK      bool              `json:"ok"`
	Scripts []GeneratedScript `json:"scripts,omitempty"`
	Error   string            `json:"error,omitempty"`
	TraceID string            `json:"trace_id,omitempty"`
}

// GeneratedScript represents a single generated script
type GeneratedScript struct {
	Topic  string `json:"topic"`
	Script string `json:"script"`
	Title  string `json:"title,omitempty"`
	Error  string `json:"error,omitempty"`
}

// PipelineStatusResponse is the status of a pipeline job
type PipelineStatusResponse struct {
	OK        bool                   `json:"ok"`
	TraceID   string                 `json:"trace_id"`
	Status    string                 `json:"status"` // pending, running, completed, failed
	Progress  float64                `json:"progress,omitempty"`
	Result    map[string]interface{} `json:"result,omitempty"`
	Error     string                 `json:"error,omitempty"`
	CreatedAt time.Time              `json:"created_at,omitempty"`
	UpdatedAt time.Time              `json:"updated_at,omitempty"`
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
	url := fmt.Sprintf("%s/api/remote/pipeline/status/%s", strings.TrimSuffix(c.config.URL, "/"), traceID)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
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

	var result PipelineStatusResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
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

	url := strings.TrimSuffix(c.config.URL, "/") + "/api/remote/pipeline/generate"

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

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return result, nil
}

// Close closes the client
func (c *Client) Close() error {
	c.httpClient.CloseIdleConnections()
	return nil
}

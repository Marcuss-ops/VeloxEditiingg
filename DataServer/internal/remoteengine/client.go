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
	"net/http"
	"time"
)

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

// Close closes the client.
func (c *Client) Close() error {
	c.httpClient.CloseIdleConnections()
	return nil
}

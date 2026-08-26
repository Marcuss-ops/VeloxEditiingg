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
	"net"
	"net/http"
	"time"

	"velox-server/internal/logging"
)

// NewClient creates a new remote engine client.
func NewClient(cfg Config) *Client {
	timeout := time.Duration(cfg.TimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          32,
		MaxIdleConnsPerHost:   8,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}
	return &Client{
		config: cfg,
		httpClient: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
		logger: logging.NewLogger("remoteengine"),
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

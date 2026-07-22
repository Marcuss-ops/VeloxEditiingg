package remoteengine

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// newRequest builds an HTTP request for the remote engine.
// It sets Content-Type and Authorization headers, and optionally an
// Idempotency-Key header.
func (c *Client) newRequest(ctx context.Context, method, path string, body []byte, idempotencyKey string) (*http.Request, error) {
	url := strings.TrimSuffix(c.config.URL, "/") + path

	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	httpReq, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
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

	return httpReq, nil
}

// doRequest executes an HTTP request and returns the response body.
// It classifies network errors and HTTP error statuses into *RemoteError.
func (c *Client) doRequest(httpReq *http.Request) ([]byte, error) {
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		// If the caller's context was canceled or its deadline exceeded,
		// classify it as a permanent RemoteError so the retry loop stops
		// immediately while keeping the typed error contract.
		if ctxErr := httpReq.Context().Err(); ctxErr != nil {
			return nil, ClassifyNetworkError(ctxErr)
		}
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

	return respBody, nil
}

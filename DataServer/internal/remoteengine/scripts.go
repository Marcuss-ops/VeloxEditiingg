package remoteengine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
)

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
	idempotencyKey := scriptIdempotencyKey("/api/script-simple", body)

	var resp *SimpleScriptResponse

	retryErr := c.withRetry(ctx, func(attempt int) error {
		r, e := c.doSimpleScriptRequest(ctx, body, idempotencyKey)
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

func (c *Client) doSimpleScriptRequest(ctx context.Context, body []byte, idempotencyKey string) (*SimpleScriptResponse, error) {
	httpReq, err := c.newRequest(ctx, http.MethodPost, "/api/script-simple", body, idempotencyKey)
	if err != nil {
		return nil, err
	}

	respBody, err := c.doRequest(httpReq)
	if err != nil {
		return nil, err
	}

	var result SimpleScriptResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, ClassifyDecodeError(err, string(respBody))
	}

	return &result, nil
}

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
	idempotencyKey := scriptIdempotencyKey("/api/script-multiple", body)

	var resp *BatchScriptResponse

	retryErr := c.withRetry(ctx, func(attempt int) error {
		r, e := c.doBatchScriptRequest(ctx, body, idempotencyKey)
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

func (c *Client) doBatchScriptRequest(ctx context.Context, body []byte, idempotencyKey string) (*BatchScriptResponse, error) {
	httpReq, err := c.newRequest(ctx, http.MethodPost, "/api/script-multiple", body, idempotencyKey)
	if err != nil {
		return nil, err
	}

	respBody, err := c.doRequest(httpReq)
	if err != nil {
		return nil, err
	}

	var result BatchScriptResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, ClassifyDecodeError(err, string(respBody))
	}

	return &result, nil
}

// scriptIdempotencyKey binds retries to the exact endpoint and request body.
// The public script helpers do not receive a caller-owned operation ID, so a
// digest is the only stable key available at this boundary. Including the
// endpoint prevents a simple-script request from colliding with a batch
// request that happens to serialize to the same JSON.
func scriptIdempotencyKey(endpoint string, body []byte) string {
	h := sha256.New()
	_, _ = h.Write([]byte(endpoint))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(body)
	return "velox-script-" + hex.EncodeToString(h.Sum(nil))
}
